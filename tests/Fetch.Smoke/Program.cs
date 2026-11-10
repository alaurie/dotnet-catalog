using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace Fetch.Smoke;

internal static class Program
{
    private const string Etag = "\"smoke-v1\"";
    private static readonly byte[] Payload = CreatePayload(2 * 1024 * 1024 + 113);
    private static readonly string Digest = Convert.ToHexStringLower(SHA256.HashData(Payload));

    private static async Task<int> Main(string[] args)
    {
        if (args.Length != 1 || !File.Exists(args[0]))
        {
            Console.Error.WriteLine("Usage: Fetch.Smoke <published-fetch-executable>");
            return 2;
        }

        string executable = Path.GetFullPath(args[0]);
        string root = Path.Combine(Path.GetTempPath(), "fetch-smoke-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        int failed = 0;
        (string Name, Func<string, string, Task> Run)[] cases =
        [
            ("multibuffer conditional ranges", RangedAsync),
            ("interruption and cross-process resume", ResumeAsync),
            ("checksum failure preserves destination and repairs bad partial", ChecksumRepairAsync),
            ("malformed Content-Range rejected before writes", (exe, dir) => InvalidResponseAsync(exe, dir, false)),
            ("changed ETag rejected before writes", (exe, dir) => InvalidResponseAsync(exe, dir, true)),
            ("weak ETag streaming, headers, directory and printed hash", (exe, dir) => StreamingAsync(exe, dir, "W/\"weak\"")),
            ("missing ETag streaming", (exe, dir) => StreamingAsync(exe, dir, null)),
            ("oversized manifest fallback and renamed output", ManifestAsync),
            ("chunk retry continues from received offset", RetryAsync)
        ];
        try
        {
            for (int i = 0; i < cases.Length; i++)
            {
                string directory = Path.Combine(root, i.ToString(CultureInfo.InvariantCulture));
                Directory.CreateDirectory(directory);
                try
                {
                    await cases[i].Run(executable, directory);
                    Console.WriteLine("PASS " + cases[i].Name);
                }
                catch (Exception error)
                {
                    failed++;
                    Console.Error.WriteLine("FAIL " + cases[i].Name + ": " + error);
                }
            }
        }
        finally
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (Exception error)
            {
                failed++;
                Console.Error.WriteLine("FAIL temporary directory cleanup: " + error.Message);
            }
        }
        Console.WriteLine($"{cases.Length - failed}/{cases.Length} scenarios passed.");
        return failed == 0 ? 0 : 1;
    }

    private static async Task RangedAsync(string executable, string directory)
    {
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Method == "HEAD")
                await HeadAsync(stream, Etag, token);
            else
                await RangeAsync(request, stream, Payload, token);
        });
        string target = Path.Combine(directory, "multi buffer.bin");
        Result result = await RunAsync(executable, directory, server.Url("data.bin"), "-o", target,
            "-c", "4", "--expected-hash", Digest);
        Success(result);
        EqualFile(target, Payload);
        NoPartial(target);
        var ranges = server.Requests.Where(r => r.Method == "GET").Select(Range).OrderBy(r => r.Start).ToArray();
        Check(ranges.Length == 4, "Expected four range workers.");
        Check(ranges[0].Start == 0 && ranges[^1].End == Payload.Length - 1, "Ranges do not cover the resource.");
        for (int i = 0; i < ranges.Length; i++)
        {
            Check(ranges[i].Length > 128 * 1024, "Range did not exercise multiple downloader buffers.");
            if (i > 0)
                Check(ranges[i].Start == ranges[i - 1].End + 1, "Ranges overlap or leave a gap.");
        }
        server.ThrowIfFaulted();
    }

    private static async Task ResumeAsync(string executable, string directory)
    {
        const int interruptedAt = 327803;
        int phase = 0;
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Method == "HEAD")
            {
                await HeadAsync(stream, Etag, token);
                return;
            }
            var range = Range(request);
            if (Volatile.Read(ref phase) == 0)
            {
                Check(range.Start == 0 && range.End == Payload.Length - 1, "Initial range is unexpected.");
                await RangeAsync(request, stream, Payload, token, truncateAt: interruptedAt);
            }
            else
            {
                Check(range.Start == interruptedAt, "Resume re-requested completed bytes or skipped missing bytes.");
                await RangeAsync(request, stream, Payload, token);
            }
        });
        string target = Path.Combine(directory, "resume.bin");
        string[] arguments = [server.Url("resume.bin"), "-o", target, "-c", "1", "--expected-hash", Digest];
        Failure(await RunAsync(executable, directory, arguments));
        Check(!File.Exists(target), "An interrupted transfer promoted its destination.");
        Check(File.Exists(target + ".part.meta"), "Interrupted range state was not persisted.");
        byte[] partial = File.ReadAllBytes(target + ".part");
        Check(partial.Length >= interruptedAt, "Partial file lost received bytes.");
        Check(partial.AsSpan(0, interruptedAt).SequenceEqual(Payload.AsSpan(0, interruptedAt)),
            "Partial prefix differs from the received bytes.");
        int before = server.Requests.Count(r => r.Method == "GET");
        Volatile.Write(ref phase, 1);
        Success(await RunAsync(executable, directory, arguments));
        Check(server.Requests.Count(r => r.Method == "GET") == before + 1, "Resume did not issue exactly one remaining range.");
        EqualFile(target, Payload);
        NoPartial(target);
        server.ThrowIfFaulted();
    }

    private static async Task ChecksumRepairAsync(string executable, string directory)
    {
        byte[] bad = (byte[])Payload.Clone();
        bad[17] ^= 0xFF;
        int phase = 0;
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Method == "HEAD")
                await HeadAsync(stream, Etag, token);
            else
                await RangeAsync(request, stream, Volatile.Read(ref phase) == 0 ? bad : Payload, token);
        });
        string target = Path.Combine(directory, "existing.bin");
        byte[] old = Encoding.UTF8.GetBytes("Existing destination must survive failed verification.");
        File.WriteAllBytes(target, old);
        string[] arguments = [server.Url("existing.bin"), "-o", target, "-c", "1", "--expected-hash", Digest];
        Failure(await RunAsync(executable, directory, arguments));
        EqualFile(target, old);
        EqualFile(target + ".part", bad);
        Check(File.Exists(target + ".part.meta"), "Checksum failure removed resume metadata.");
        int before = server.Requests.Count(r => r.Method == "GET");
        Volatile.Write(ref phase, 1);
        Success(await RunAsync(executable, directory, arguments));
        var repair = server.Requests.Where(r => r.Method == "GET").Skip(before).ToArray();
        Check(repair.Length == 1 && Range(repair[0]).Start == 0, "Completed corrupt partial was not downloaded afresh.");
        EqualFile(target, Payload);
        NoPartial(target);
        server.ThrowIfFaulted();
    }

    private static async Task InvalidResponseAsync(string executable, string directory, bool changedEtag)
    {
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Method == "HEAD")
            {
                await HeadAsync(stream, Etag, token);
                return;
            }
            var range = Range(request);
            RequireConditional(request);
            string contentRange = changedEtag
                ? $"bytes {range.Start}-{range.End}/{Payload.Length}"
                : $"bytes {range.Start}-{range.End}/{Payload.Length + 1}";
            await HeadersAsync(stream, 206, range.Length, token,
                ("ETag", changedEtag ? "\"changed\"" : Etag), ("Content-Range", contentRange));
            await stream.WriteAsync(Payload.AsMemory(range.Start, range.Length), token);
        });
        string target = Path.Combine(directory, "untouched.bin");
        byte[] old = [1, 3, 5, 7, 9];
        File.WriteAllBytes(target, old);
        Failure(await RunAsync(executable, directory, server.Url("invalid.bin"), "-o", target,
            "-c", "1", "--no-checksum"));
        EqualFile(target, old);
        Check(server.Requests.Count(r => r.Method == "GET") == 1, "Invalid range response was not exercised.");
        if (File.Exists(target + ".part"))
            Check(File.ReadAllBytes(target + ".part").All(b => b == 0), "Invalid response wrote untrusted body bytes.");
        server.ThrowIfFaulted();
    }

    private static async Task StreamingAsync(string executable, string directory, string? etag)
    {
        const string customAgent = "Fetch smoke custom agent/1.0";
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            Check(request.Header("User-Agent") == customAgent, "Custom user agent was not sent.");
            Check(request.Header("Authorization") == "Bearer smoke-secret", "Custom authorization header was not sent.");
            Check(request.Header("X-Smoke") == "value:with:colons", "Custom header parsing lost its value.");
            if (request.Method == "HEAD")
                await HeadAsync(stream, etag, token);
            else
            {
                Check(request.Header("Range") is null && request.Header("If-Match") is null,
                    "A resource without a strong ETag used ranged requests.");
                await HeadersAsync(stream, 200, Payload.Length, token);
                await WriteBodyAsync(stream, Payload, token);
            }
        });
        string outputDirectory = Path.Combine(directory, "output folder");
        Directory.CreateDirectory(outputDirectory);
        Result result = await RunAsync(executable, directory, server.Url("stream.bin"), "-o", outputDirectory,
            "--no-checksum", "--print-hash", "-c", "4", "-A", customAgent,
            "-H", "Authorization: Bearer smoke-secret", "--header", "X-Smoke: value:with:colons");
        Success(result);
        string target = Path.Combine(outputDirectory, "stream.bin");
        EqualFile(target, Payload);
        PrintedHash(result);
        NoPartial(target);
        Check(server.Requests.Count(r => r.Method == "HEAD") == 1 &&
            server.Requests.Count(r => r.Method == "GET") == 1, "Streaming path issued unexpected requests.");
        server.ThrowIfFaulted();
    }

    private static async Task ManifestAsync(string executable, string directory)
    {
        const string localName = "renamed-output.bin";
        byte[] oversized = Encoding.ASCII.GetBytes(new string('0', 128) + "  " + localName + "\n" + new string('\n', 72 * 1024));
        byte[] valid = Encoding.ASCII.GetBytes(Digest + "  " + localName + "\n");
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Path == "/auto/remote.bin")
            {
                Check(request.Header("Authorization") == "Bearer private-download", "Download lost custom credentials.");
                if (request.Method == "HEAD")
                    await HeadAsync(stream, Etag, token);
                else
                    await RangeAsync(request, stream, Payload, token);
                return;
            }
            Check(request.Method == "GET", "Manifest request was not a GET.");
            Check(request.Header("Authorization") is null && request.Header("X-Private") is null,
                "Caller credentials leaked to a manifest probe.");
            if (request.Path == "/auto/SHA512SUMS")
            {
                // No Content-Length: the actual body limit must reject this candidate.
                byte[] header = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
                await stream.WriteAsync(header, token);
                await stream.WriteAsync(Encoding.ASCII.GetBytes(oversized.Length.ToString("X", CultureInfo.InvariantCulture) + "\r\n"), token);
                await stream.WriteAsync(oversized, token);
                await stream.WriteAsync("\r\n0\r\n\r\n"u8.ToArray(), token);
            }
            else if (request.Path == "/auto/SHA256SUMS")
            {
                await HeadersAsync(stream, 200, valid.Length, token);
                await stream.WriteAsync(valid, token);
            }
            else
                await HeadersAsync(stream, 404, 0, token);
        });
        string target = Path.Combine(directory, localName);
        Result result = await RunAsync(executable, directory, server.Url("auto/remote.bin"), "-o", target,
            "-c", "2", "--print-hash", "-H", "Authorization: Bearer private-download", "-H", "X-Private: private");
        Success(result);
        EqualFile(target, Payload);
        PrintedHash(result);
        NoPartial(target);
        string[] probes = server.Requests.Where(r => r.Path != "/auto/remote.bin").Select(r => r.Path).ToArray();
        Check(probes.SequenceEqual(["/auto/SHA512SUMS", "/auto/SHA256SUMS"]),
            "Manifest fallback ordering or renamed-filename matching failed.");
        server.ThrowIfFaulted();
    }

    private static async Task RetryAsync(string executable, string directory)
    {
        const int interruptedAt = 200003;
        int attempts = 0;
        await using var server = new LoopbackServer(async (request, stream, token) =>
        {
            if (request.Method == "HEAD")
            {
                await HeadAsync(stream, Etag, token);
                return;
            }
            int attempt = Interlocked.Increment(ref attempts);
            var range = Range(request);
            Check(range.Start == (attempt == 1 ? 0 : interruptedAt), "Chunk retry lost its received offset.");
            Check(attempt <= 2, "More chunk attempts than the retry allowance.");
            await RangeAsync(request, stream, Payload, token, truncateAt: attempt == 1 ? interruptedAt : null);
        });
        string target = Path.Combine(directory, "retry.bin");
        Success(await RunAsync(executable, directory, server.Url("retry.bin"), "-o", target,
            "-c", "1", "--retries", "1", "--expected-hash", Digest));
        Check(attempts == 2, "The failed chunk was not retried exactly once.");
        EqualFile(target, Payload);
        NoPartial(target);
        server.ThrowIfFaulted();
    }

    private static async Task<Result> RunAsync(string executable, string directory, params string[] arguments)
    {
        var start = new ProcessStartInfo(executable)
        {
            WorkingDirectory = directory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        start.ArgumentList.Add("--quiet");
        foreach (string argument in arguments)
            start.ArgumentList.Add(argument);
        using var process = new Process { StartInfo = start };
        Check(process.Start(), "Could not start the published executable.");
        Task<string> stdout = process.StandardOutput.ReadToEndAsync();
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(35));
        try
        {
            await process.WaitForExitAsync(deadline.Token);
            return new Result(process.ExitCode,
                await stdout.WaitAsync(TimeSpan.FromSeconds(5)),
                await stderr.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            throw new TimeoutException("Fetch did not exit within 35 seconds.");
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            }
        }
    }

    private static Task HeadAsync(NetworkStream stream, string? etag, CancellationToken token)
    {
        var headers = new List<(string, string)> { ("Accept-Ranges", "bytes") };
        if (etag is not null)
            headers.Add(("ETag", etag));
        return HeadersAsync(stream, 200, Payload.Length, token, headers.ToArray());
    }

    private static async Task RangeAsync(Request request, NetworkStream stream, byte[] body,
        CancellationToken token, int? truncateAt = null)
    {
        RequireConditional(request);
        var range = Range(request);
        Check(range.Start >= 0 && range.End < body.Length, "Requested range is outside the resource.");
        await HeadersAsync(stream, 206, range.Length, token,
            ("ETag", Etag), ("Content-Range", $"bytes {range.Start}-{range.End}/{body.Length}"));
        int length = truncateAt ?? range.Length;
        Check(length > 0 && length <= range.Length, "Invalid interruption offset.");
        await WriteBodyAsync(stream, body.AsMemory(range.Start, length), token);
        // Closing the connection with an advertised longer Content-Length deliberately interrupts the body.
    }

    private static void RequireConditional(Request request)
    {
        Check(request.Method == "GET", "Unexpected request method.");
        Check(request.Header("If-Match") == Etag, "Range GET did not send the exact strong If-Match validator.");
    }

    private static ByteRange Range(Request request)
    {
        string? header = request.Header("Range");
        Check(header is not null && header.StartsWith("bytes=", StringComparison.Ordinal), "Missing byte range.");
        string[] ends = header![6..].Split('-');
        Check(ends.Length == 2 && int.TryParse(ends[0], NumberStyles.None, CultureInfo.InvariantCulture, out _) &&
            int.TryParse(ends[1], NumberStyles.None, CultureInfo.InvariantCulture, out _), "Malformed request range.");
        int start = int.Parse(ends[0], CultureInfo.InvariantCulture);
        int end = int.Parse(ends[1], CultureInfo.InvariantCulture);
        Check(start >= 0 && end >= start, "Invalid request range bounds.");
        return new ByteRange(start, end);
    }

    private static async Task HeadersAsync(NetworkStream stream, int status, int length,
        CancellationToken token, params (string Name, string Value)[] headers)
    {
        var text = new StringBuilder($"HTTP/1.1 {status} Response\r\nContent-Length: {length}\r\nConnection: close\r\n");
        foreach (var (name, value) in headers)
            text.Append(name).Append(": ").Append(value).Append("\r\n");
        text.Append("\r\n");
        await stream.WriteAsync(Encoding.ASCII.GetBytes(text.ToString()), token);
    }

    private static async Task WriteBodyAsync(NetworkStream stream, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        while (!body.IsEmpty)
        {
            int count = Math.Min(body.Length, 16 * 1024);
            await stream.WriteAsync(body[..count], token);
            body = body[count..];
        }
        await stream.FlushAsync(token);
    }

    private static byte[] CreatePayload(int length)
    {
        byte[] bytes = new byte[length];
        new Random(713901).NextBytes(bytes);
        return bytes;
    }

    private static void EqualFile(string path, byte[] expected)
    {
        Check(File.Exists(path), "Missing output: " + path);
        byte[] actual = File.ReadAllBytes(path);
        Check(actual.AsSpan().SequenceEqual(expected), "File bytes differ: " + path);
    }

    private static void NoPartial(string target)
    {
        Check(!File.Exists(target + ".part") && !File.Exists(target + ".part.meta") &&
            !File.Exists(target + ".part.meta.tmp"), "Successful transfer left partial state.");
    }

    private static void PrintedHash(Result result)
    {
        // Match the digest, not an implementation's human-readable label.
        Check(result.Stdout.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
            .Any(word => string.Equals(word, Digest, StringComparison.OrdinalIgnoreCase)),
            "--print-hash did not put the complete SHA-256 digest on stdout.");
    }

    private static void Success(Result result) => Check(result.ExitCode == 0, "Expected success. " + result);
    private static void Failure(Result result) => Check(result.ExitCode != 0, "Expected failure. " + result);

    private static void Check(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    private sealed record Result(int ExitCode, string Stdout, string Stderr);
    private readonly record struct ByteRange(int Start, int End)
    {
        public int Length => End - Start + 1;
    }

    private sealed record Request(string Method, string Path, Dictionary<string, string> Headers)
    {
        public string? Header(string name) => Headers.GetValueOrDefault(name);
    }

    /// <summary>One-request-per-connection HTTP/1.1 fixture without HttpListener URL ACL requirements.</summary>
    private sealed class LoopbackServer : IAsyncDisposable
    {
        private readonly TcpListener listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource shutdown = new();
        private readonly ConcurrentBag<Task> connections = [];
        private readonly ConcurrentQueue<Exception> faults = new();
        private readonly Func<Request, NetworkStream, CancellationToken, Task> handler;
        private readonly Task accepting;
        private readonly int port;
        public ConcurrentQueue<Request> Requests { get; } = new();

        public LoopbackServer(Func<Request, NetworkStream, CancellationToken, Task> handler)
        {
            this.handler = handler;
            listener.Start();
            port = ((IPEndPoint)listener.LocalEndpoint).Port;
            accepting = AcceptAsync();
        }

        public string Url(string path) => $"http://127.0.0.1:{port}/{path}";

        private async Task AcceptAsync()
        {
            try
            {
                while (!shutdown.IsCancellationRequested)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync(shutdown.Token);
                    connections.Add(ServeAsync(client));
                }
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
            catch (SocketException) when (shutdown.IsCancellationRequested) { }
            catch (ObjectDisposedException) when (shutdown.IsCancellationRequested) { }
        }

        private async Task ServeAsync(TcpClient client)
        {
            using (client)
            using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(shutdown.Token))
            {
                deadline.CancelAfter(TimeSpan.FromSeconds(15));
                try
                {
                    client.NoDelay = true;
                    NetworkStream stream = client.GetStream();
                    using var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, leaveOpen: true);
                    string? first = await reader.ReadLineAsync(deadline.Token);
                    Check(first is not null && first.Length < 8192, "Missing or oversized request line.");
                    string[] parts = first!.Split(' ');
                    Check(parts.Length == 3, "Malformed request line.");
                    var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    int total = first.Length;
                    while (true)
                    {
                        string? line = await reader.ReadLineAsync(deadline.Token);
                        Check(line is not null, "Truncated request headers.");
                        if (line!.Length == 0)
                            break;
                        total += line.Length;
                        Check(total <= 32 * 1024, "Oversized request headers.");
                        int colon = line.IndexOf(':');
                        Check(colon > 0, "Malformed request header.");
                        string name = line[..colon];
                        string value = line[(colon + 1)..].Trim();
                        headers[name] = headers.TryGetValue(name, out string? previous) ? previous + ", " + value : value;
                    }
                    var request = new Request(parts[0], parts[1].Split('?')[0], headers);
                    Requests.Enqueue(request);
                    await handler(request, stream, deadline.Token);
                }
                // Clients may stop reading rejected headers and capped manifests; those disconnects are expected.
                catch (IOException) { }
                catch (SocketException) { }
                catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
                catch (Exception error) { faults.Enqueue(error); }
            }
        }

        public void ThrowIfFaulted()
        {
            if (!faults.IsEmpty)
                throw new AggregateException("Loopback server invariant failed.", faults);
        }

        public async ValueTask DisposeAsync()
        {
            shutdown.Cancel();
            listener.Stop();
            try
            {
                await accepting.WaitAsync(TimeSpan.FromSeconds(5));
                await Task.WhenAll(connections).WaitAsync(TimeSpan.FromSeconds(5));
                ThrowIfFaulted();
            }
            finally
            {
                shutdown.Dispose();
            }
        }
    }
}
