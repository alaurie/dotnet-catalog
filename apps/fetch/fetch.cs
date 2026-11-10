#:property OutputType=Exe
#:property TargetFramework=net10.0
#:property ImplicitUsings=enable
#:property Nullable=enable
#:property AssemblyName=fetch
#:property PublishAot=true
#:property OptimizationPreference=Size
#:property InvariantGlobalization=true
#:property UseSystemResourceKeys=true
#:property StackTraceSupport=false
#:property DebuggerSupport=false
#:property EventSourceSupport=false
#:property MetricsSupport=false
#:property HttpActivityPropagationSupport=false
#:property Http3Support=false
#:property DebugType=none
#:property DebugSymbols=false
#:property StripSymbols=true

using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace Fetch;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cancellation = new CancellationTokenSource();
        ConsoleCancelEventHandler interrupt = (_, e) => { e.Cancel = true; cancellation.Cancel(); };
        Console.CancelKeyPress += interrupt;
        try
        {
            var options = Options.Parse(args);
            if (options.Help)
            {
                Console.WriteLine(Options.HelpText);
                return 0;
            }
            if (options.Version)
            {
                Console.WriteLine("fetch 2.0 (.NET 10 Native AOT)");
                return 0;
            }
            using var handler = new SocketsHttpHandler
            {
                ConnectTimeout = TimeSpan.FromSeconds(15),
                MaxConnectionsPerServer = 64,
                AutomaticDecompression = DecompressionMethods.None
            };
            using var client = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
            await new Downloader(options, client).RunAsync(cancellation.Token);
            return 0;
        }
        catch (ArgumentException error)
        {
            Console.Error.WriteLine($"Error: {error.Message}");
            return 2;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            Console.Error.WriteLine("Download interrupted. Ranged progress saved for resumption.");
            return 130;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine($"Error: {error.Message}");
            return 1;
        }
        finally
        {
            Console.CancelKeyPress -= interrupt;
        }
    }
}

/// <summary>Streams into a sibling .part; only a verified completed transfer may replace the destination.</summary>
internal sealed class Downloader(Options options, HttpClient client)
{
    private readonly Uri uri = options.Uri!;
    private const int BufferSize = 128 * 1024;

    public async Task RunAsync(CancellationToken token)
    {
        var urlPath = Uri.UnescapeDataString(uri.AbsolutePath);
        var remoteName = SafeFilename(urlPath);
        var directoryOutput = options.Output is not null && (Directory.Exists(options.Output)
            || options.Output.EndsWith('/') || options.Output.EndsWith('\\'));
        var output = options.Output is null ? remoteName
            : directoryOutput ? Path.Combine(options.Output, remoteName) : options.Output;
        output = Path.GetFullPath(output);
        var initialOutput = output;
        var expected = options.ExplicitHash is null ? null : Hashing.ParseExplicit(options.ExplicitHash);
        if (expected is not null && await AlreadyVerifiedAsync(output, expected, token))
            return;

        using var headRequest = Request(HttpMethod.Head);
        using var head = await client.SendAsync(headRequest, HttpCompletionOption.ResponseHeadersRead, token);
        head.EnsureSuccessStatusCode();
        if (options.Output is null || directoryOutput)
        {
            var disposition = head.Content.Headers.ContentDisposition;
            var suggested = disposition?.FileNameStar ?? disposition?.FileName;
            if (!string.IsNullOrWhiteSpace(suggested))
            {
                var name = SafeFilename(suggested.Trim('"'));
                output = directoryOutput ? Path.Combine(options.Output!, name) : name;
            }
        }
        output = Path.GetFullPath(output);
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        if (expected is null && !options.NoChecksum)
            expected = await Hashing.FindAsync(client, uri, remoteName, Path.GetFileName(output), token);
        if (expected is not null && (options.ExplicitHash is null || output != initialOutput)
            && await AlreadyVerifiedAsync(output, expected, token))
            return;
        if (File.Exists(output))
            Log("Local file exists. Re-downloading...");

        var length = head.Content.Headers.ContentLength ?? -1;
        var etag = Header(head, "ETag");
        if (!IsStrongEtag(etag))
            etag = null;
        var modified = Header(head, "Last-Modified");
        var ranges = head.Headers.AcceptRanges.Any(v => v.Equals("bytes", StringComparison.OrdinalIgnoreCase));
        var part = output + ".part";
        var metaPath = part + ".meta";
        if (options.NoResume)
        {
            File.Delete(part);
            File.Delete(metaPath);
            File.Delete(metaPath + ".tmp");
        }
        Resume? resume = null;
        if (File.Exists(part))
        {
            if (File.Exists(metaPath) && !options.NoResume)
                resume = Resume.Load(metaPath, part, uri, length, etag, modified);
            if (resume is not null && expected is not null && resume.Chunks.All(c => c.Complete)
                && !EqualHash(expected.Hash, await Hashing.ComputeAsync(part, expected.Algorithm, token)))
                resume = null;
            if (resume is null)
            {
                Log("Previous partial download cannot be safely resumed. Starting fresh...");
                File.Delete(part);
                File.Delete(metaPath);
            }
        }

        string? streamedHash = null;
        if (ranges && length > 0 && etag is not null)
        {
            var chunks = resume?.Chunks ?? Resume.CreateChunks(length, options.Connections);
            var state = new Resume(uri.OriginalString, length, etag, modified, chunks);
            Log($"{(resume is null ? "Connecting" : "Resuming")} with {chunks.Count} range workers "
                + $"({length / 1048576.0:F2} MB)...");
            await DownloadChunksAsync(part, metaPath, state, token);
        }
        else
        {
            File.Delete(metaPath);
            File.Delete(metaPath + ".tmp");
            Log(length >= 0 ? $"Connecting ({length / 1048576.0:F2} MB)..." : "Connecting (unknown size)...");
            streamedHash = await DownloadStreamAsync(part, expected, token);
        }
        if (expected is not null)
        {
            Log($"Found manifest: {expected.Source} (Algorithm: {expected.Algorithm})");
            streamedHash ??= await Hashing.ComputeAsync(part, expected.Algorithm, token);
            var actual = streamedHash;
            if (!EqualHash(expected.Hash, actual))
                throw new IOException($"Checksum mismatch. Expected: {expected.Hash}; actual: {actual}");
            Log($"Checksum OK: {actual}");
        }
        else if (!options.NoChecksum)
            Log("No matching checksum manifest detected; file was not checksum-verified.");

        // Same-directory rename: never remove an existing destination on a failed transfer or checksum.
        File.Move(part, output, true);
        File.Delete(metaPath);
        File.Delete(metaPath + ".tmp");
        Log($"Saved: {output}");
        if (options.PrintHash)
        {
            var hash = expected?.Algorithm == "SHA-256" && streamedHash is not null
                ? streamedHash : await Hashing.ComputeAsync(output, "SHA-256", token);
            Console.WriteLine($"SHA-256: {hash}");
        }
    }

    private async Task<bool> AlreadyVerifiedAsync(string path, ExpectedHash expected, CancellationToken token)
    {
        if (!File.Exists(path))
            return false;
        var actual = await Hashing.ComputeAsync(path, expected.Algorithm, token);
        if (!EqualHash(expected.Hash, actual))
            return false;
        Log("File already downloaded and verified. Skipping download.");
        if (options.PrintHash)
        {
            var hash = expected.Algorithm == "SHA-256" ? actual
                : await Hashing.ComputeAsync(path, "SHA-256", token);
            Console.WriteLine($"SHA-256: {hash}");
        }
        return true;
    }

    /// <summary>No body deadline: large downloads remain cancellable without a fixed duration limit.</summary>
    private async Task<string?> DownloadStreamAsync(string part, ExpectedHash? expected, CancellationToken token)
    {
        using var request = Request(HttpMethod.Get);
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
        response.EnsureSuccessStatusCode();
        if (response.StatusCode != HttpStatusCode.OK)
            throw new IOException($"Unexpected HTTP {(int)response.StatusCode} for a full download");
        await using var input = await response.Content.ReadAsStreamAsync(token);
        await using var output = new FileStream(part, FileMode.Create, FileAccess.Write, FileShare.Read,
            1, FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var digest = expected is null ? null : IncrementalHash.CreateHash(Hashing.AlgorithmName(expected.Algorithm));
        await using var progress = new Progress(Path.GetFileName(part), response.Content.Headers.ContentLength ?? -1,
            0, options.Quiet);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        long downloaded = 0;
        try
        {
            int read;
            while ((read = await input.ReadAsync(buffer.AsMemory(0, BufferSize), token)) > 0)
            {
                await output.WriteAsync(buffer.AsMemory(0, read), token);
                digest?.AppendData(buffer, 0, read);
                downloaded += read;
                progress.Advance(read);
            }
            if (response.Content.Headers.ContentLength is long total && downloaded != total)
                throw new IOException($"Incomplete response: received {downloaded} of {total} bytes");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return digest is null ? null : Convert.ToHexStringLower(digest.GetHashAndReset());
    }

    private async Task DownloadChunksAsync(string part, string metaPath, Resume state, CancellationToken token)
    {
        using var file = File.OpenHandle(part, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await using var progress = new Progress(Path.GetFileName(part), state.Length,
            state.Chunks.Sum(c => c.Downloaded), options.Quiet);
        using var saverStop = new CancellationTokenSource();
        var saver = SavePeriodicallyAsync(state, metaPath, saverStop.Token);
        try
        {
            await Task.WhenAll(state.Chunks.Where(c => !c.Complete).Select(async chunk =>
            {
                var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
                try
                {
                    for (var attempt = 0; ; attempt++)
                    {
                        try
                        {
                            var start = chunk.Start + chunk.Downloaded;
                            if (start > chunk.End)
                                return;
                            using var request = Request(HttpMethod.Get);
                            request.Headers.Range = new RangeHeaderValue(start, chunk.End);
                            request.Headers.Remove("If-Match");
                            request.Headers.TryAddWithoutValidation("If-Match", state.Etag);
                            using var response = await client.SendAsync(request,
                                HttpCompletionOption.ResponseHeadersRead, token);
                            if (response.StatusCode != HttpStatusCode.PartialContent)
                                throw new IOException($"Server returned HTTP {(int)response.StatusCode} for range {start}-{chunk.End}");
                            if (Header(response, "ETag") != state.Etag)
                                throw new IOException("ETag changed during ranged download");
                            var range = response.Content.Headers.ContentRange;
                            if (range is null || range.Unit != "bytes" || range.From != start
                                || range.To != chunk.End || range.Length != state.Length)
                                throw new IOException($"Invalid Content-Range for bytes={start}-{chunk.End}");
                            var remaining = chunk.End - start + 1;
                            if (response.Content.Headers.ContentLength is long bodyLength && bodyLength != remaining)
                                throw new IOException("Range body length does not match Content-Range");
                            await using var input = await response.Content.ReadAsStreamAsync(token);
                            while (remaining > 0)
                            {
                                var read = await input.ReadAsync(buffer.AsMemory(0, (int)Math.Min(BufferSize, remaining)), token);
                                if (read == 0)
                                    throw new IOException($"Connection closed with {remaining} bytes unread in chunk {chunk.Index}");
                                await RandomAccess.WriteAsync(file, buffer.AsMemory(0, read), start, token);
                                chunk.Advance(read);
                                start += read;
                                remaining -= read;
                                progress.Advance(read);
                            }
                            return;
                        }
                        catch (Exception error) when (attempt < options.Retries && !token.IsCancellationRequested
                            && error is HttpRequestException or IOException)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(500.0 * attempt), token);
                        }
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }));
        }
        finally
        {
            saverStop.Cancel();
            await saver;
            state.Save(metaPath);
        }
    }

    private static async Task SavePeriodicallyAsync(Resume state, string path, CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        try
        {
            while (await timer.WaitForNextTickAsync(token))
                state.Save(path);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private HttpRequestMessage Request(HttpMethod method)
    {
        var request = new HttpRequestMessage(method, uri);
        request.Headers.TryAddWithoutValidation("User-Agent", options.UserAgent ?? "DotnetFetch/2.0");
        foreach (var header in options.Headers)
        {
            var colon = header.IndexOf(':');
            if (colon <= 0)
                continue;
            var name = header[..colon].Trim();
            var value = header[(colon + 1)..].Trim();
            if (!request.Headers.TryAddWithoutValidation(name, value))
            {
                request.Content ??= new ByteArrayContent([]);
                if (!request.Content.Headers.TryAddWithoutValidation(name, value))
                    throw new ArgumentException($"Invalid HTTP header: {name}");
            }
        }
        return request;
    }

    private static string? Header(HttpResponseMessage response, string name) =>
        response.Headers.TryGetValues(name, out var values) ? values.FirstOrDefault()
        : response.Content.Headers.TryGetValues(name, out values) ? values.FirstOrDefault() : null;

    private static bool IsStrongEtag(string? value) => value is { Length: >= 2 }
        && value[0] == '"' && value.IndexOf('"', 1) == value.Length - 1;

    private static bool EqualHash(string expected, string actual) =>
        expected.Equals(actual, StringComparison.OrdinalIgnoreCase);

    private static string SafeFilename(string path)
    {
        var name = path.Replace('\\', '/');
        name = name[(name.LastIndexOf('/') + 1)..];
        if (name is "" or "." or "..")
            return "downloaded_file";
        if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new ArgumentException("Remote filename is invalid on this platform; specify --output.");
        return name;
    }

    private void Log(string message)
    {
        if (!options.Quiet)
            Console.WriteLine(message);
    }
}

internal sealed class Options
{
    internal Uri? Uri { get; private set; }
    internal string? Output { get; private set; }
    internal int Connections { get; private set; } = 4;
    internal bool Quiet { get; private set; }
    internal List<string> Headers { get; } = [];
    internal string? UserAgent { get; private set; }
    internal bool NoResume { get; private set; }
    internal bool NoChecksum { get; private set; }
    internal string? ExplicitHash { get; private set; }
    internal bool PrintHash { get; private set; }
    internal int Retries { get; private set; }
    internal bool Help { get; private set; }
    internal bool Version { get; private set; }

    internal const string HelpText = """
        Usage: fetch [OPTIONS] URL
        High-performance multi-threaded CLI file downloader with auto-checksum verification

          -o, --output PATH          Target file output path (or directory)
          -c, --connections NUMBER   Concurrent connections (default: 4; clamped to 1–64)
          -q, --quiet                Disable progress and non-essential logs
          -H, --header HEADER        Custom HTTP header; may be repeated
          -A, --user-agent VALUE     Custom User-Agent string
              --no-resume            Start fresh instead of resuming a partial download
              --no-checksum          Skip automatic checksum discovery and verification
              --expected-hash HASH   Explicit hexadecimal hash, optionally prefixed by algo:
              --print-hash           Print the downloaded file's SHA-256 hash
              --retries NUMBER       Retries per failed chunk (default: 0)
          -h, --help                 Show this help
          -V, --version              Show version information

        URL must use HTTP or HTTPS. Use -- to end option parsing.
        Hash algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512 (detected by hash length).
        Long options also accept --key=value. Custom headers apply to downloads, not checksum probes.
        """;

    internal static Options Parse(string[] args)
    {
        var result = new Options { Help = args.Length == 0 };
        string? target = null;
        bool positionalOnly = false;

        for (int index = 0; index < args.Length; index++)
        {
            string argument = args[index];
            if (!positionalOnly && argument == "--")
            {
                positionalOnly = true;
                continue;
            }

            if (!positionalOnly && argument.StartsWith("--", StringComparison.Ordinal))
            {
                int equals = argument.IndexOf('=');
                string option = equals < 0 ? argument : argument[..equals];
                string? attached = equals < 0 ? null : argument[(equals + 1)..];
                Apply(option, attached, ref index);
            }
            else if (!positionalOnly && argument.Length > 1 && argument[0] == '-')
            {
                // Picocli-style short options allow both -c4 and clusters such as -qc4.
                for (int offset = 1; offset < argument.Length; offset++)
                {
                    string option = argument[offset] switch
                    {
                        'o' => "--output",
                        'c' => "--connections",
                        'q' => "--quiet",
                        'H' => "--header",
                        'A' => "--user-agent",
                        'h' => "--help",
                        'V' => "--version",
                        _ => throw new ArgumentException($"Unknown option '-{argument[offset]}'. Use --help for usage.")
                    };
                    bool takesValue = option is "--output" or "--connections" or "--header" or "--user-agent";
                    string? attached = null;
                    if (offset + 1 < argument.Length && argument[offset + 1] == '=')
                    {
                        attached = argument[(offset + 2)..];
                        offset = argument.Length;
                    }
                    else if (takesValue && offset + 1 < argument.Length)
                    {
                        attached = argument[(offset + 1)..];
                        offset = argument.Length;
                    }
                    Apply(option, attached, ref index);
                    if (takesValue)
                        break;
                }
            }
            else
            {
                if (target is not null)
                    throw new ArgumentException($"Unexpected argument '{argument}': only one URL is accepted.");
                target = argument;
            }
        }

        if (result.NoChecksum && result.ExplicitHash is not null)
            throw new ArgumentException("--expected-hash cannot be combined with --no-checksum.");
        if (result.ExplicitHash is not null)
            _ = Hashing.ParseExplicit(result.ExplicitHash);

        if (target is not null)
        {
            if (!System.Uri.TryCreate(target, UriKind.Absolute, out Uri? uri)
                || (uri.Scheme != System.Uri.UriSchemeHttp && uri.Scheme != System.Uri.UriSchemeHttps)
                || string.IsNullOrEmpty(uri.Host))
                throw new ArgumentException($"Invalid URL '{target}': an absolute HTTP or HTTPS URL is required.");
            result.Uri = uri;
        }
        else if (!result.Help && !result.Version)
        {
            throw new ArgumentException("A target URL is required. Use --help for usage.");
        }

        return result;

        void Apply(string option, string? attached, ref int index)
        {
            switch (option)
            {
                case "--output": result.Output = Value(option, attached, ref index); break;
                case "--connections":
                    result.Connections = Math.Clamp(Integer(option, Value(option, attached, ref index)), 1, 64);
                    break;
                case "--quiet": result.Quiet = Boolean(option, attached); break;
                case "--header": result.Headers.Add(Value(option, attached, ref index)); break;
                case "--user-agent": result.UserAgent = Value(option, attached, ref index); break;
                case "--no-resume": result.NoResume = Boolean(option, attached); break;
                case "--no-checksum": result.NoChecksum = Boolean(option, attached); break;
                case "--expected-hash": result.ExplicitHash = Value(option, attached, ref index); break;
                case "--print-hash": result.PrintHash = Boolean(option, attached); break;
                case "--retries":
                    result.Retries = Math.Max(0, Integer(option, Value(option, attached, ref index)));
                    break;
                case "--help": result.Help = Boolean(option, attached); break;
                case "--version": result.Version = Boolean(option, attached); break;
                default: throw new ArgumentException($"Unknown option '{option}'. Use --help for usage.");
            }
        }

        string Value(string option, string? attached, ref int index)
        {
            if (attached is not null)
            {
                if (attached.Length == 0)
                    throw new ArgumentException($"Option '{option}' requires a value.");
                return attached;
            }
            if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Option '{option}' requires a value.");
            return args[++index];
        }
    }

    private static int Integer(string option, string value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int number))
            throw new ArgumentException($"Option '{option}' requires a 32-bit integer, not '{value}'.");
        return number;
    }

    private static bool Boolean(string option, string? value)
    {
        if (value is null)
            return true;
        if (bool.TryParse(value, out bool enabled))
            return enabled;
        throw new ArgumentException($"Option '{option}' expects true or false, not '{value}'.");
    }
}

internal sealed record ExpectedHash(string Algorithm, string Hash, string Source);

internal static class Hashing
{
    private const int MaxManifestBytes = 64 * 1024;
    private static readonly TimeSpan ManifestTimeout = TimeSpan.FromSeconds(3);
    private static readonly string[] ManifestNames =
    [
        "SHA512SUMS", "SHA256SUMS", "SHA512", "SHA256", "MD5SUMS", "MD5",
        "CHECKSUMS", "CHECKSUM", "sha512sums.txt", "sha256sums.txt", "sha512sum.txt", "sha256sum.txt"
    ];

    /// <summary>The Java CLI ignores an optional prefix and detects the algorithm from the hexadecimal length.</summary>
    internal static ExpectedHash ParseExplicit(string value)
    {
        ReadOnlySpan<char> hash = value.AsSpan().Trim();
        int colon = hash.IndexOf(':');
        if (colon >= 0)
            hash = hash[(colon + 1)..].Trim();
        string? algorithm = InferAlgorithm(hash.Length);
        if (algorithm is null || !IsHex(hash))
            throw new ArgumentException("--expected-hash requires a hexadecimal MD5, SHA-1, SHA-256, SHA-384, or SHA-512 hash (32, 40, 64, 96, or 128 characters), optionally prefixed by algo:.");
        return new ExpectedHash(algorithm, hash.ToString().ToLowerInvariant(), "user-provided");
    }

    /// <summary>Probes the original manifest order without adding the download's custom headers.</summary>
    internal static async Task<ExpectedHash?> FindAsync(HttpClient client, Uri uri, string remoteFilename,
        string localFilename, CancellationToken token)
    {
        Uri baseUri = new(uri, "./");
        bool differentNames = !string.Equals(remoteFilename, localFilename, StringComparison.Ordinal);
        int candidateCount = ManifestNames.Length + (differentNames ? 4 : 2);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(MaxManifestBytes + 1);
        try
        {
            for (int index = 0; index < candidateCount; index++)
            {
                token.ThrowIfCancellationRequested();
                string candidate = index < ManifestNames.Length ? ManifestNames[index]
                    : (index - ManifestNames.Length) switch
                    {
                        0 => remoteFilename + ".sha256",
                        1 => remoteFilename + ".sha512",
                        2 => localFilename + ".sha256",
                        _ => localFilename + ".sha512"
                    };
                // Escape the entire filename so a literal '#' or '?' is not interpreted as a URI component.
                Uri manifestUri = new(baseUri, Uri.EscapeDataString(candidate));
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeout.CancelAfter(ManifestTimeout);
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, manifestUri);
                    using HttpResponseMessage response = await client.SendAsync(request,
                        HttpCompletionOption.ResponseHeadersRead, timeout.Token).ConfigureAwait(false);
                    if (response.StatusCode != HttpStatusCode.OK
                        || response.Content.Headers.ContentLength is > MaxManifestBytes)
                        continue;

                    await using Stream body = await response.Content.ReadAsStreamAsync(timeout.Token).ConfigureAwait(false);
                    int length = 0;
                    while (length <= MaxManifestBytes)
                    {
                        int read = await body.ReadAsync(buffer.AsMemory(length, MaxManifestBytes + 1 - length),
                            timeout.Token).ConfigureAwait(false);
                        if (read == 0)
                            break;
                        length += read;
                    }
                    if (length > MaxManifestBytes)
                        continue;
                    timeout.Token.ThrowIfCancellationRequested();

                    string text = Encoding.UTF8.GetString(buffer, 0, length);
                    string? algorithm = CandidateAlgorithm(candidate);
                    string? hash = ExtractHash(text, remoteFilename, algorithm);
                    if (hash is null && differentNames)
                        hash = ExtractHash(text, localFilename, algorithm);
                    if (hash is not null)
                        return new ExpectedHash(algorithm ?? InferAlgorithm(hash.Length)!, hash, candidate);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    // A manifest's deadline is independent of the download and other candidates.
                }
                catch (HttpRequestException)
                {
                    // Missing or inaccessible manifests are normal; try the next candidate.
                }
                catch (IOException)
                {
                    // Truncated or unreadable manifest bodies are not usable checksums.
                }
            }
            token.ThrowIfCancellationRequested();
            return null;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Hashes the file incrementally without buffering the downloaded payload in memory.</summary>
    internal static async Task<string> ComputeAsync(string path, string algorithm, CancellationToken token)
    {
        using var hash = IncrementalHash.CreateHash(AlgorithmName(algorithm));
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1, FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(128 * 1024);
        try
        {
            int read;
            while ((read = await file.ReadAsync(buffer.AsMemory(), token).ConfigureAwait(false)) != 0)
                hash.AppendData(buffer, 0, read);
            token.ThrowIfCancellationRequested();
            return Convert.ToHexStringLower(hash.GetHashAndReset());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static HashAlgorithmName AlgorithmName(string algorithm) => algorithm switch
    {
        "MD5" => HashAlgorithmName.MD5,
        "SHA-1" or "SHA1" => HashAlgorithmName.SHA1,
        "SHA-256" or "SHA256" => HashAlgorithmName.SHA256,
        "SHA-384" or "SHA384" => HashAlgorithmName.SHA384,
        "SHA-512" or "SHA512" => HashAlgorithmName.SHA512,
        _ => throw new ArgumentException($"Unsupported hash algorithm '{algorithm}'.", nameof(algorithm))
    };

    private static string? CandidateAlgorithm(string candidate)
    {
        // Generic manifests may mix algorithms. Named manifests define their algorithm instead.
        if (candidate is "CHECKSUMS" or "CHECKSUM")
            return null;
        if (candidate.EndsWith(".sha256", StringComparison.OrdinalIgnoreCase))
            return "SHA-256";
        if (candidate.EndsWith(".sha512", StringComparison.OrdinalIgnoreCase))
            return "SHA-512";
        if (candidate.Contains("sha512", StringComparison.OrdinalIgnoreCase)
            || candidate.Contains("sha-512", StringComparison.OrdinalIgnoreCase))
            return "SHA-512";
        if (candidate.Contains("sha384", StringComparison.OrdinalIgnoreCase)
            || candidate.Contains("sha-384", StringComparison.OrdinalIgnoreCase))
            return "SHA-384";
        if (candidate.Contains("sha1", StringComparison.OrdinalIgnoreCase)
            || candidate.Contains("sha-1", StringComparison.OrdinalIgnoreCase))
            return "SHA-1";
        if (candidate.Contains("md5", StringComparison.OrdinalIgnoreCase))
            return "MD5";
        return "SHA-256";
    }

    private static string? ExtractHash(string text, string filename, string? algorithm)
    {
        foreach (ReadOnlySpan<char> original in text.AsSpan().EnumerateLines())
        {
            ReadOnlySpan<char> line = original.Trim().TrimStart('\uFEFF');
            if (line.IsEmpty || line[0] == '#')
                continue;

            // BSD: SHA256 (path/to/file name) = hash, including MD5(filename)=hash.
            int open = line.IndexOf('(');
            int close = line.LastIndexOf(')');
            int equals = line.LastIndexOf('=');
            if (open >= 0 && close > open && equals > close
                && MatchesFilename(line[(open + 1)..close].Trim(), filename))
            {
                ReadOnlySpan<char> bsdHash = line[(equals + 1)..].Trim();
                if (IsMatchingHash(bsdHash, algorithm))
                    return bsdHash.ToString().ToLowerInvariant();
            }

            // GNU: hash [* ]path/to/file name. Keep spaces inside the filename intact.
            int separator = 0;
            while (separator < line.Length && !char.IsWhiteSpace(line[separator]))
                separator++;
            ReadOnlySpan<char> hash = line[..separator];
            if (!IsMatchingHash(hash, algorithm))
                continue;
            if (separator == line.Length)
                return hash.ToString().ToLowerInvariant(); // Raw hash sidecar.
            ReadOnlySpan<char> target = line[separator..].Trim();
            if (!target.IsEmpty && target[0] == '*')
                target = target[1..].Trim();
            if (MatchesFilename(target, filename))
                return hash.ToString().ToLowerInvariant();
        }
        return null;
    }

    private static bool MatchesFilename(ReadOnlySpan<char> target, string filename)
    {
        return target.SequenceEqual(filename.AsSpan())
            || (target.Length > filename.Length && target[target.Length - filename.Length - 1] == '/'
                && target.EndsWith(filename.AsSpan(), StringComparison.Ordinal));
    }

    private static bool IsMatchingHash(ReadOnlySpan<char> hash, string? algorithm)
    {
        string? inferred = InferAlgorithm(hash.Length);
        return inferred is not null && (algorithm is null || inferred == algorithm) && IsHex(hash);
    }

    private static string? InferAlgorithm(int length) => length switch
    {
        32 => "MD5",
        40 => "SHA-1",
        64 => "SHA-256",
        96 => "SHA-384",
        128 => "SHA-512",
        _ => null
    };

    private static bool IsHex(ReadOnlySpan<char> hash)
    {
        foreach (char character in hash)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f') and not (>= 'A' and <= 'F'))
                return false;
        }
        return true;
    }
}

/// <summary>A single contiguous range; progress advances only after completed file writes.</summary>
internal sealed class Chunk(int index, long start, long end, long downloaded = 0)
{
    public int Index { get; } = index;
    public long Start { get; } = start;
    public long End { get; } = end;
    private long downloaded = downloaded;
    public long Downloaded => Interlocked.Read(ref downloaded);
    public bool Complete => Downloaded == End - Start + 1;
    public void Advance(int bytes) => Interlocked.Add(ref downloaded, bytes);
}

/// <summary>Version-one metadata compatible with the Java fetch .part.meta format.</summary>
internal sealed record Resume(string Uri, long Length, string? Etag, string? LastModified, List<Chunk> Chunks)
{
    public static List<Chunk> CreateChunks(long length, int workers)
    {
        var size = length / workers + (length % workers == 0 ? 0 : 1);
        var chunks = new List<Chunk>(workers);
        for (long start = 0; start < length;)
        {
            var end = start + Math.Min(size, length - start) - 1;
            chunks.Add(new Chunk(chunks.Count, start, end));
            start = end + 1;
        }
        return chunks;
    }

    /// <summary>Rejects changed entities, incomplete range maps and progress beyond physical file bounds.</summary>
    public static Resume? Load(string path, string part, Uri uri, long length, string? etag, string? modified)
    {
        try
        {
            if (etag is null || length <= 0 || new FileInfo(path).Length > 64 * 1024)
                return null;
            string? savedUri = null, savedEtag = null, savedModified = null;
            long savedLength = -1;
            int version = 0, count = -1;
            var chunks = new List<Chunk>();
            foreach (var raw in File.ReadLines(path))
            {
                var line = raw.Trim();
                var eq = line.IndexOf('=');
                if (eq < 0 || line.StartsWith('#'))
                    continue;
                var value = line[(eq + 1)..].Trim();
                switch (line[..eq].Trim())
                {
                    case "version": version = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "uri": savedUri = value; break;
                    case "contentLength": savedLength = long.Parse(value, CultureInfo.InvariantCulture); break;
                    case "etag": savedEtag = value; break;
                    case "lastModified": savedModified = value; break;
                    case "chunks": count = int.Parse(value, CultureInfo.InvariantCulture); break;
                    case "chunk":
                        var fields = value.Split(',');
                        if (fields.Length != 4 || chunks.Count >= 64)
                            return null;
                        chunks.Add(new Chunk(int.Parse(fields[0], CultureInfo.InvariantCulture),
                            long.Parse(fields[1], CultureInfo.InvariantCulture),
                            long.Parse(fields[2], CultureInfo.InvariantCulture),
                            long.Parse(fields[3], CultureInfo.InvariantCulture)));
                        break;
                }
            }
            if (version != 1 || savedUri != uri.OriginalString || savedLength != length || savedEtag != etag
                || (modified is not null && savedModified is not null && modified != savedModified)
                || chunks.Count == 0 || chunks.Count != count)
                return null;
            var physicalLength = new FileInfo(part).Length;
            if (physicalLength > length)
                return null;
            long next = 0;
            for (var i = 0; i < chunks.Count; i++)
            {
                var chunk = chunks[i];
                if (chunk.Index != i || chunk.Start != next || chunk.End < chunk.Start || chunk.End >= length
                    || chunk.Downloaded < 0 || chunk.Downloaded > chunk.End - chunk.Start + 1
                    || (chunk.Downloaded > 0 && chunk.Start + chunk.Downloaded > physicalLength))
                    return null;
                next = chunk.End + 1;
            }
            return next == length ? new Resume(savedUri!, length, etag, modified, chunks) : null;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException
            or FormatException or OverflowException)
        {
            return null;
        }
    }

    /// <summary>Atomically replaces the last snapshot; no partially written metadata is ever loaded.</summary>
    public void Save(string path)
    {
        var text = new StringBuilder();
        text.Append("version=1\nuri=").Append(Uri).Append("\ncontentLength=")
            .Append(Length.ToString(CultureInfo.InvariantCulture)).Append('\n');
        if (Etag is not null)
            text.Append("etag=").Append(Etag).Append('\n');
        if (LastModified is not null)
            text.Append("lastModified=").Append(LastModified).Append('\n');
        text.Append("chunks=").Append(Chunks.Count.ToString(CultureInfo.InvariantCulture)).Append('\n');
        foreach (var chunk in Chunks)
            text.Append("chunk=").Append(chunk.Index.ToString(CultureInfo.InvariantCulture)).Append(',')
                .Append(chunk.Start.ToString(CultureInfo.InvariantCulture)).Append(',')
                .Append(chunk.End.ToString(CultureInfo.InvariantCulture)).Append(',')
                .Append(chunk.Downloaded.ToString(CultureInfo.InvariantCulture)).Append('\n');
        File.WriteAllText(path + ".tmp", text.ToString(), new UTF8Encoding(false));
        File.Move(path + ".tmp", path, true);
    }
}

/// <summary>Bounded interactive progress and coarse redirected diagnostics; never writes to result stdout.</summary>
internal sealed class Progress : IAsyncDisposable
{
    private readonly string name;
    private readonly long total;
    private readonly long initial;
    private readonly bool quiet;
    private readonly bool interactive;
    private readonly Stopwatch elapsed = Stopwatch.StartNew();
    private readonly CancellationTokenSource stop = new();
    private readonly Task renderer;
    private long transferred;
    private int lastPercent = -20;

    public Progress(string name, long total, long initial, bool quiet)
    {
        this.name = name;
        this.total = total;
        this.initial = initial;
        this.quiet = quiet;
        interactive = !Console.IsErrorRedirected;
        renderer = quiet ? Task.CompletedTask : RenderLoopAsync();
    }

    public void Advance(int bytes) => Interlocked.Add(ref transferred, bytes);

    private async Task RenderLoopAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(interactive ? 100 : 1000));
        try
        {
            while (await timer.WaitForNextTickAsync(stop.Token))
                Render();
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private void Render()
    {
        var session = Interlocked.Read(ref transferred);
        var current = initial + session;
        var speed = session / 1048576.0 / Math.Max(0.001, elapsed.Elapsed.TotalSeconds);
        var percent = total > 0 ? Math.Min(100.0, current * 100.0 / total) : 0;
        if (!interactive && (total <= 0 || (int)percent < lastPercent + 20))
            return;
        lastPercent = (int)percent;
        var line = total > 0
            ? $"{name}: {percent:F1}% {current / 1048576.0:F2}/{total / 1048576.0:F2} MB {speed:F2} MB/s "
                + $"ETA {(speed > 0 ? Math.Max(0, total - current) / 1048576.0 / speed : 0):F0}s"
            : $"{name}: {current / 1048576.0:F2} MB {speed:F2} MB/s";
        if (interactive)
        {
            var width = Math.Max(1, Console.WindowWidth - 1);
            if (line.Length > width && total > 0)
                line = $"{percent:F1}% {current / 1048576.0:F2}/{total / 1048576.0:F2} MB {speed:F2} MB/s";
            line = line[..Math.Min(line.Length, width)];
            Console.Error.Write('\r' + line.PadRight(width));
        }
        else
            Console.Error.WriteLine(line);
    }

    public async ValueTask DisposeAsync()
    {
        stop.Cancel();
        await renderer;
        if (!quiet)
        {
            Render();
            if (interactive)
                Console.Error.WriteLine();
        }
        stop.Dispose();
    }
}
