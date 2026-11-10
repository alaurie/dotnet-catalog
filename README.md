# dotnet-catalog

Single-file .NET 10 utilities with embedded build settings and standalone Native AOT publishing. Published native binaries do not require a .NET runtime installation.

The launcher is maintained separately in [dotnet-runr](../dotnet-runr), a conventional .NET project. After installing `drun`, run this catalog's apps from this checkout:

```powershell
drun run fetch https://example.com/file.iso --print-hash
```

GitHub shorthand (`drun run fetch@alaurie ...`) requires this catalog's manifest and app sources to be published publicly as `alaurie/dotnet-catalog`.


## fetch

A dependency-free, single-file C# port of [`fetch` from jbang-catalog](https://github.com/ALaurie/jbang-catalog), retaining concurrent range downloads, safe resume, automatic checksum discovery, explicit verification, custom headers, retries and SHA-256 output. Source and build settings live together in `apps/fetch/fetch.cs`; no application project file or helper source files are required. The Java repository is unchanged.

### Run from source

Requires the .NET 10 SDK. `global.json` selects SDK 10.0.403 or a newer .NET 10 feature band. From the repository root:

```powershell
dotnet run --file apps/fetch/fetch.cs -- https://example.com/file.iso -c 8 --print-hash
dotnet run --file apps/fetch/fetch.cs -- --help
```

Arguments after `--` go to fetch. The SDK builds and runs the single source file; C++ Build Tools are **not** required for this workflow. The source can also be copied elsewhere and run on its own. There are no NuGet dependencies to restore.

### Build standalone Native AOT

The `#:property` directives at the top of `fetch.cs` preserve the native size settings. On Windows, publishing also requires Visual Studio 2022+ Build Tools with the **Desktop development with C++** workload and its default Windows SDK components:

```powershell
dotnet publish apps/fetch/fetch.cs -c Release -r win-x64 -o artifacts/fetch/win-x64
```

The publish directory contains **only `fetch.exe`**. The verified Windows x64 build with SDK 10.0.403 is **4,529,152 bytes (4.319 MiB)**. Copy that file wherever desired; no adjacent DLLs, configuration files or extraction step are required. Sizes can change with SDK updates.

For Linux or macOS, build on the target OS with its native compiler prerequisites and the corresponding RID, for example `linux-x64` or `osx-arm64`:

```sh
dotnet publish apps/fetch/fetch.cs -c Release -r linux-x64 -o artifacts/fetch/linux-x64
```

Only Windows x64 has been exercised here. See Microsoft's [Native AOT prerequisites and platform restrictions](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/).

### Usage

```powershell
.\artifacts\fetch\win-x64\fetch.exe https://example.com/file.iso
.\artifacts\fetch\win-x64\fetch.exe https://example.com/file.iso -o "$HOME\Downloads\" -c 8 --retries 3 --print-hash
.\artifacts\fetch\win-x64\fetch.exe https://example.com/file.iso --expected-hash <hexadecimal-digest>
.\artifacts\fetch\win-x64\fetch.exe https://example.com/private.zip -H "Authorization: Bearer token" -A "MyDownloader/1.0"
```

Existing directories, or output paths ending in `/` or `\`, receive the remote filename; parent directories are created. Without an explicit output filename, `Content-Disposition` can supply one; path components are stripped. An empty URL filename defaults to `downloaded_file`.

Downloads are written to a sibling `.part` file. The destination is replaced only after a complete transfer and any requested checksum verification. A failure leaves an existing destination untouched. Verified existing files are skipped; `--print-hash` still prints their digest.

Range transfers require a strong ETag. Every response must be HTTP 206 with a matching ETag and exact Content-Range. `.part.meta` records completed writes every 500 ms and on completion, failure or Ctrl+C. Its version-one text format is compatible with Java fetch. Resume requires the same URL, length and strong ETag, consistent Last-Modified values when present, a complete range map and physically present recorded bytes. Invalid state and completed partials with bad checksums restart fresh. `--no-resume` discards partial state. Servers without strong ETags or range support use a fresh single-stream transfer.

Automatic verification probes the original ordered checksum manifests (`SHA512SUMS`, `SHA256SUMS`, `SHA512`, `SHA256`, `MD5SUMS`, `MD5`, `CHECKSUMS`, `CHECKSUM`, lowercase SHA sum text files), then remote and renamed-local `.sha256`/`.sha512` sidecars. BSD, GNU and raw-hash formats are supported. Each probe has a three-second body-inclusive deadline and a 64-KiB cap. Custom download headers are **not** sent to manifest probes. Explicit hashes bypass probing; their algorithm is inferred by length, including SHA-384. An optional `algo:` prefix is accepted but does not override length detection. `--no-checksum` and `--expected-hash` are mutually exclusive. Missing manifests are reported unless quiet; HTTPS/checksum integrity still depends on the trustworthiness of the checksum source.

Connections have a 15-second timeout; downloads have no fixed total-duration timeout. Stalled bodies require Ctrl+C. Retries apply to failed range requests, continuing after successfully written bytes, not to single-stream transfers. Exit codes: `0` success/help, `1` download or verification failure, `2` argument errors, `130` Ctrl+C. Results and status messages use stdout; errors and progress use stderr. `--quiet --print-hash` emits only the requested digest line on success.

### Options

Verbatim output of the published executable's `--help`:

```text
Usage: fetch [OPTIONS] URL
High-performance multi-threaded CLI file downloader with auto-checksum verification

  -o, --output PATH          Target file output path (or directory)
  -c, --connections NUMBER   Concurrent connections (default: 4; clamped to 1-64)
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
```

### Size choices

The source header enables Native AOT's implicit trimming, `OptimizationPreference=Size`, invariant globalization, stripped symbols, and no third-party packages. HTTP/3, stack trace generation, debugger support, EventSource, metrics, HTTP diagnostic propagation and framework exception message resources are disabled. HTTP/1.1, HTTP/2 and certificate-validated HTTPS remain available. No executable packer is used.

Tradeoffs: culture-specific formatting/localization and rich runtime diagnostics are unavailable; some framework failures display resource identifiers rather than full English explanations. Size optimization can reduce peak throughput. These settings favor a compact portable-to-the-same-platform executable without dropping fetch's download safety checks. See Microsoft's [AOT size guidance](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/optimizing) and [trimming feature switches](https://learn.microsoft.com/en-us/dotnet/core/deploying/trimming/trimming-options#trim-framework-library-features).

### Verification

Run the dependency-free integration harness against the actual published binary:

```powershell
dotnet run --project tests/Fetch.Smoke -c Release -- artifacts/fetch/win-x64/fetch.exe
```

After consolidation, file-based help execution, standalone source execution outside the repository (complete downloaded bytes and explicit SHA-256 verification), Native AOT publishing, and all nine native integration scenarios passed. Those scenarios cover multibuffer concurrent conditional ranges; interruption and cross-process resume; checksum failure preserving the destination and repairing a bad completed partial; malformed Content-Range rejection; changed ETag rejection; weak/missing ETag streaming fallback; custom headers, directory output and printed hashes; oversized chunked-manifest fallback with renamed output; and chunk retry offsets. The earlier port also passed a real HTTPS download of the .NET runtime license with explicit SHA-256 verification.

## License

MIT. Ported from ALaurie's jbang-catalog; original copyright retained in `LICENSE`.
