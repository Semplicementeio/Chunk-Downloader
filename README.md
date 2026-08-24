# ⚡ Chunk Downloader

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat&logo=go)](https://go.dev/)
[![CI Status](https://github.com/semplicementeio/Chunk-Downloader/actions/workflows/ci.yml/badge.svg)](https://github.com/semplicementeio/Chunk-Downloader/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/semplicementeio/chunk-downloader)](https://goreportcard.com/report/github.com/semplicementeio/chunk-downloader)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, concurrent, and resumable HTTP downloader written in Go. It uses HTTP range requests, adaptive worker concurrency, checkpoint-based recovery, SHA-256 integrity verification, and optional Prometheus telemetry.

The project was built as an exploration of concurrent network I/O, resumable transfers, adaptive scheduling, and fault-tolerant file handling in Go.

## 🎯 Key Features

- **Concurrent Range Downloads**: Splits files into HTTP Range chunks downloaded across configurable goroutine worker pools.
- **Direct Chunk Writes**: Writes each downloaded range directly to its final byte offset using `os.File.WriteAt`, avoiding a separate file-merging phase.
- **Checkpoint-Based Resumability**: Tracks real-time chunk state in `.chk.json` manifest files, updated atomically to seamlessly resume downloads after network loss or process interruptions (`SIGINT`/`SIGTERM`).
- **Adaptive Concurrency**: Dynamically tunes worker pool size based on real-time throughput telemetry to prevent network link congestion or server-side connection throttling.
- **Integrity Verification**: Verifies SHA-256 file hashes before final atomic renaming.

## 🏗 System Architecture

```
                    ┌────────────────────────┐
                    │    HTTP Web Server     │
                    └───────────┬────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 │ HTTP Range Request Probing  │
                 └──────────────┬──────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 │  Resumable JSON Manifest    │
                 │     (.file.chk.json)        │
                 └──────────────┬──────────────┘
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
      ┌─────────────┐                       ┌─────────────┐
      │  Worker 1   │                       │  Worker N   │
      └──────┬──────┘                       └──────┬──────┘
             │                                     │
             └──────────────────┬──────────────────┘
                                │
                     ┌──────────▼──────────┐
                     │ Direct Chunk Write  │
                     │  (os.File.WriteAt)  │
                     └──────────┬──────────┘
                                │
                     ┌──────────▼──────────┐
                     │ All Chunks Done     │
                     └──────────┬──────────┘
                                │
                     ┌──────────▼──────────┐
                     │ SHA-256 Checksum    │
                     └──────────┬──────────┘
                         ┌──────┴──────┐
                         ▼             ▼
                    [ Valid ]     [ Invalid ]
                         │             │
                         ▼             ▼
                   Atomic Rename  Mark Failed &
                   to Target      Reject File
```

### 📂 Key Implementation Areas

- [`cmd/downloader/main.go`](cmd/downloader/main.go): CLI entry point, flag parsing, and signal handling (`SIGINT`/`SIGTERM`).
- [`pkg/downloader/engine.go`](pkg/downloader/engine.go): Core download orchestrator, range probing, `WriteAt` dispatching, and SHA-256 verification.
- [`pkg/downloader/retry.go`](pkg/downloader/retry.go): Exponential backoff retry handler with randomized jitter.
- [`pkg/manifest/manifest.go`](pkg/manifest/manifest.go): Deterministic state persistence and `.chk.json` manifest management.
- [`pkg/adaptive/controller.go`](pkg/adaptive/controller.go): Sliding window throughput monitor & adaptive worker controller.
- [`pkg/metrics/exporter.go`](pkg/metrics/exporter.go): Prometheus telemetry exporter (`:9090/metrics`).

## 🛡 Failure & Recovery Matrix

| Scenario | System Behavior |
|---|---|
| **Network Loss / Dropouts** | Retries chunk requests with exponential backoff and randomized jitter. |
| **Process Interruption (`SIGINT`/`SIGTERM`)** | Persists the checkpoint through an atomic manifest update; resumes cleanly via `downloader resume`. |
| **Server 5xx Errors** | Retries until configured max attempt threshold before marking chunk failed. |
| **No Range Support (`HTTP 200`)** | Falls back to a single-stream download when the server does not support byte-range requests. |
| **Final Checksum Mismatch** | Download is rejected as invalid, temporary file is cleaned up, error is reported. |

## 📊 Performance & Benchmarks

> ℹ️ **Benchmark environment**: Go 1.22+, local isolated HTTP test server, 50 MB payload, loopback networking. Results are from single benchmark runs intended for comparative concurrency scaling analysis rather than absolute performance claims. These results reflect the local test environment and should not be interpreted as expected WAN throughput.

| Workers | Chunk Size | Duration | Throughput | Total Alloc | Heap Sys |
|---|---|---|---|---|---|
| **1** | 8 MB | 120ms | **416 MB/s** | 99.8 MB | 59.4 MB |
| **2** | 8 MB | 85ms | **588 MB/s** | 99.8 MB | 91.5 MB |
| **4** | 8 MB | 75ms | **666 MB/s** | 99.8 MB | 104.0 MB |
| **8** | 8 MB | 82ms | **609 MB/s** | 99.8 MB | 104.2 MB |
| **16** | 8 MB | 67ms | **748 MB/s** | 99.8 MB | 104.2 MB |
| **32** | 8 MB | 60ms | **826 MB/s** | 99.8 MB | 104.5 MB |

*The non-monotonic results at intermediate worker counts illustrate why fixed concurrency is not always optimal and motivate the adaptive controller.*

### Concurrency Scaling Analysis

Parallel worker scaling exhibits diminishing returns beyond an optimal threshold due to standard systems bottlenecks:
1. **Network Bandwidth & TCP Contention**: Shared link saturation where multi-worker streams compete for socket buffers.
2. **Server-Side Throttling**: HTTP servers and CDNs enforce per-IP rate limits or connection pools.
3. **Disk I/O Contention**: Concurrent writes to non-contiguous file offsets may introduce additional storage contention.
4. **Go Scheduler Overhead**: Higher worker counts introduce context-switching costs between goroutines and OS threads.

## 🧠 Adaptive Concurrency

The adaptive controller samples transfer throughput every 500ms and uses a rolling window to adjust the worker pool when additional concurrency improves or degrades performance. The controller increases concurrency when recent throughput improves and backs off when additional workers stop producing meaningful gains or negatively affect transfer performance.

## 📋 Requirements

- **Go**: 1.22+
- **Operating System**: Linux, macOS, or Windows
- **Docker** *(Optional)*: Required only for isolated integration tests and benchmark containers.

## 🚀 Quick Start & CLI Usage

All CLI examples below use the real **Ubuntu Base 24.04.4 archive** (~30 MB, SHA-256 verified) as a single consistent sample file:

### Build from Source
```bash
git clone https://github.com/semplicementeio/Chunk-Downloader.git
cd Chunk-Downloader
go build -o downloader ./cmd/downloader
```

### 1. Standard Download with Checksum Verification
```bash
./downloader download https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.4-base-amd64.tar.gz -w 8 -c 4 --sha256 c1e67ef7b17a6300e136118bd1dc04725009cb376c1aad10abcf8cd453628d58

# Live Terminal Output:
# [██████████████████░░░░░░░░░░]  68.5% | 19.50/28.60 MB | Workers: 8
```

### 2. Download with Adaptive Concurrency Control
```bash
./downloader download https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.4-base-amd64.tar.gz -a
```

### 3. Resume an Interrupted Download
```bash
./downloader resume downloads/ubuntu-base-24.04.4-base-amd64.tar.gz.chk.json
```

### 4. Enable Prometheus Telemetry Exporter
Expose metrics at `http://localhost:9090/metrics` while downloading:
```bash
./downloader download https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.4-base-amd64.tar.gz -m 9090
```

### 5. Run Automated Performance Benchmarks
```bash
./downloader bench --size 50
```

## 🐋 Docker & Local Testing

Docker Compose is provided for reproducible local integration testing and benchmarks. It launches an isolated HTTP test server alongside the downloader benchmarking suite:
```bash
docker-compose up --build
```

## 🧪 Testing

Run unit tests, race detector, and integration suites:
```bash
go test -v -race -cover ./...
```

## 📐 Design Decisions

- **Why HTTP Range Requests?**: HTTP Range Requests allow independent byte ranges to be downloaded concurrently, while completed ranges can be skipped when resuming an interrupted transfer.
- **Why `os.File.WriteAt`?**: Chunks can complete out of order. Writing directly to each chunk's final byte offset eliminates locks around a shared file write cursor and avoids a separate file-merging step.
- **Why Checkpoint Manifests?**: Persisting chunk state (`.chk.json`) makes recovery deterministic and avoids re-downloading completed byte ranges after network or process failures.
- **Why Adaptive Concurrency?**: Increasing concurrency does not always increase throughput. The controller monitors rolling transfer performance to avoid unnecessary concurrency when the network or server becomes the bottleneck.
- **Why Context Cancellation?**: Download workers share a cancellation context so active HTTP requests, retry loops, and background goroutines terminate cleanly when the transfer is interrupted or fails.

## ⚠️ Limitations

- **HTTP Range Support**: Performance acceleration depends heavily on server support for HTTP Range requests (`206 Partial Content`).
- **Server Throttling**: Multiple concurrent connections may be restricted by server-side rate limiting or CDN policy.
- **Adaptive Tuning**: Adaptive concurrency optimizes for throughput trends and does not guarantee speedup under all network conditions.
- **Filesystem Sparse Files**: Pre-allocation behavior depends on the underlying OS filesystem.
- **Current Scope**: Currently designed for single-file HTTP/HTTPS transfers.

## 📜 License

Distributed under the [MIT License](LICENSE).
