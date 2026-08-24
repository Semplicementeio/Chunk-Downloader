package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/semplicementeio/chunk-downloader/pkg/benchmark"
	"github.com/semplicementeio/chunk-downloader/pkg/downloader"
	"github.com/semplicementeio/chunk-downloader/pkg/manifest"
	"github.com/semplicementeio/chunk-downloader/pkg/metrics"
	"github.com/semplicementeio/chunk-downloader/pkg/testserver"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "download":
		runDownload(os.Args[2:])
	case "resume":
		runResume(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "mock-server":
		runMockServer(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		// If first arg is a URL, assume download command
		if isURL(command) {
			runDownload(os.Args[1:])
		} else {
			fmt.Printf("Unknown command: %s\n\n", command)
			printUsage()
			os.Exit(1)
		}
	}
}

func printUsage() {
	fmt.Print(`⚡ Chunk Downloader - High Performance Concurrent & Resumable Downloader

Usage:
  downloader <command> [arguments]

Commands:
  download <url> [options]    Download a file concurrently with parallel chunks
  resume <file|manifest>       Resume an interrupted download using its manifest
  bench [options]             Run performance benchmarks across worker concurrency matrices
  mock-server [options]        Launch a local HTTP test server supporting range requests
  help                         Show this help message

Download Options:
  -o, --output <file>         Target output file path (default: downloads/<filename>)
  -w, --workers <count>       Number of concurrent worker goroutines (default: 4)
  -c, --chunk-size <mb>       Chunk size in MB (default: 8)
  -a, --adaptive              Enable adaptive concurrency control
  -s, --sha256 <hash>         Expected SHA-256 checksum for verification
  -m, --metrics-port <port>   Prometheus metrics HTTP port (default: 0 disabled)

Examples:
  downloader download https://example.com/file.zip -w 8 -c 16
  downloader resume downloads/file.zip.chk.json
  downloader bench --size 50
`)
}

func runDownload(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	output := fs.String("o", "", "Target output path")
	fs.StringVar(output, "output", "", "Target output path")

	workers := fs.Int("w", 4, "Number of concurrent workers")
	fs.IntVar(workers, "workers", 4, "Number of concurrent workers")

	chunkSizeMB := fs.Int("c", 8, "Chunk size in MB")
	fs.IntVar(chunkSizeMB, "chunk-size", 8, "Chunk size in MB")

	adaptive := fs.Bool("a", false, "Enable adaptive concurrency")
	fs.BoolVar(adaptive, "adaptive", false, "Enable adaptive concurrency")

	sha256Hash := fs.String("s", "", "Expected SHA-256 checksum")
	fs.StringVar(sha256Hash, "sha256", "", "Expected SHA-256 checksum")

	metricsPort := fs.Int("m", 0, "Prometheus metrics port")
	fs.IntVar(metricsPort, "metrics-port", 0, "Prometheus metrics port")

	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Println("Error: URL argument required")
		os.Exit(1)
	}

	urlStr := fs.Arg(0)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if *metricsPort > 0 {
		_, err := metrics.StartMetricsServer(*metricsPort)
		if err != nil {
			logger.Warn("Failed starting metrics server", "error", err)
		} else {
			logger.Info("Prometheus metrics server active", "endpoint", fmt.Sprintf("http://localhost:%d/metrics", *metricsPort))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Warn("Received shutdown signal, stopping workers gracefully and saving checkpoint...")
		cancel()
	}()

	cfg := downloader.Config{
		URL:            urlStr,
		OutputPath:     *output,
		Workers:        *workers,
		ChunkSize:      int64(*chunkSizeMB) * 1024 * 1024,
		Adaptive:       *adaptive,
		ExpectedSHA256: *sha256Hash,
		Logger:         logger,
	}

	eng, err := downloader.NewEngine(cfg)
	if err != nil {
		logger.Error("Failed creating downloader engine", "error", err)
		os.Exit(1)
	}

	if err := eng.Download(ctx); err != nil {
		if ctx.Err() != nil {
			logger.Warn("Download interrupted by user. Run 'downloader resume' to continue.")
		} else {
			logger.Error("Download failed", "error", err)
			os.Exit(1)
		}
	}
}

func runResume(args []string) {
	if len(args) < 1 {
		fmt.Println("Error: Manifest or target file argument required for resume")
		os.Exit(1)
	}

	targetArg := args[0]
	manifestPath := targetArg
	if filepath.Ext(targetArg) != ".json" {
		manifestPath = targetArg + ".chk.json"
	}

	m, err := manifest.LoadManifest(manifestPath)
	if err != nil {
		fmt.Printf("Error: Failed loading manifest file %s: %v\n", manifestPath, err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Warn("Interrupted, saving manifest checkpoint...")
		cancel()
	}()

	cfg := downloader.Config{
		URL:            m.URL,
		OutputPath:     m.TargetFilePath,
		ManifestPath:   m.ManifestPath,
		Workers:        4,
		ChunkSize:      m.ChunkSize,
		ExpectedSHA256: m.ChecksumSHA256,
		Logger:         logger,
	}

	eng, err := downloader.NewEngine(cfg)
	if err != nil {
		logger.Error("Failed initializing engine for resume", "error", err)
		os.Exit(1)
	}

	if err := eng.Download(ctx); err != nil {
		if ctx.Err() != nil {
			logger.Warn("Resume download interrupted. Run resume again to continue.")
		} else {
			logger.Error("Resume download failed", "error", err)
			os.Exit(1)
		}
	}
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	fileSizeMB := fs.Int("size", 50, "File size in MB for benchmark")
	targetURL := fs.String("url", "", "Custom HTTP URL to benchmark (optional)")
	_ = fs.Parse(args)

	fmt.Printf("🚀 Starting Chunk Downloader Benchmark Suite (%d MB File)\n", *fileSizeMB)

	workerMatrix := []int{1, 2, 4, 8, 16, 32}
	suite := benchmark.NewSuite(*fileSizeMB, *targetURL)

	_, table, err := suite.Run(workerMatrix, 8)
	if err != nil {
		fmt.Printf("Benchmark error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n" + table)
}

func runMockServer(args []string) {
	fs := flag.NewFlagSet("mock-server", flag.ExitOnError)
	_ = fs.Int("port", 8080, "Port to listen on")
	fileSizeMB := fs.Int("size", 100, "Simulated file size in MB")
	_ = fs.Parse(args)

	cfg := testserver.MockServerConfig{
		ContentLength: int64(*fileSizeMB) * 1024 * 1024,
		AllowRanges:   true,
	}

	ms := testserver.NewMockServer(cfg)
	fmt.Printf("⚡ Mock HTTP Server running on %s (Serving %d MB file, SHA-256: %s)\n",
		ms.Server.URL, *fileSizeMB, ms.SHA256)
	fmt.Println("Press Ctrl+C to stop.")

	select {}
}

func isURL(str string) bool {
	return len(str) > 7 && (str[:7] == "http://" || str[:8] == "https://")
}
