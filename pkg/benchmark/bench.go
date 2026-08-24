package benchmark

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/semplicementeio/chunk-downloader/pkg/downloader"
	"github.com/semplicementeio/chunk-downloader/pkg/testserver"
)

type Result struct {
	Workers       int
	ChunkSizeMB   int
	ThroughputMBs float64
	Duration      time.Duration
	AllocMB       float64
	SysMB         float64
}

type Suite struct {
	FileSizeMB int
	TargetURL  string
}

func NewSuite(fileSizeMB int, targetURL string) *Suite {
	if fileSizeMB <= 0 {
		fileSizeMB = 50 // 50MB test file by default
	}
	return &Suite{
		FileSizeMB: fileSizeMB,
		TargetURL:  targetURL,
	}
}

func (s *Suite) Run(workerCounts []int, chunkSizeMB int) ([]Result, string, error) {
	mockServerCreated := false
	url := s.TargetURL

	if url == "" {
		mockServer := testserver.NewMockServer(testserver.MockServerConfig{
			ContentLength: int64(s.FileSizeMB) * 1024 * 1024,
			AllowRanges:   true,
		})
		defer mockServer.Close()
		url = mockServer.Server.URL
		mockServerCreated = true
	}

	tempDir, err := os.MkdirTemp("", "bench_downloader_*")
	if err != nil {
		return nil, "", fmt.Errorf("failed creating bench temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	var results []Result
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	for _, w := range workerCounts {
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		outputPath := filepath.Join(tempDir, fmt.Sprintf("bench_w%d.bin", w))

		cfg := downloader.Config{
			URL:          url,
			OutputPath:   outputPath,
			Workers:      w,
			ChunkSize:    int64(chunkSizeMB) * 1024 * 1024,
			Logger:       logger,
			ManifestPath: outputPath + ".chk.json",
		}

		eng, err := downloader.NewEngine(cfg)
		if err != nil {
			return nil, "", err
		}

		start := time.Now()
		err = eng.Download(context.Background())
		duration := time.Since(start)

		if err != nil {
			return nil, "", fmt.Errorf("bench run failed for %d workers: %w", w, err)
		}

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		totalBytes := int64(s.FileSizeMB) * 1024 * 1024
		throughput := (float64(totalBytes) / (1024 * 1024)) / duration.Seconds()

		allocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)
		sysMB := float64(memAfter.Sys) / (1024 * 1024)

		results = append(results, Result{
			Workers:       w,
			ChunkSizeMB:   chunkSizeMB,
			ThroughputMBs: throughput,
			Duration:      duration,
			AllocMB:       allocMB,
			SysMB:         sysMB,
		})
	}

	tableMarkdown := FormatResultsTable(results, s.FileSizeMB, mockServerCreated)
	return results, tableMarkdown, nil
}

func FormatResultsTable(results []Result, fileSizeMB int, isLocalMock bool) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("### 📊 Benchmark Results (%d MB File)\n\n", fileSizeMB))
	if isLocalMock {
		sb.WriteString("*Tested against local mock HTTP server (loopback interface)*\n\n")
	}

	sb.WriteString("| Workers | Chunk Size | Duration | Throughput | Memory Alloc | Heap Sys |\n")
	sb.WriteString("|---|---|---|---|---|---|\n")

	for _, r := range results {
		sb.WriteString(fmt.Sprintf("| %d | %d MB | %v | **%.2f MB/s** | %.2f MB | %.2f MB |\n",
			r.Workers, r.ChunkSizeMB, r.Duration.Round(time.Millisecond), r.ThroughputMBs, r.AllocMB, r.SysMB))
	}

	return sb.String()
}
