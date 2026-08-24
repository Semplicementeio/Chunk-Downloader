package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/semplicementeio/chunk-downloader/pkg/adaptive"
	"github.com/semplicementeio/chunk-downloader/pkg/manifest"
	"github.com/semplicementeio/chunk-downloader/pkg/metrics"
)

type Config struct {
	URL            string
	OutputPath     string
	Workers        int
	ChunkSize      int64
	Adaptive       bool
	ExpectedSHA256 string
	ManifestPath   string
	RetryConfig    RetryConfig
	HTTPClient     *http.Client
	Logger         *slog.Logger
}

type Engine struct {
	cfg        Config
	manifest   *manifest.Manifest
	httpClient *http.Client
	logger     *slog.Logger
}

type Info struct {
	URL           string
	ContentLength int64
	AcceptRanges  bool
	ContentType   string
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.URL == "" {
		return nil, errors.New("url cannot be empty")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 8 * 1024 * 1024 // 8 MB default
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	if cfg.OutputPath == "" {
		filename := filepath.Base(cfg.URL)
		if filename == "" || filename == "." || filename == "/" {
			filename = "downloaded.bin"
		}
		cfg.OutputPath = filepath.Join("downloads", filename)
	}

	return &Engine{
		cfg:        cfg,
		httpClient: cfg.HTTPClient,
		logger:     cfg.Logger,
	}, nil
}

func (e *Engine) Probe(ctx context.Context) (*Info, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, e.cfg.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HEAD request: %w", err)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HEAD request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		reqGET, errGET := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.URL, nil)
		if errGET == nil {
			reqGET.Header.Set("Range", "bytes=0-0")
			respGET, errDo := e.httpClient.Do(reqGET)
			if errDo == nil {
				defer respGET.Body.Close()
				if respGET.StatusCode == http.StatusPartialContent {
					contentRange := respGET.Header.Get("Content-Range")
					totalSize := parseTotalSizeFromContentRange(contentRange)
					return &Info{
						URL:           e.cfg.URL,
						ContentLength: totalSize,
						AcceptRanges:  true,
						ContentType:   respGET.Header.Get("Content-Type"),
					}, nil
				}
			}
		}
		return nil, fmt.Errorf("server returned unexpected status code: %d", resp.StatusCode)
	}

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	acceptRanges := strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")

	if !acceptRanges {
		reqRange, errRange := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.URL, nil)
		if errRange == nil {
			reqRange.Header.Set("Range", "bytes=0-0")
			respRange, errDo := e.httpClient.Do(reqRange)
			if errDo == nil {
				defer respRange.Body.Close()
				if respRange.StatusCode == http.StatusPartialContent {
					acceptRanges = true
				}
			}
		}
	}

	return &Info{
		URL:           e.cfg.URL,
		ContentLength: contentLength,
		AcceptRanges:  acceptRanges,
		ContentType:   resp.Header.Get("Content-Type"),
	}, nil
}

func parseTotalSizeFromContentRange(contentRange string) int64 {
	parts := strings.Split(contentRange, "/")
	if len(parts) == 2 {
		size, err := strconv.ParseInt(parts[1], 10, 64)
		if err == nil {
			return size
		}
	}
	return 0
}



func (e *Engine) Download(ctx context.Context) error {
	info, err := e.Probe(ctx)
	if err != nil {
		return fmt.Errorf("probe failed: %w", err)
	}

	e.logger.Info("File info probed",
		"url", info.URL,
		"size_bytes", info.ContentLength,
		"accept_ranges", info.AcceptRanges,
		"content_type", info.ContentType,
	)

	manifestPath := e.cfg.ManifestPath
	if manifestPath == "" {
		manifestPath = e.cfg.OutputPath + ".chk.json"
	}

	if _, err := os.Stat(manifestPath); err == nil {
		e.logger.Info("Found existing manifest, attempting resume...", "manifest", manifestPath)
		m, errLoad := manifest.LoadManifest(manifestPath)
		if errLoad == nil && m.URL == e.cfg.URL && m.TotalSize == info.ContentLength {
			e.manifest = m
		} else {
			e.logger.Warn("Existing manifest incompatible or invalid, starting fresh manifest")
		}
	}

	if e.manifest == nil {
		if info.ContentLength > 0 {
			if err := CheckAvailableDiskSpace(e.cfg.OutputPath, info.ContentLength); err != nil {
				return err
			}
		}

		m, errNew := manifest.NewManifest(e.cfg.URL, e.cfg.OutputPath, manifestPath, info.ContentLength, e.cfg.ChunkSize, e.cfg.ExpectedSHA256)
		if errNew != nil {
			return fmt.Errorf("failed to create manifest: %w", errNew)
		}
		e.manifest = m
		if err := e.manifest.Save(); err != nil {
			return fmt.Errorf("failed to save initial manifest: %w", err)
		}
	}

	dir := filepath.Dir(e.manifest.TempFilePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	tempFile, err := os.OpenFile(e.manifest.TempFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open/create temp file %s: %w", e.manifest.TempFilePath, err)
	}
	defer tempFile.Close()

	if info.ContentLength > 0 {
		if err := tempFile.Truncate(info.ContentLength); err != nil {
			return fmt.Errorf("failed to pre-allocate temp file size: %w", err)
		}
	}

	if !info.AcceptRanges || info.ContentLength <= 0 {
		e.logger.Warn("Server does not support ranged requests. Falling back to single-stream download.")
		return e.downloadSingleStream(ctx, tempFile)
	}

	return e.downloadParallel(ctx, tempFile)
}

func (e *Engine) downloadSingleStream(ctx context.Context, file *os.File) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.URL, nil)
	if err != nil {
		return err
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("single stream download failed with status %d", resp.StatusCode)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write single stream download: %w", err)
	}

	return e.finalizeDownload()
}

func (e *Engine) downloadParallel(ctx context.Context, file *os.File) error {
	pendingChunks := e.manifest.PendingChunks()
	if len(pendingChunks) == 0 {
		e.logger.Info("All chunks already downloaded!")
		return e.finalizeDownload()
	}

	chunkChan := make(chan *manifest.Chunk, len(pendingChunks))
	for _, c := range pendingChunks {
		chunkChan <- c
	}
	close(chunkChan)

	workerCount := e.cfg.Workers
	var adaptCtrl *adaptive.AdaptiveController
	if e.cfg.Adaptive {
		adaptCtrl = adaptive.NewAdaptiveController(1, e.cfg.Workers*2, e.cfg.Workers, 500*time.Millisecond)
		e.logger.Info("Adaptive concurrency controller enabled", "initial_workers", workerCount)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(pendingChunks))

	metrics.GlobalMetrics.SetActiveWorkers(workerCount)

	startTime := time.Now()
	totalBytes := e.manifest.TotalSize

	// Progress ticker
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				completed := e.manifest.CompletedBytes()
				if totalBytes <= 0 {
					continue
				}

				pct := float64(completed) / float64(totalBytes) * 100.0
				if pct > 100.0 {
					pct = 100.0
				}

				currentWorkers := workerCount
				if adaptCtrl != nil {
					currentWorkers = adaptCtrl.CurrentWorkers()
				}

				renderProgressBar(completed, totalBytes, pct, currentWorkers)
			}
		}
	}()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for chunk := range chunkChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				e.manifest.UpdateChunkStatus(chunk.ID, manifest.StatusInFlight, 0)
				err := e.downloadChunkWithRetry(ctx, file, chunk)
				if err != nil {
					e.manifest.UpdateChunkStatus(chunk.ID, manifest.StatusFailed, 0)
					metrics.GlobalMetrics.IncFailedChunks()
					e.logger.Error("Chunk failed after retries", "chunk_id", chunk.ID, "error", err)
					errChan <- fmt.Errorf("chunk %d failed: %w", chunk.ID, err)
					return
				}

				e.manifest.UpdateChunkStatus(chunk.ID, manifest.StatusCompleted, chunk.End-chunk.Start+1)
				metrics.GlobalMetrics.IncCompletedChunks()
				metrics.GlobalMetrics.AddDownloadedBytes(chunk.End - chunk.Start + 1)

				if errSave := e.manifest.Save(); errSave != nil {
					e.logger.Warn("Failed to save manifest snapshot", "error", errSave)
				}

				if adaptCtrl != nil {
					newWorkers, changed := adaptCtrl.RecordProgress(e.manifest.CompletedBytes())
					if changed {
						e.logger.Info("Adaptive concurrency tuned worker count", "new_workers", newWorkers)
						metrics.GlobalMetrics.SetActiveWorkers(newWorkers)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	cancelProgress()

	// Final progress line clear
	if totalBytes > 0 {
		renderProgressBar(totalBytes, totalBytes, 100.0, workerCount)
		fmt.Fprint(os.Stderr, "\n")
	}

	if len(errChan) > 0 {
		firstErr := <-errChan
		_ = e.manifest.Save()
		return fmt.Errorf("download encountered errors: %w", firstErr)
	}

	elapsed := time.Since(startTime)
	speedMBs := (float64(totalBytes) / (1024 * 1024)) / elapsed.Seconds()

	e.logger.Info("Download completed successfully",
		"elapsed", elapsed.String(),
		"speed_mbs", fmt.Sprintf("%.2f MB/s", speedMBs),
	)

	return e.finalizeDownload()
}

func renderProgressBar(completed, total int64, pct float64, workers int) {
	barWidth := 25
	filled := int((pct / 100.0) * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	if filled < 0 {
		filled = 0
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	completedMB := float64(completed) / (1024 * 1024)
	totalMB := float64(total) / (1024 * 1024)

	fmt.Fprintf(os.Stderr, "\r[%s] %5.1f%% | %.2f/%.2f MB | Workers: %d   ",
		bar, pct, completedMB, totalMB, workers)
}

func (e *Engine) downloadChunkWithRetry(ctx context.Context, file *os.File, chunk *manifest.Chunk) error {
	retryCfg := e.cfg.RetryConfig
	if retryCfg.MaxAttempts <= 0 {
		retryCfg = DefaultRetryConfig()
	}

	return DoWithRetry(ctx, retryCfg, func(attempt int) error {
		if attempt > 1 {
			metrics.GlobalMetrics.IncRetries()
			e.logger.Warn("Retrying chunk download", "chunk_id", chunk.ID, "attempt", attempt)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.URL, nil)
		if err != nil {
			return err
		}

		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End))

		resp, err := e.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("server responded with status %d", resp.StatusCode)
		}

		expectedLen := int(chunk.End - chunk.Start + 1)
		data := make([]byte, 0, expectedLen)
		buf := make([]byte, 32*1024)
		var downloaded int64 = 0

		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				data = append(data, buf[:n]...)
				downloaded += int64(n)
				e.manifest.UpdateChunkStatus(chunk.ID, manifest.StatusInFlight, downloaded)
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return fmt.Errorf("failed reading chunk stream: %w", readErr)
			}
		}

		if len(data) != expectedLen {
			return fmt.Errorf("chunk size mismatch: expected %d bytes, got %d", expectedLen, len(data))
		}

		_, err = file.WriteAt(data, chunk.Start)
		if err != nil {
			return fmt.Errorf("failed writing chunk at offset %d: %w", chunk.Start, err)
		}

		return nil
	})
}

func (e *Engine) finalizeDownload() error {
	e.logger.Info("Verifying file integrity & finalizing download...")

	tempPath := e.manifest.TempFilePath
	targetPath := e.manifest.TargetFilePath

	if e.cfg.ExpectedSHA256 != "" || e.manifest.ChecksumSHA256 != "" {
		expectedHash := e.cfg.ExpectedSHA256
		if expectedHash == "" {
			expectedHash = e.manifest.ChecksumSHA256
		}

		hashHex, err := calculateSHA256(tempPath)
		if err != nil {
			return fmt.Errorf("failed calculating file SHA256: %w", err)
		}

		if !strings.EqualFold(hashHex, expectedHash) {
			return fmt.Errorf("checksum mismatch! Expected %s, got %s", expectedHash, hashHex)
		}

		e.logger.Info("SHA-256 checksum verified successfully!", "sha256", hashHex)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("failed to move temp file to target location: %w", err)
	}

	if err := e.manifest.Clean(); err != nil {
		e.logger.Warn("Failed to clean up manifest file", "error", err)
	}

	e.logger.Info("File download finalized!", "output", targetPath)
	return nil
}

func calculateSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
