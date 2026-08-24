package downloader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/semplicementeio/chunk-downloader/pkg/manifest"
	"github.com/semplicementeio/chunk-downloader/pkg/testserver"
)

func TestDownloaderParallelSuccess(t *testing.T) {
	mockServer := testserver.NewMockServer(testserver.MockServerConfig{
		ContentLength: 2 * 1024 * 1024, // 2MB
		AllowRanges:   true,
	})
	defer mockServer.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "downloaded.bin")

	engine, err := NewEngine(Config{
		URL:            mockServer.Server.URL,
		OutputPath:     outputPath,
		Workers:        4,
		ChunkSize:      256 * 1024, // 256KB chunks
		ExpectedSHA256: mockServer.SHA256,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	if err := engine.Download(ctx); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	downloadedData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if len(downloadedData) != len(mockServer.Data) {
		t.Fatalf("file length mismatch: expected %d, got %d", len(mockServer.Data), len(downloadedData))
	}

	hash, err := calculateSHA256(outputPath)
	if err != nil {
		t.Fatalf("failed to calculate sha256: %v", err)
	}

	if hash != mockServer.SHA256 {
		t.Errorf("sha256 mismatch: expected %s, got %s", mockServer.SHA256, hash)
	}
}

func TestDownloaderSingleStreamFallback(t *testing.T) {
	mockServer := testserver.NewMockServer(testserver.MockServerConfig{
		ContentLength: 500 * 1024,
		AllowRanges:   false, // Server does not allow ranges
	})
	defer mockServer.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "single_stream.bin")

	engine, err := NewEngine(Config{
		URL:        mockServer.Server.URL,
		OutputPath: outputPath,
		Workers:    4,
		ChunkSize:  100 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	if err := engine.Download(context.Background()); err != nil {
		t.Fatalf("single stream download failed: %v", err)
	}

	downloadedData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	if len(downloadedData) != len(mockServer.Data) {
		t.Fatalf("data length mismatch: expected %d, got %d", len(mockServer.Data), len(downloadedData))
	}
}

func TestDownloaderResumeInterrupted(t *testing.T) {
	mockServer := testserver.NewMockServer(testserver.MockServerConfig{
		ContentLength: 1 * 1024 * 1024,
		AllowRanges:   true,
	})
	defer mockServer.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "resumable.bin")
	manifestPath := outputPath + ".chk.json"

	// Create pre-existing manifest with chunk 0 completed
	m, err := manifest.NewManifest(mockServer.Server.URL, outputPath, manifestPath, mockServer.Config.ContentLength, 256*1024, mockServer.SHA256)
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	tempFile, err := os.Create(m.TempFilePath)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_ = tempFile.Truncate(mockServer.Config.ContentLength)

	// Write chunk 0 data into temp file manually
	chunk0Data := mockServer.Data[0:256*1024]
	_, err = tempFile.WriteAt(chunk0Data, 0)
	tempFile.Close()
	if err != nil {
		t.Fatalf("failed to pre-populate chunk 0 data: %v", err)
	}

	m.UpdateChunkStatus(0, manifest.StatusCompleted, 256*1024)
	if err := m.Save(); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	engine, err := NewEngine(Config{
		URL:            mockServer.Server.URL,
		OutputPath:     outputPath,
		ManifestPath:   manifestPath,
		Workers:        2,
		ChunkSize:      256 * 1024,
		ExpectedSHA256: mockServer.SHA256,
	})
	if err != nil {
		t.Fatalf("failed creating engine: %v", err)
	}

	if err := engine.Download(context.Background()); err != nil {
		t.Fatalf("resumed download failed: %v", err)
	}

	hash, err := calculateSHA256(outputPath)
	if err != nil {
		t.Fatalf("failed calculating hash: %v", err)
	}
	if hash != mockServer.SHA256 {
		t.Errorf("resumed file hash mismatch: expected %s, got %s", mockServer.SHA256, hash)
	}
}

func TestDownloaderRetryMechanism(t *testing.T) {
	mockServer := testserver.NewMockServer(testserver.MockServerConfig{
		ContentLength:   500 * 1024,
		AllowRanges:     true,
		FailProbability: 0.3, // 30% chance of request failure
	})
	defer mockServer.Close()

	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "flaky.bin")

	retryCfg := RetryConfig{
		MaxAttempts:     10,
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		Multiplier:      1.5,
		Jitter:          false,
	}

	engine, err := NewEngine(Config{
		URL:            mockServer.Server.URL,
		OutputPath:     outputPath,
		Workers:        2,
		ChunkSize:      100 * 1024,
		RetryConfig:    retryCfg,
		ExpectedSHA256: mockServer.SHA256,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	if err := engine.Download(context.Background()); err != nil {
		t.Fatalf("download with retry failed: %v", err)
	}
}
