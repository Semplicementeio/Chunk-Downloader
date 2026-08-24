package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type ChunkStatus string

const (
	StatusPending   ChunkStatus = "pending"
	StatusInFlight  ChunkStatus = "in_flight"
	StatusCompleted ChunkStatus = "completed"
	StatusFailed    ChunkStatus = "failed"
)

type Chunk struct {
	ID              int         `json:"id"`
	Start           int64       `json:"start"`
	End             int64       `json:"end"`
	DownloadedBytes int64       `json:"downloaded_bytes"`
	Status          ChunkStatus `json:"status"`
}

type Manifest struct {
	URL            string `json:"url"`
	TargetFilePath string `json:"target_file_path"`
	TempFilePath   string `json:"temp_file_path"`
	ManifestPath   string `json:"manifest_path"`
	TotalSize      int64  `json:"total_size"`
	ChunkSize      int64  `json:"chunk_size"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	Chunks         []*Chunk `json:"chunks"`

	mu sync.RWMutex `json:"-"`
}

func NewManifest(url, targetPath, manifestPath string, totalSize, chunkSize int64, checksum string) (*Manifest, error) {
	if totalSize <= 0 || chunkSize <= 0 {
		return nil, fmt.Errorf("invalid total size (%d) or chunk size (%d)", totalSize, chunkSize)
	}

	tempPath := targetPath + ".tmp"
	if manifestPath == "" {
		manifestPath = targetPath + ".chk.json"
	}

	var chunks []*Chunk
	var start int64 = 0
	id := 0

	for start < totalSize {
		end := start + chunkSize - 1
		if end >= totalSize {
			end = totalSize - 1
		}
		chunks = append(chunks, &Chunk{
			ID:              id,
			Start:           start,
			End:             end,
			DownloadedBytes: 0,
			Status:          StatusPending,
		})
		start = end + 1
		id++
	}

	m := &Manifest{
		URL:            url,
		TargetFilePath: targetPath,
		TempFilePath:   tempPath,
		ManifestPath:   manifestPath,
		TotalSize:      totalSize,
		ChunkSize:      chunkSize,
		ChecksumSHA256: checksum,
		Chunks:         chunks,
	}

	return m, nil
}

func LoadManifest(manifestPath string) (*Manifest, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file %s: %w", manifestPath, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest JSON: %w", err)
	}

	m.ManifestPath = manifestPath
	for _, c := range m.Chunks {
		if c.Status == StatusInFlight {
			c.Status = StatusPending
		}
	}

	return &m, nil
}

func (m *Manifest) Save() error {
	m.mu.RLock()
	data, err := json.MarshalIndent(m, "", "  ")
	m.mu.RUnlock()

	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	dir := filepath.Dir(m.ManifestPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create manifest directory: %w", err)
		}
	}

	tmpFile := m.ManifestPath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp manifest file: %w", err)
	}

	if err := os.Rename(tmpFile, m.ManifestPath); err != nil {
		return fmt.Errorf("failed to replace manifest file: %w", err)
	}

	return nil
}

func (m *Manifest) UpdateChunkStatus(id int, status ChunkStatus, downloadedBytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id >= 0 && id < len(m.Chunks) {
		m.Chunks[id].Status = status
		if downloadedBytes >= 0 {
			m.Chunks[id].DownloadedBytes = downloadedBytes
		}
	}
}

func (m *Manifest) PendingChunks() []*Chunk {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pending []*Chunk
	for _, c := range m.Chunks {
		if c.Status != StatusCompleted {
			pending = append(pending, c)
		}
	}
	return pending
}

func (m *Manifest) CompletedBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int64
	for _, c := range m.Chunks {
		if c.Status == StatusCompleted {
			total += (c.End - c.Start + 1)
		} else {
			total += c.DownloadedBytes
		}
	}
	return total
}

func (m *Manifest) IsComplete() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, c := range m.Chunks {
		if c.Status != StatusCompleted {
			return false
		}
	}
	return true
}

func (m *Manifest) Clean() error {
	return os.Remove(m.ManifestPath)
}
