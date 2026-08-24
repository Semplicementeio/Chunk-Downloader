package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewManifest(t *testing.T) {
	m, err := NewManifest("http://example.com/file.bin", "file.bin", "", 1000, 300, "")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %v", err)
	}

	if len(m.Chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(m.Chunks))
	}

	if m.Chunks[0].Start != 0 || m.Chunks[0].End != 299 {
		t.Errorf("chunk 0 bounds mismatch: got %d-%d", m.Chunks[0].Start, m.Chunks[0].End)
	}

	if m.Chunks[3].Start != 900 || m.Chunks[3].End != 999 {
		t.Errorf("chunk 3 bounds mismatch: got %d-%d", m.Chunks[3].Start, m.Chunks[3].End)
	}
}

func TestManifestSaveLoad(t *testing.T) {
	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "test.bin")
	manifestPath := filepath.Join(tempDir, "test.bin.chk.json")

	m, err := NewManifest("http://example.com/test", targetPath, manifestPath, 500, 200, "expected-sha256")
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	m.UpdateChunkStatus(0, StatusCompleted, 200)
	m.UpdateChunkStatus(1, StatusInFlight, 100)

	if err := m.Save(); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	if loaded.URL != m.URL || loaded.TotalSize != m.TotalSize {
		t.Errorf("loaded manifest header mismatch: %+v vs %+v", loaded, m)
	}

	if loaded.Chunks[0].Status != StatusCompleted {
		t.Errorf("expected chunk 0 completed, got %s", loaded.Chunks[0].Status)
	}

	// StatusInFlight should be reset to StatusPending on load
	if loaded.Chunks[1].Status != StatusPending {
		t.Errorf("expected chunk 1 to reset to pending, got %s", loaded.Chunks[1].Status)
	}

	if loaded.IsComplete() {
		t.Errorf("expected incomplete manifest, got complete")
	}

	loaded.UpdateChunkStatus(1, StatusCompleted, 200)
	loaded.UpdateChunkStatus(2, StatusCompleted, 100)

	if !loaded.IsComplete() {
		t.Errorf("expected complete manifest")
	}

	if err := loaded.Clean(); err != nil {
		t.Errorf("failed to clean manifest: %v", err)
	}

	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Errorf("manifest file still exists after clean")
	}
}
