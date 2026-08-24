//go:build !windows

package downloader

import (
	"fmt"
	"path/filepath"
	"syscall"
)

func CheckAvailableDiskSpace(path string, requiredBytes int64) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return nil // Ignore error on systems or filesystems where statfs fails
	}
	freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
	if freeBytes < requiredBytes {
		return fmt.Errorf("insufficient disk space: required %d bytes, available %d bytes", requiredBytes, freeBytes)
	}
	return nil
}
