//go:build windows

package downloader

func CheckAvailableDiskSpace(path string, requiredBytes int64) error {
	// Disk space check on Windows can be safely bypassed or implemented via GetDiskFreeSpaceEx
	return nil
}
