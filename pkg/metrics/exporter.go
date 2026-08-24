package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	TotalBytesDownloaded int64
	ActiveWorkers        int64
	CompletedChunks      int64
	FailedChunks         int64
	Retries              int64
}

var GlobalMetrics Metrics

func (m *Metrics) AddDownloadedBytes(bytes int64) {
	atomic.AddInt64(&m.TotalBytesDownloaded, bytes)
}

func (m *Metrics) SetActiveWorkers(workers int) {
	atomic.StoreInt64(&m.ActiveWorkers, int64(workers))
}

func (m *Metrics) IncCompletedChunks() {
	atomic.AddInt64(&m.CompletedChunks, 1)
}

func (m *Metrics) IncFailedChunks() {
	atomic.AddInt64(&m.FailedChunks, 1)
}

func (m *Metrics) IncRetries() {
	atomic.AddInt64(&m.Retries, 1)
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP chunk_downloader_bytes_downloaded_total Total downloaded bytes\n")
		fmt.Fprintf(w, "# TYPE chunk_downloader_bytes_downloaded_total counter\n")
		fmt.Fprintf(w, "chunk_downloader_bytes_downloaded_total %d\n", atomic.LoadInt64(&m.TotalBytesDownloaded))

		fmt.Fprintf(w, "# HELP chunk_downloader_active_workers Current number of active workers\n")
		fmt.Fprintf(w, "# TYPE chunk_downloader_active_workers gauge\n")
		fmt.Fprintf(w, "chunk_downloader_active_workers %d\n", atomic.LoadInt64(&m.ActiveWorkers))

		fmt.Fprintf(w, "# HELP chunk_downloader_completed_chunks_total Total completed chunks\n")
		fmt.Fprintf(w, "# TYPE chunk_downloader_completed_chunks_total counter\n")
		fmt.Fprintf(w, "chunk_downloader_completed_chunks_total %d\n", atomic.LoadInt64(&m.CompletedChunks))

		fmt.Fprintf(w, "# HELP chunk_downloader_failed_chunks_total Total failed chunks\n")
		fmt.Fprintf(w, "# TYPE chunk_downloader_failed_chunks_total counter\n")
		fmt.Fprintf(w, "chunk_downloader_failed_chunks_total %d\n", atomic.LoadInt64(&m.FailedChunks))

		fmt.Fprintf(w, "# HELP chunk_downloader_retries_total Total retry attempts\n")
		fmt.Fprintf(w, "# TYPE chunk_downloader_retries_total counter\n")
		fmt.Fprintf(w, "chunk_downloader_retries_total %d\n", atomic.LoadInt64(&m.Retries))
	}
}

func StartMetricsServer(port int) (*http.Server, error) {
	if port <= 0 {
		return nil, nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", GlobalMetrics.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		_ = server.ListenAndServe()
	}()

	return server, nil
}
