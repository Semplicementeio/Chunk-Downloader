package testserver

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MockServerConfig struct {
	ContentLength   int64
	AllowRanges     bool
	FailProbability float64
	RateLimitKBps   int
	Latency         time.Duration
	Pattern         byte
}

type MockServer struct {
	Server *httptest.Server
	Config MockServerConfig
	Data   []byte
	SHA256 string

	mu        sync.Mutex
	CallCount int64
}

func NewMockServer(cfg MockServerConfig) *MockServer {
	if cfg.ContentLength <= 0 {
		cfg.ContentLength = 10 * 1024 * 1024 // 10MB default
	}

	data := make([]byte, cfg.ContentLength)
	pattern := cfg.Pattern
	if pattern == 0 {
		pattern = 'A'
	}
	for i := range data {
		data[i] = pattern + byte(i%26)
	}

	hash := sha256.Sum256(data)
	sha256Hex := hex.EncodeToString(hash[:])

	ms := &MockServer{
		Config: cfg,
		Data:   data,
		SHA256: sha256Hex,
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ms.mu.Lock()
		ms.CallCount++
		ms.mu.Unlock()

		if ms.Config.Latency > 0 {
			time.Sleep(ms.Config.Latency)
		}

		if ms.Config.FailProbability > 0 && rand.Float64() < ms.Config.FailProbability {
			http.Error(w, "simulated server failure", http.StatusInternalServerError)
			return
		}

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatInt(ms.Config.ContentLength, 10))
			if ms.Config.AllowRanges {
				w.Header().Set("Accept-Ranges", "bytes")
			} else {
				w.Header().Set("Accept-Ranges", "none")
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if !ms.Config.AllowRanges || rangeHeader == "" || !strings.HasPrefix(rangeHeader, "bytes=") {
			w.Header().Set("Content-Length", strconv.FormatInt(ms.Config.ContentLength, 10))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			ms.writeThrottled(w, ms.Data)
			return
		}

		// Handle range request: bytes=start-end
		rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeSpec, "-")
		if len(parts) != 2 {
			http.Error(w, "invalid range header", http.StatusBadRequest)
			return
		}

		start, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || start < 0 || start >= ms.Config.ContentLength {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", ms.Config.ContentLength))
			http.Error(w, "invalid range start", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		end := ms.Config.ContentLength - 1
		if parts[1] != "" {
			parsedEnd, err := strconv.ParseInt(parts[1], 10, 64)
			if err == nil && parsedEnd >= start {
				end = parsedEnd
			}
		}

		if end >= ms.Config.ContentLength {
			end = ms.Config.ContentLength - 1
		}

		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, ms.Config.ContentLength))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusPartialContent)

		ms.writeThrottled(w, ms.Data[start:end+1])
	})

	ms.Server = httptest.NewServer(handler)
	return ms
}

func (ms *MockServer) Close() {
	if ms.Server != nil {
		ms.Server.Close()
	}
}

func (ms *MockServer) writeThrottled(w io.Writer, data []byte) {
	if ms.Config.RateLimitKBps <= 0 {
		_, _ = w.Write(data)
		return
	}

	chunkSize := 64 * 1024 // 64KB chunks
	delayPerChunk := time.Duration(float64(chunkSize) / float64(ms.Config.RateLimitKBps*1024) * float64(time.Second))

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		_, _ = w.Write(data[i:end])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if delayPerChunk > 0 {
			time.Sleep(delayPerChunk)
		}
	}
}
