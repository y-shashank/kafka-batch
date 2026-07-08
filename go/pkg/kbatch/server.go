package kbatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// ServerConfig configures the kbatch sidecar HTTP server.
type ServerConfig struct {
	SocketPath string
}

// Server serves handler execution over a Unix domain socket.
type Server struct {
	cfg ServerConfig
}

// NewServer returns a sidecar server.
func NewServer(cfg ServerConfig) *Server {
	return &Server{cfg: cfg}
}

// ListenAndServe blocks until ctx is cancelled or the server errors.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.cfg.SocketPath == "" {
		return fmt.Errorf("socket path is required")
	}

	if err := os.MkdirAll(filepath.Dir(s.cfg.SocketPath), 0o755); err != nil {
		return fmt.Errorf("create socket dir: %w", err)
	}
	_ = os.Remove(s.cfg.SocketPath)

	ln, err := net.Listen("unix", s.cfg.SocketPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", s.cfg.SocketPath, err)
	}
	defer ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/v1/execute", s.handleExecute)

	srv := &http.Server{Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{OK: true})
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var req ExecuteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.JobType == "" {
		http.Error(w, "job_type required", http.StatusBadRequest)
		return
	}

	resp := runHandler(req)
	status := http.StatusOK
	if !resp.OK {
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, resp)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
