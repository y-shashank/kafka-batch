package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

func TestRubySocketExecutorSuccess(t *testing.T) {
	socket := startRubyWorkerTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["job_type"] != "orders.process" {
			t.Fatalf("job_type %+v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})

	exec := RubySocketExecutor{SocketPath: socket, Timeout: 5 * time.Second}
	jobID := "j1"
	err := exec.Execute(context.Background(), protocol.JobMessage{
		JobType: "orders.process", JobID: jobID, Payload: map[string]interface{}{"n": 1}, Attempt: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRubySocketExecutorFailure(t *testing.T) {
	socket := startRubyWorkerTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok": false, "error_class": "RuntimeError", "error_message": "boom",
		})
	})

	exec := RubySocketExecutor{SocketPath: socket, Timeout: 5 * time.Second}
	err := exec.Execute(context.Background(), protocol.JobMessage{
		JobType: "fail.job", JobID: "j1", Payload: map[string]interface{}{}, Attempt: 0,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*RubyExecutionError)
	if !ok || re.Class != "RuntimeError" || re.Message != "boom" {
		t.Fatalf("err %+v", err)
	}
}

func startRubyWorkerTestServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	dir := filepath.Join("..", "tmp", "go-test-sockets")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, fmt.Sprintf("worker-%d.sock", time.Now().UnixNano()))
	_ = os.Remove(socket)

	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close(); _ = os.Remove(socket) })

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		handler(w, r)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return socket
}

func TestProcessRubyHandlerViaSocket(t *testing.T) {
	socket := startRubyWorkerTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["job_type"] != "ruby.job" {
			t.Fatalf("job_type %+v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "b-ruby"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "ruby.job", WorkerClass: "RubyWorker",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, CompleteAfterRetries: 3,
		BatchSeq: &seq,
	})

	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"ruby.job": {Runtime: "ruby", Topic: "jobs"},
	}}
	p := &Processor{
		Cfg:      config.DefaultDaemon(),
		Manifest: manifest,
		Store:    st,
		Producer: &memProducer{},
		RubyExec: RubySocketExecutor{SocketPath: socket, Timeout: 5 * time.Second},
	}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out.Event == nil || out.Event.Status != "success" {
		t.Fatalf("event %+v", out.Event)
	}
}
