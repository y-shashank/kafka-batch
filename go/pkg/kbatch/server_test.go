package kbatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
)

func unixClient(socket string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			},
		},
	}
}

func TestServerExecuteOverUnixSocket(t *testing.T) {
	t.Cleanup(kbatch.Reset)

	var seenJobID string
	kbatch.Register("segment.export", func(ctx *kbatch.Context) error {
		seenJobID = ctx.JobID
		id, ok := ctx.Payload["segment_id"].(float64)
		if !ok || id != 42 {
			return &kbatch.HandlerError{Class: "segment.Invalid", Message: "bad segment_id"}
		}
		return nil
	})

	socket := filepath.Join(t.TempDir(), "kbatch.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- kbatch.NewServer(kbatch.ServerConfig{SocketPath: socket}).ListenAndServe(ctx)
	}()
	waitForSocket(t, socket)

	client := unixClient(socket)
	reqBody := readFixture(t, "../../../protocol/execute_request.json")
	resp := post(t, client, "/v1/execute", reqBody)

	if !resp["ok"].(bool) {
		t.Fatalf("expected ok, got %v", resp)
	}
	if seenJobID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("handler job_id %q", seenJobID)
	}

	health := get(t, client, "/health")
	if health["ok"] != true {
		t.Fatalf("health: %v", health)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exit: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerHandlerErrorResponse(t *testing.T) {
	t.Cleanup(kbatch.Reset)

	kbatch.Register("segment.export", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "segment.NotFound", Message: "segment 42 not found"}
	})

	socket := filepath.Join(t.TempDir(), "kbatch.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go kbatch.NewServer(kbatch.ServerConfig{SocketPath: socket}).ListenAndServe(ctx)
	waitForSocket(t, socket)

	client := unixClient(socket)
	resp := post(t, client, "/v1/execute", readFixture(t, "../../../protocol/execute_request.json"))
	if resp["ok"].(bool) {
		t.Fatal("expected failure")
	}
	if resp["error_class"] != "segment.NotFound" {
		t.Fatalf("error_class: %v", resp["error_class"])
	}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", path)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("socket %s not ready", path)
}

func post(t *testing.T, client *http.Client, path string, body []byte) map[string]interface{} {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://unix"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return decodeJSON(t, resp.Body)
}

func get(t *testing.T, client *http.Client, path string) map[string]interface{} {
	t.Helper()
	resp, err := client.Get("http://unix" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return decodeJSON(t, resp.Body)
}

func decodeJSON(t *testing.T, r io.Reader) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := json.NewDecoder(r).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func readFixture(t *testing.T, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(rel)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
