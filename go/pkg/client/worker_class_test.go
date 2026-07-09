package client

import (
	"testing"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

func TestLookupWorkerClassFromManifest(t *testing.T) {
	c := &Client{
		cfg: DefaultConfig(),
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"email.send": {
				Runtime:     config.RuntimeRuby,
				WorkerClass: "MyApp::EmailWorker",
				Topic:       "jobs.email",
			},
		}},
	}
	c.buildWorkerIndex()

	jt, entry, err := c.lookupWorkerClass("MyApp::EmailWorker")
	if err != nil {
		t.Fatal(err)
	}
	if jt != "email.send" || entry.WorkerClass != "MyApp::EmailWorker" {
		t.Fatalf("jt=%s entry=%+v", jt, entry)
	}
}

func TestLookupWorkerClassFromConfigMap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workers = map[string]WorkerClassConfig{
		"MyApp::LegacyWorker": {Topic: "jobs.legacy"},
	}
	c := &Client{cfg: cfg, manifest: config.Manifest{}}
	c.buildWorkerIndex()

	jt, entry, err := c.lookupWorkerClass("MyApp::LegacyWorker")
	if err != nil {
		t.Fatal(err)
	}
	if jt != "MyApp::LegacyWorker" || entry.Topic != "jobs.legacy" {
		t.Fatalf("jt=%s entry=%+v", jt, entry)
	}
}

func TestBuildWorkerMessageUsesClassName(t *testing.T) {
	c := &Client{cfg: DefaultConfig()}
	entry := config.HandlerEntry{Runtime: config.RuntimeRuby, WorkerClass: "MyApp::EmailWorker"}
	msg := c.buildWorkerMessage(entry, "email.send", "MyApp::EmailWorker", map[string]interface{}{"x": 1}, "j1", nil, PushOptions{}, nil)
	if msg.WorkerClass != "MyApp::EmailWorker" || msg.JobType != "email.send" {
		t.Fatalf("msg %+v", msg)
	}
}

func TestUnknownWorkerClass(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}
	c.buildWorkerIndex()
	_, _, err := c.lookupWorkerClass("Missing::Worker")
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err %v", err)
	}
}
