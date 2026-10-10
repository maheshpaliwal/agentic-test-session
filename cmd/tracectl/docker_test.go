package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"tsession/controlplane"
)

const cannedEvents = `{"status":"start","id":"c1aaaaaaaaaaaa","from":"img","Type":"container","Action":"start","Actor":{"ID":"c1aaaaaaaaaaaa","Attributes":{"image":"img","name":"web","tsession.id":"checkout-flow"}},"scope":"local","time":1700000000}
{"status":"start","id":"c2","Type":"container","Action":"start","Actor":{"ID":"c2","Attributes":{"name":"unlabeled"}}}
{"Type":"network","Action":"connect","Actor":{"ID":"n1","Attributes":{"tsession.id":"x"}}}
not json
{"status":"exec_start: sh","Type":"container","Action":"exec_start: sh","Actor":{"ID":"c1aaaaaaaaaaaa","Attributes":{"tsession.id":"checkout-flow"}}}
{"status":"die","id":"c1aaaaaaaaaaaa","Type":"container","Action":"die","Actor":{"ID":"c1aaaaaaaaaaaa","Attributes":{"exitCode":"0","tsession.id":"checkout-flow"}}}
{"status":"die","id":"c9","Type":"container","Action":"die","Actor":{"ID":"c9","Attributes":{"tsession.id":"never-started"}}}
`

func TestParseDockerEvent(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(cannedEvents), "\n")
	type want struct {
		ok  bool
		err bool
		ev  DockerEvent
	}
	wants := []want{
		{ok: true, ev: DockerEvent{"start", "c1aaaaaaaaaaaa", "checkout-flow"}},
		{}, {}, {err: true}, {},
		{ok: true, ev: DockerEvent{"die", "c1aaaaaaaaaaaa", "checkout-flow"}},
		{ok: true, ev: DockerEvent{"die", "c9", "never-started"}},
	}
	for i, l := range lines {
		ev, ok, err := ParseDockerEvent(l)
		if ok != wants[i].ok || (err != nil) != wants[i].err || (ok && ev != wants[i].ev) {
			t.Errorf("line %d: got %+v ok=%v err=%v; want %+v", i, ev, ok, err, wants[i])
		}
	}
	// legacy format: only status/id, no Type/Action/Actor.ID
	ev, ok, _ := ParseDockerEvent(`{"status":"start","id":"abc","Actor":{"Attributes":{"tsession.id":"n"}}}`)
	if !ok || ev.ContainerID != "abc" || ev.Name != "n" {
		t.Errorf("legacy: %+v %v", ev, ok)
	}
}

func TestWatcherRun(t *testing.T) {
	var mu sync.Mutex
	var started, stopped []string
	w := &Watcher{
		Start: func(_ context.Context, name string) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			started = append(started, name)
			return "ts_" + name, nil
		},
		Stop: func(_ context.Context, id string) error {
			mu.Lock()
			defer mu.Unlock()
			stopped = append(stopped, id)
			return nil
		},
	}
	// duplicate start should not start twice
	in := cannedEvents + strings.Split(cannedEvents, "\n")[0] + "\n"
	if err := w.Run(context.Background(), strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}
	if len(started) != 2 || started[0] != "checkout-flow" {
		t.Fatalf("started %v", started)
	}
	if len(stopped) != 1 || stopped[0] != "ts_checkout-flow" {
		t.Fatalf("stopped %v (die of untracked container must be ignored)", stopped)
	}
}

func TestCLIAgainstControlPlane(t *testing.T) {
	m := controlplane.NewManager(controlplane.Config{}) // no collector: stays DRAINING
	defer m.Close()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	_ = http.StatusOK

	var out, errb bytes.Buffer
	if code := run([]string{"--cp", srv.URL, "start", "--name", "cli-test"}, &out, &errb); code != 0 {
		t.Fatalf("start: %d %s", code, errb.String())
	}
	if !strings.Contains(out.String(), `"state": "ACTIVE"`) {
		t.Fatalf("%s", out.String())
	}
	out.Reset()
	if code := run([]string{"--cp", srv.URL, "list", "--state", "ACTIVE"}, &out, &errb); code != 0 || !strings.Contains(out.String(), "cli-test") {
		t.Fatalf("list: %d %q %s", code, out.String(), errb.String())
	}
	out.Reset()
	if code := run([]string{"--cp", srv.URL, "stop", "cli-test"}, &out, &errb); code != 0 || !strings.Contains(out.String(), "DRAINING") {
		t.Fatalf("stop by name: %d %q %s", code, out.String(), errb.String())
	}
	out.Reset()
	if code := run([]string{"--cp", srv.URL, "status", "ghost"}, &out, &errb); code == 0 {
		t.Fatal("status of unknown name should fail")
	}
	if code := run([]string{"bogus"}, &out, &errb); code == 0 {
		t.Fatal("unknown command should fail")
	}
}
