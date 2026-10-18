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
	"tsession/sdk/client"
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
		{ok: true, ev: DockerEvent{Action: "start", ContainerID: "c1aaaaaaaaaaaa", Name: "checkout-flow"}},
		{}, {}, {err: true}, {},
		{ok: true, ev: DockerEvent{Action: "die", ContainerID: "c1aaaaaaaaaaaa", Name: "checkout-flow"}},
		{ok: true, ev: DockerEvent{Action: "die", ContainerID: "c9", Name: "never-started"}},
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
		Start: func(_ context.Context, name string, _ []client.Binding) (string, error) {
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

func newCountingWatcher() (*Watcher, *[]string, *[]string) {
	var started, stopped []string
	w := &Watcher{
		Start: func(_ context.Context, name string, _ []client.Binding) (string, error) {
			started = append(started, name)
			return "ts_" + name, nil
		},
		Stop: func(_ context.Context, id string) error {
			stopped = append(stopped, id)
			return nil
		},
	}
	return w, &started, &stopped
}

func TestSharedSessionRefcount(t *testing.T) {
	w, started, stopped := newCountingWatcher()
	ctx := context.Background()
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "a", Name: "flow"})
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "b", Name: "flow"})
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "c", Name: "other"})
	if len(*started) != 2 {
		t.Fatalf("started %v (same label must share one session)", *started)
	}
	w.Handle(ctx, DockerEvent{Action: "die", ContainerID: "a", Name: "flow"})
	if len(*stopped) != 0 {
		t.Fatalf("stopped early: %v", *stopped)
	}
	w.Handle(ctx, DockerEvent{Action: "die", ContainerID: "b", Name: "flow"})
	if len(*stopped) != 1 || (*stopped)[0] != "ts_flow" {
		t.Fatalf("stopped %v", *stopped)
	}
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "d", Name: "flow"}) // new session after the last one ended
	if len(*started) != 3 {
		t.Fatalf("started %v", *started)
	}
}

func TestParseDockerPS(t *testing.T) {
	got := ParseDockerPS([]byte("abc123\tflow\n\nnolabel\t\ndef456\tother\r\ngarbage\n"))
	want := []DockerEvent{{Action: "start", ContainerID: "abc123", Name: "flow"}, {Action: "start", ContainerID: "def456", Name: "other"}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("%+v", got)
	}
}

func TestStartupListAndReconcile(t *testing.T) {
	w, started, stopped := newCountingWatcher()
	psOut := "c1\tflow\nc2\tflow\nc3\tsolo\n"
	var args []string
	w.Runner = func(_ context.Context, bin string, a ...string) ([]byte, error) {
		args = append([]string{bin}, a...)
		return []byte(psOut), nil
	}
	ctx := context.Background()
	running, err := w.ListRunning(ctx, "docker")
	if err != nil || len(running) != 3 || args[1] != "ps" || !strings.Contains(strings.Join(args, " "), "label=tsession.id") {
		t.Fatalf("%v %v %v", running, err, args)
	}
	w.Reconcile(ctx, running)
	if len(*started) != 2 {
		t.Fatalf("attach on startup: %v", *started)
	}
	// reconcile again with an identical list: no-op
	w.Reconcile(ctx, running)
	if len(*started) != 2 || len(*stopped) != 0 {
		t.Fatalf("not idempotent: %v %v", *started, *stopped)
	}
	// reconnect: c2 and c3 vanished while disconnected, c4 appeared
	w.Reconcile(ctx, []DockerEvent{{Action: "start", ContainerID: "c1", Name: "flow"}, {Action: "start", ContainerID: "c4", Name: "new"}})
	if len(*stopped) != 1 || (*stopped)[0] != "ts_solo" {
		t.Fatalf("stopped %v (flow must survive: c1 still alive)", *stopped)
	}
	if len(*started) != 3 || (*started)[2] != "new" {
		t.Fatalf("started %v", *started)
	}
	w.Reconcile(ctx, nil)
	if len(*stopped) != 3 {
		t.Fatalf("stopped %v", *stopped)
	}
}

func TestParseBindLabelAndPS(t *testing.T) {
	got := ParseDockerPS([]byte("a1\tflow\theader:x-user-id=u_1,jwt:sub=u_1\nb2\tflow\t\n"))
	if len(got) != 2 || got[0].Bind != "header:x-user-id=u_1,jwt:sub=u_1" || got[1].Bind != "" {
		t.Fatalf("%+v", got)
	}
	ev, ok, _ := ParseDockerEvent(`{"Type":"container","Action":"start","Actor":{"ID":"c","Attributes":{"tsession.id":"n","tsession.bind":"cookie:sid=z"}}}`)
	if !ok || ev.Bind != "cookie:sid=z" {
		t.Fatalf("%+v", ev)
	}
	bs, err := parseBindLabel("header:X-User-Id=u_1, jwt:sub=u_1")
	if err != nil || len(bs) != 2 || bs[1].Source != "jwt_claim" || bs[1].Name != "sub" {
		t.Fatalf("%+v %v", bs, err)
	}
	if _, err := parseBindLabel("nonsense"); err == nil {
		t.Fatal("want error")
	}
	if _, err := parseBindLabel("body:x=1"); err == nil {
		t.Fatal("bad source must fail")
	}
}

func TestWatcherBindingsSharing(t *testing.T) {
	var gotBinds [][]client.Binding
	var started, stopped []string
	w := &Watcher{
		Start: func(_ context.Context, name string, b []client.Binding) (string, error) {
			gotBinds = append(gotBinds, b)
			id := "ts_" + name + string(rune('0'+len(started)))
			started = append(started, id)
			return id, nil
		},
		Stop: func(_ context.Context, id string) error { stopped = append(stopped, id); return nil },
	}
	ctx := context.Background()
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "a", Name: "f", Bind: "header:x-user-id=u1"})
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "b", Name: "f", Bind: "header:X-User-Id=u1"}) // same bindings: shared
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "c", Name: "f", Bind: "header:x-user-id=u2"}) // different: own session
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "d", Name: "f"})                              // unbound: own session
	w.Handle(ctx, DockerEvent{Action: "start", ContainerID: "e", Name: "f", Bind: "garbage"})             // bad label: ignored
	if len(started) != 3 {
		t.Fatalf("started %v", started)
	}
	if len(gotBinds[0]) != 1 || gotBinds[0][0] != (client.Binding{Source: "header", Name: "x-user-id", Value: "u1"}) || len(gotBinds[2]) != 0 {
		t.Fatalf("binds %+v", gotBinds)
	}
	w.Handle(ctx, DockerEvent{Action: "die", ContainerID: "a"})
	if len(stopped) != 0 {
		t.Fatalf("stopped early %v", stopped)
	}
	w.Handle(ctx, DockerEvent{Action: "die", ContainerID: "b"})
	w.Handle(ctx, DockerEvent{Action: "die", ContainerID: "e"})
	if len(stopped) != 1 || stopped[0] != started[0] {
		t.Fatalf("stopped %v", stopped)
	}
}

func TestCLIBindFlags(t *testing.T) {
	m := controlplane.NewManager(controlplane.Config{})
	defer m.Close()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	var out, errb bytes.Buffer
	code := run([]string{"--cp", srv.URL, "start", "--name", "b1", "--bind", "header:X-User-Id=u_123", "--bind", "jwt:sub=u_123", "--bind", "cookie:sid=c", "--bind", "query:uid=7"}, &out, &errb)
	if code != 0 {
		t.Fatalf("start: %d %s", code, errb.String())
	}
	for _, w := range []string{`"source": "header"`, `"name": "x-user-id"`, `"source": "jwt_claim"`, `"source": "cookie"`, `"source": "query"`} {
		if !strings.Contains(out.String(), w) {
			t.Fatalf("missing %s in %s", w, out.String())
		}
	}
	if !strings.Contains(errb.String(), "WITHOUT verifying") {
		t.Fatalf("no spoofing warning: %q", errb.String())
	}
	// header-only start: no warning
	errb.Reset()
	if code := run([]string{"--cp", srv.URL, "start", "--bind", "header:x-other=1"}, &out, &errb); code != 0 || strings.Contains(errb.String(), "WITHOUT") {
		t.Fatalf("%d %q", code, errb.String())
	}
	// conflict -> failure
	errb.Reset()
	if code := run([]string{"--cp", srv.URL, "start", "--bind", "header:x-user-id=u_123"}, &out, &errb); code == 0 || !strings.Contains(errb.String(), "u_123") && !strings.Contains(errb.String(), "conflict") {
		t.Fatalf("conflict: %d %q", code, errb.String())
	}
	// bad specs
	for _, bad := range []string{"nosep", "body:x=1", "header:=v", "header:x="} {
		if code := run([]string{"--cp", srv.URL, "start", "--bind", bad}, &out, &errb); code == 0 {
			t.Errorf("%q accepted", bad)
		}
	}
	// exclusive + bind -> 400
	if code := run([]string{"--cp", srv.URL, "start", "--mode", "exclusive", "--bind", "header:x=1"}, &out, &errb); code == 0 {
		t.Error("exclusive+bind accepted")
	}
	out.Reset()
	run([]string{"--cp", srv.URL, "status", "b1"}, &out, &errb)
	if !strings.Contains(out.String(), "bindings: header:x-user-id=u_123,jwt_claim:sub=u_123") {
		t.Fatalf("status: %s", out.String())
	}
	out.Reset()
	run([]string{"--cp", srv.URL, "list"}, &out, &errb)
	if !strings.Contains(out.String(), "[header:x-user-id=u_123,jwt_claim:sub=u_123,cookie:sid=c,query:uid=7]") {
		t.Fatalf("list: %s", out.String())
	}
}

func TestCLIRenameAndAutoName(t *testing.T) {
	m := controlplane.NewManager(controlplane.Config{})
	defer m.Close()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	var out, errb bytes.Buffer
	// no --name with --bind: tracectl sends no name; server auto-names
	if code := run([]string{"--cp", srv.URL, "start", "--bind", "jwt:sub=u_123"}, &out, &errb); code != 0 {
		t.Fatalf("%d %s", code, errb.String())
	}
	if !strings.Contains(out.String(), `"name": "jwt:sub=u_123"`) || !strings.Contains(out.String(), `"name_auto": true`) {
		t.Fatalf("%s", out.String())
	}
	out.Reset()
	run([]string{"--cp", srv.URL, "list"}, &out, &errb)
	if !strings.Contains(out.String(), "jwt:sub=u_123 (auto)") {
		t.Fatalf("list: %s", out.String())
	}
	// rename by (auto) name with multi-word new name (unquoted) and a template
	out.Reset()
	if code := run([]string{"--cp", srv.URL, "rename", "jwt:sub=u_123", "checkout", "{bind}"}, &out, &errb); code != 0 {
		t.Fatalf("rename: %d %s", code, errb.String())
	}
	if !strings.Contains(out.String(), `"name": "checkout u_123"`) || !strings.Contains(out.String(), `"name_auto": false`) {
		t.Fatalf("%s", out.String())
	}
	out.Reset()
	run([]string{"--cp", srv.URL, "status", "checkout u_123"}, &out, &errb)
	if !strings.Contains(out.String(), "name: checkout u_123\n") || strings.Contains(out.String(), "(auto)") {
		t.Fatalf("status: %s", out.String())
	}
	// template via --name
	out.Reset()
	if code := run([]string{"--cp", srv.URL, "start", "--name", "activity {bind.x-wms-uid}", "--bind", "header:X-WMS-UID=act_bind_9"}, &out, &errb); code != 0 || !strings.Contains(out.String(), `"name": "activity act_bind_9"`) {
		t.Fatalf("%d %s %s", code, out.String(), errb.String())
	}
	if code := run([]string{"--cp", srv.URL, "rename", "ghost", "x"}, &out, &errb); code == 0 {
		t.Fatal("rename unknown should fail")
	}
	if code := run([]string{"--cp", srv.URL, "rename", "onlyone"}, &out, &errb); code == 0 {
		t.Fatal("rename without name should fail")
	}
}

func TestWatcherNameLabel(t *testing.T) {
	ev, ok, err := ParseDockerEvent(`{"Type":"container","Action":"start","Actor":{"ID":"c1","Attributes":{"tsession.id":"grp","tsession.name":"run {bind}"}}}`)
	if err != nil || !ok || ev.SessName != "run {bind}" || ev.Name != "grp" {
		t.Fatalf("%+v %v %v", ev, ok, err)
	}
	ps := ParseDockerPS([]byte("c1\tgrp\theader:x=1\trun {bind}\nc2\tgrp\t\t\n"))
	if len(ps) != 2 || ps[0].SessName != "run {bind}" || ps[1].SessName != "" || ps[1].Bind != "" {
		t.Fatalf("%+v", ps)
	}
	var names []string
	w := &Watcher{
		Start: func(_ context.Context, name string, _ []client.Binding) (string, error) {
			names = append(names, name)
			return "ts_x", nil
		},
		Stop: func(context.Context, string) error { return nil },
	}
	w.Handle(context.Background(), DockerEvent{Action: "start", ContainerID: "a", Name: "grp", SessName: "run {bind}"})
	w.Handle(context.Background(), DockerEvent{Action: "start", ContainerID: "b", Name: "grp"})
	if len(names) != 2 || names[0] != "run {bind}" || names[1] != "grp" {
		t.Fatalf("%v", names)
	}
}
