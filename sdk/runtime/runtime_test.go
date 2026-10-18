package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tsession/core"
)

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

type fakeCollector struct {
	mu      sync.Mutex
	events  []core.Event
	posts   int
	inst    string
	failFor int32 // number of requests to fail with 503
}

func (f *fakeCollector) handler(w http.ResponseWriter, r *http.Request) {
	if atomic.AddInt32(&f.failFor, -1) >= 0 {
		http.Error(w, "busy", 503)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.posts++
	f.inst = r.Header.Get("X-TSession-Instance")
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		var e core.Event
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			f.events = append(f.events, e)
		}
	}
	fmt.Fprint(w, `{"accepted":1,"duplicates":0}`)
}

func (f *fakeCollector) snapshot() ([]core.Event, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.Event(nil), f.events...), f.posts
}

func TestRingOverflowEmitsDropEvent(t *testing.T) {
	r := NewRecorder(4)
	for i := 0; i < 10; i++ {
		r.Record(core.Event{SessionID: "ts_a", SpanID: strconv.Itoa(i)})
	}
	r.Record(core.Event{SessionID: "ts_b"}) // also dropped
	out := r.Drain(100)
	var kept, dropped int
	counts := map[string]string{}
	for _, e := range out {
		if e.Kind == core.KindSDKDropped {
			dropped++
			counts[e.SessionID] = e.Attrs["count"]
		} else {
			kept++
		}
	}
	if kept != 4 || dropped != 2 || counts["ts_a"] != "6" || counts["ts_b"] != "1" {
		t.Fatalf("kept=%d dropped=%d counts=%v", kept, dropped, counts)
	}
	if out[0].SpanID != "0" {
		t.Fatal("FIFO order broken")
	}
	if len(r.Drain(10)) != 0 {
		t.Fatal("drop counters not reset")
	}
}

func TestRecorderNeverBlocks(t *testing.T) {
	r := NewRecorder(2)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100000; i++ {
			r.Record(core.Event{SessionID: "s"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Record blocked")
	}
}

func TestExporterBatchingAndRetry(t *testing.T) {
	fc := &fakeCollector{failFor: 1}
	srv := httptest.NewServer(http.HandlerFunc(fc.handler))
	defer srv.Close()
	core.Configure(core.Config{Service: "svc", Instance: "inst-1", Enabled: true})
	defer core.Configure(core.Config{})

	r := NewRecorder(1000)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, BatchSize: 10, FlushInterval: 20 * time.Millisecond, RetryBackoff: 5 * time.Millisecond})
	for i := 0; i < 25; i++ {
		r.Record(core.Event{SessionID: "s", LocalSeq: uint64(i + 1)})
	}
	if !x.Flush(3 * time.Second) {
		t.Fatal("flush timeout")
	}
	x.Close(time.Second)
	evs, posts := fc.snapshot()
	if len(evs) != 25 || posts != 3 {
		t.Fatalf("events=%d posts=%d", len(evs), posts)
	}
	if !strings.HasPrefix(fc.inst, "inst-1-") || fc.inst != core.GetConfig().Instance {
		t.Fatalf("instance header %q", fc.inst)
	}
}

func TestExporterCollectorDownDoesNotBlock(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens
	r := NewRecorder(8)
	x := NewExporter(r, ExporterConfig{CollectorURL: url, FlushInterval: 10 * time.Millisecond, RetryBackoff: time.Millisecond, HTTPTimeout: 200 * time.Millisecond})
	start := time.Now()
	for i := 0; i < 1000; i++ {
		r.Record(core.Event{SessionID: "s"})
	}
	if time.Since(start) > time.Second {
		t.Fatal("Record slowed by dead collector")
	}
	time.Sleep(100 * time.Millisecond)
	cs := time.Now()
	x.Close(time.Second)
	if time.Since(cs) > 3*time.Second {
		t.Fatal("Close took too long")
	}
}

func TestExporterRecoversAndReportsDrops(t *testing.T) {
	fc := &fakeCollector{failFor: 1000}
	srv := httptest.NewServer(http.HandlerFunc(fc.handler))
	defer srv.Close()
	r := NewRecorder(5)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, FlushInterval: 10 * time.Millisecond, RetryBackoff: time.Millisecond, MaxCooldown: 20 * time.Millisecond})
	defer x.Close(time.Second)
	for i := 0; i < 8; i++ { // 3 overflow
		r.Record(core.Event{SessionID: "s"})
	}
	time.Sleep(100 * time.Millisecond)
	atomic.StoreInt32(&fc.failFor, 0)
	eventually(t, "drop event", func() bool {
		evs, _ := fc.snapshot()
		for _, e := range evs {
			if e.Kind == core.KindSDKDropped && e.Attrs["count"] != "" {
				return true
			}
		}
		return false
	})
}

// fakeCP serves /v1/active and /v1/active/stream from mutable state.
type fakeCP struct {
	mu      sync.Mutex
	state   string
	notify  chan struct{}
	streams int32
	drop    chan struct{} // closing handlers on signal
}

func newFakeCP(state string) *fakeCP {
	return &fakeCP{state: state, notify: make(chan struct{}, 8), drop: make(chan struct{}, 8)}
}

func (c *fakeCP) set(s string) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
	c.notify <- struct{}{}
}
func (c *fakeCP) get() string { c.mu.Lock(); defer c.mu.Unlock(); return c.state }

func (c *fakeCP) mux() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/v1/active", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, c.get()) })
	m.HandleFunc("/v1/active/stream", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&c.streams, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprintf(w, ": hello\n\ndata: %s\n\n", c.get())
		fl.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-c.drop:
				return
			case <-c.notify:
				fmt.Fprintf(w, "data: %s\n\n", c.get())
				fl.Flush()
			}
		}
	})
	return m
}

func TestSubscriberUpdatesKillAndReconnect(t *testing.T) {
	cp := newFakeCP(`{"active":["ts_1"],"exclusive":"","kill":false}`)
	srv := httptest.NewServer(cp.mux())
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	s.maxBackoff = 50 * time.Millisecond

	eventually(t, "initial", func() bool { return s.IsActive("ts_1") })
	if s.IsActive("ts_2") {
		t.Fatal("ts_2 active")
	}
	cp.set(`{"active":["ts_2"],"exclusive":"ts_2","kill":false}`)
	eventually(t, "update", func() bool { return s.IsActive("ts_2") && !s.IsActive("ts_1") })
	if id, ok := s.Exclusive(); !ok || id != "ts_2" {
		t.Fatal("exclusive")
	}
	cp.set(`{"active":["ts_2"],"exclusive":"ts_2","kill":true}`)
	eventually(t, "kill", func() bool { return !s.IsActive("ts_2") })
	if _, ok := s.Exclusive(); ok {
		t.Fatal("exclusive during kill")
	}

	// drop the stream, change state while disconnected, expect reconnect
	before := atomic.LoadInt32(&cp.streams)
	cp.mu.Lock()
	cp.state = `{"active":["ts_3"],"exclusive":"","kill":false}`
	cp.mu.Unlock()
	cp.drop <- struct{}{}
	eventually(t, "reconnect", func() bool { return atomic.LoadInt32(&cp.streams) > before && s.IsActive("ts_3") })
}

func TestSubscriberPollFallbackWithoutStream(t *testing.T) {
	m := http.NewServeMux()
	m.HandleFunc("/v1/active", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"active":["ts_p"],"kill":false}`)
	})
	srv := httptest.NewServer(m) // /v1/active/stream -> 404
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	eventually(t, "poll", func() bool { return s.IsActive("ts_p") })
}

func TestEndToEnd(t *testing.T) {
	fc := &fakeCollector{}
	col := httptest.NewServer(http.HandlerFunc(fc.handler))
	defer col.Close()
	cp := newFakeCP(`{"active":["ts_e2e"],"exclusive":"","kill":false}`)
	cps := httptest.NewServer(cp.mux())
	defer cps.Close()

	if err := Start(Config{Service: "orders", Instance: "i-1", ControlPlaneURL: cps.URL, CollectorURL: col.URL,
		Exporter: ExporterConfig{FlushInterval: 20 * time.Millisecond}}); err != nil {
		t.Fatal(err)
	}
	defer Stop()
	if err := Start(Config{}); err != ErrStarted {
		t.Fatalf("second start: %v", err)
	}
	eventually(t, "active set", func() bool { return core.Enabled() && ActiveRecorder() != nil })

	h := http.Header{}
	h.Set(core.KeySession, "ts_e2e")
	eventually(t, "session active", func() bool {
		_, s := core.BeginIngress(context.Background(), core.KindAPIIn, "probe", core.HTTPHeaderCarrier(h))
		return s != nil
	})
	ctx, root := core.BeginIngress(context.Background(), core.KindAPIIn, "GET /orders", core.HTTPHeaderCarrier(h))
	_, child := core.Begin(ctx, core.KindDBWrite, "INSERT")
	child.SetReq(map[string]any{"password": "p", "id": 1})
	child.End()
	root.End()
	// inactive session: nothing recorded
	h2 := http.Header{}
	h2.Set(core.KeySession, "ts_other")
	if _, s := core.BeginIngress(context.Background(), core.KindAPIIn, "x", core.HTTPHeaderCarrier(h2)); s != nil {
		t.Fatal("inactive session traced")
	}

	if !Flush(3 * time.Second) {
		t.Fatal("flush")
	}
	evs, _ := fc.snapshot()
	var got []string
	for _, e := range evs {
		if e.Name != "probe" {
			got = append(got, e.Name)
		}
		if e.Service != "orders" || !strings.HasPrefix(e.Instance, "i-1-") {
			t.Fatalf("bad identity %+v", e)
		}
		if e.Name == "INSERT" && (!e.Req.Redacted || strings.Contains(string(e.Req.Inline), `"p"`)) {
			t.Fatalf("not redacted: %+v", e.Req)
		}
	}
	if len(got) != 2 || got[0] != "INSERT" || got[1] != "GET /orders" {
		t.Fatalf("events %v", got)
	}
}

func TestTransportInjectsAndIsIdempotent(t *testing.T) {
	var gotSess atomic.Value
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSess.Store(r.Header.Get(core.KeySession))
		w.WriteHeader(201)
	}))
	defer backend.Close()
	cp := newFakeCP(`{"active":["ts_t"],"kill":false}`)
	cps := httptest.NewServer(cp.mux())
	defer cps.Close()

	orig := http.DefaultTransport
	cfg := Config{Service: "s", ControlPlaneURL: cps.URL, CollectorURL: "http://127.0.0.1:1", PatchDefaultTransport: true}
	if err := Start(cfg); err != nil {
		t.Fatal(err)
	}
	patched := http.DefaultTransport
	if _, ok := patched.(*Transport); !ok {
		t.Fatal("not patched")
	}
	core.SetRecorder(nil)
	mem := &core.MemRecorder{}
	core.SetRecorder(mem)
	eventually(t, "active", func() bool {
		return core.Enabled() && ActiveRecorder() != nil && func() bool {
			h := http.Header{}
			h.Set(core.KeySession, "ts_t")
			_, s := core.BeginIngress(context.Background(), core.KindAPIIn, "p", core.HTTPHeaderCarrier(h))
			return s != nil
		}()
	})
	h := http.Header{}
	h.Set(core.KeySession, "ts_t")
	ctx, root := core.BeginIngress(context.Background(), core.KindAPIIn, "in", core.HTTPHeaderCarrier(h))
	req, _ := http.NewRequestWithContext(ctx, "GET", backend.URL+"/x", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	root.End()
	if gotSess.Load() != "ts_t" {
		t.Fatalf("session header not injected: %v", gotSess.Load())
	}
	var out *core.Event
	for _, e := range mem.Events() {
		if e.Kind == core.KindAPIOut {
			e := e
			out = &e
		}
	}
	if out == nil || out.Attrs["http.status"] != "201" {
		t.Fatalf("api.out missing: %+v", out)
	}
	Stop()
	if http.DefaultTransport != orig {
		t.Fatal("transport not restored")
	}
}

type testAttrs map[string]string

func (a testAttrs) Get(s, n string) string { return a[s+":"+n] }

func TestSubscriberBindings(t *testing.T) {
	cp := newFakeCP(`{"active":["ts_1"],"exclusive":""}`) // older CP: no bindings field
	srv := httptest.NewServer(cp.mux())
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	eventually(t, "initial", func() bool { return s.IsActive("ts_1") })
	if _, ok := s.MatchAttrs(testAttrs{"header:x-user-id": "u1"}); ok || len(s.BindingKeys()) != 0 {
		t.Fatal("no bindings field must mean no matches / no keys")
	}
	cp.set(`{"active":["ts_1","ts_2"],"kill":false,"bindings":[{"session_id":"ts_1","source":"header","name":"X-User-Id","value":"u1"},{"session_id":"ts_2","source":"jwt_claim","name":"sub","value":"u2"}]}`)
	eventually(t, "bindings", func() bool { return len(s.BindingKeys()) == 2 })
	if id, ok := s.MatchAttrs(testAttrs{"header:x-user-id": "u1"}); !ok || id != "ts_1" {
		t.Fatal(id, ok)
	}
	if id, k, ok := s.MatchAttrsKey(testAttrs{"jwt_claim:sub": "u2"}); !ok || id != "ts_2" || k.String() != "jwt_claim:sub" {
		t.Fatal(id, k, ok)
	}
	if _, ok := s.MatchAttrs(testAttrs{"header:x-user-id": "other"}); ok {
		t.Fatal("unknown value matched")
	}
	cp.set(`{"active":["ts_1","ts_2"],"kill":true,"bindings":[{"session_id":"ts_1","source":"header","name":"x-user-id","value":"u1"}]}`)
	eventually(t, "kill", func() bool { return !s.IsActive("ts_1") })
	if _, ok := s.MatchAttrs(testAttrs{"header:x-user-id": "u1"}); ok || len(s.BindingKeys()) != 0 {
		t.Fatal("kill switch must disable bindings")
	}
	cp.set(`{"active":["ts_2"],"kill":false,"bindings":[{"session_id":"ts_2","source":"jwt_claim","name":"sub","value":"u2"}]}`)
	eventually(t, "stopped", func() bool { return !s.IsActive("ts_1") && len(s.BindingKeys()) == 1 })
	if _, ok := s.MatchAttrs(testAttrs{"header:x-user-id": "u1"}); ok {
		t.Fatal("stopped session's binding must not match")
	}
}

func TestSubscriberBindingsViaPoll(t *testing.T) {
	m := http.NewServeMux()
	m.HandleFunc("/v1/active", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"active":["ts_p"],"bindings":[{"session_id":"ts_p","source":"query","name":"uid","value":"7"}]}`)
	})
	srv := httptest.NewServer(m)
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	eventually(t, "poll", func() bool { id, ok := s.MatchAttrs(testAttrs{"query:uid": "7"}); return ok && id == "ts_p" })
}

func TestSubscriberPriorityAmbiguityStats(t *testing.T) {
	cp := newFakeCP(`{"active":["ts_1","ts_2"],"bindings":[{"session_id":"ts_1","source":"header","name":"x-user-id","value":"1"},{"session_id":"ts_2","source":"header","name":"name","value":"bob"}]}`)
	srv := httptest.NewServer(cp.mux())
	defer srv.Close()
	s := NewSubscriber(srv.URL)
	defer s.Close()
	eventually(t, "bindings", func() bool { return len(s.BindingKeys()) == 2 })
	both := testAttrs{"header:x-user-id": "1", "header:name": "bob"}
	before := GetStats().BindingAmbiguous
	if m := s.MatchAttrsDetail(both); m.OK || !m.Ambiguous {
		t.Fatalf("expected ambiguous: %+v", m)
	}
	if GetStats().BindingAmbiguous != before+1 {
		t.Fatal("stats counter")
	}
	core.SetBindPriority(core.ParseBindPriority("header:x-user-id,header:name"))
	defer core.SetBindPriority(nil)
	if m := s.MatchAttrsDetail(both); !m.OK || m.SessionID != "ts_1" || m.Fallback {
		t.Fatalf("%+v", m)
	}
	if m := s.MatchAttrsDetail(testAttrs{"header:x-user-id": "9", "header:name": "bob"}); m.OK || m.Ambiguous {
		t.Fatalf("unbound priority key must not fall through: %+v", m)
	}
	if m := s.MatchAttrsDetail(testAttrs{"header:name": "bob"}); !m.OK || m.SessionID != "ts_2" || !m.Fallback {
		t.Fatalf("%+v", m)
	}
}
