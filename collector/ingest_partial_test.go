package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// partStore fails every call for the sessions marked bad, like a store whose
// partition for those sessions is unavailable.
type partStore struct {
	store.Store
	mu  sync.Mutex
	bad map[string]bool
}

func newPartStore(bad ...string) *partStore {
	p := &partStore{Store: store.NewMem(), bad: map[string]bool{}}
	for _, id := range bad {
		p.bad[id] = true
	}
	return p
}

func (p *partStore) set(id string, bad bool) {
	p.mu.Lock()
	p.bad[id] = bad
	p.mu.Unlock()
}

func (p *partStore) check(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bad[id] {
		return errInjected
	}
	return nil
}

func (p *partStore) AppendEvents(ctx context.Context, id string, evs []core.Event) error {
	if err := p.check(id); err != nil {
		return err
	}
	return p.Store.AppendEvents(ctx, id, evs)
}

func (p *partStore) Events(ctx context.Context, id string, from uint64, limit int) ([]core.Event, error) {
	if err := p.check(id); err != nil {
		return nil, err
	}
	return p.Store.Events(ctx, id, from, limit)
}

func (p *partStore) GetSeal(ctx context.Context, id string) (*store.SealInfo, error) {
	if err := p.check(id); err != nil {
		return nil, err
	}
	return p.Store.GetSeal(ctx, id)
}

func (p *partStore) Head(ctx context.Context, id string) (uint64, error) {
	if err := p.check(id); err != nil {
		return 0, err
	}
	return p.Store.Head(ctx, id)
}

func newPartC(t *testing.T, cfg Config, st store.Store) (*Collector, string) {
	t.Helper()
	c := New(st, cfg)
	t.Cleanup(c.Close)
	return c, newLoopbackServer(t, c.Handler())
}

// newLoopbackServer serves h on 127.0.0.1 until the test ends.
func newLoopbackServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// sev is an event of session id whose text fields are recognisable, so a test
// can tell whether a response echoes event data.
func sev(id string, local uint64) core.Event {
	return core.Event{SessionID: id, TraceID: "t", SpanID: fmt.Sprintf("span-%s-%d", id, local), Service: "svc",
		Instance: "i1", Kind: core.KindAPIIn, Name: "SECRET-NAME " + id, HLC: local, EndHLC: local, LocalSeq: local,
		Attrs: map[string]string{"secret-attr": "SECRET-VALUE"}, Err: "SECRET-ERR"}
}

// ingestBody is every field an ingest response can carry.
type ingestBody struct {
	Error string `json:"error"`
	IngestResult
}

// postRaw posts evs and returns the status, the raw body and the decoded body.
func postRaw(t *testing.T, url string, evs ...core.Event) (int, string, ingestBody) {
	t.Helper()
	resp, err := http.Post(url+"/v1/ingest", ctProto, bytes.NewReader(protoBody(t, evs...)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var b ingestBody
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatalf("status %d, body %q: %v", resp.StatusCode, raw, err)
	}
	return resp.StatusCode, string(raw), b
}

func storedOf(t *testing.T, c *Collector, id string) []uint64 {
	t.Helper()
	evs, err := c.st.Events(context.Background(), id, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, e := range evs {
		out = append(out, e.LocalSeq)
	}
	return out
}

// A request whose sessions are all accepted answers as it always did: 200 and
// the counters, without a failed_sessions field.
func TestIngestAllSessionsAccepted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	_, url := newPartC(t, cfg, newPartStore())
	code, raw, b := postRaw(t, url, sev("a", 1), sev("b", 2), sev("a", 3))
	if code != 200 || b.Accepted != 3 || b.Duplicates != 0 {
		t.Fatalf("status %d body %s", code, raw)
	}
	if strings.Contains(raw, "failed_sessions") || strings.Contains(raw, "error") {
		t.Fatalf("a successful response carries failure fields: %s", raw)
	}
}

// A request in which every session failed keeps its status and its error, and
// names the sessions, so an exporter can tell which of its sessions the
// collector could not take.
func TestIngestAllSessionsFailed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	_, url := newPartC(t, cfg, newPartStore("a", "b"))
	code, raw, b := postRaw(t, url, sev("a", 1), sev("b", 2))
	if code != http.StatusInternalServerError || b.Error != "storage error" {
		t.Fatalf("status %d body %s", code, raw)
	}
	if b.Accepted != 0 || len(b.FailedSessions) != 2 || b.FailedSessions["a"] != "storage" || b.FailedSessions["b"] != "storage" || b.FailedSessionsTruncated {
		t.Fatalf("body %s", raw)
	}
	got := scrape(t, url)
	for _, want := range []string{
		`tsession_collector_ingest_errors_total{cause="storage"} 1`,
		`tsession_collector_ingest_failed_sessions_total{cause="storage"} 2`,
		`tsession_collector_ingest_partial_failures_total 0`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics lack %q:\n%s", want, got)
		}
	}
}

// One failing session does not cost the others their events: they are stored,
// the status is still an error (an exporter that does not know the field
// resends everything, and the stored events are then duplicates), and the
// body names exactly the session that failed.
func TestIngestPartialFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	st := newPartStore("bad")
	c, url := newPartC(t, cfg, st)
	batch := []core.Event{sev("a", 1), sev("bad", 2), sev("b", 3), sev("bad", 4), sev("a", 5)}
	code, raw, b := postRaw(t, url, batch...)
	if code != http.StatusInternalServerError || b.Error != "storage error" {
		t.Fatalf("status %d body %s", code, raw)
	}
	if b.Accepted != 3 || len(b.FailedSessions) != 1 || b.FailedSessions["bad"] != "storage" || b.FailedSessionsTruncated {
		t.Fatalf("body %s", raw)
	}
	if got := fmt.Sprint(storedOf(t, c, "a"), storedOf(t, c, "b")); got != "[1 5] [3]" {
		t.Fatalf("healthy sessions stored %s", got)
	}
	// The whole batch again, as an exporter that ignores the field sends it.
	code, raw, b = postRaw(t, url, batch...)
	if code != http.StatusInternalServerError || b.Accepted != 0 || b.Duplicates != 3 || len(b.FailedSessions) != 1 {
		t.Fatalf("resend: status %d body %s", code, raw)
	}
	// The session recovers: the resend is accepted for it and a duplicate for
	// the others.
	st.set("bad", false)
	code, raw, b = postRaw(t, url, batch...)
	if code != 200 || b.Accepted != 2 || b.Duplicates != 3 || len(b.FailedSessions) != 0 {
		t.Fatalf("after recovery: status %d body %s", code, raw)
	}
	if got := fmt.Sprint(storedOf(t, c, "a"), storedOf(t, c, "b"), storedOf(t, c, "bad")); got != "[1 5] [3] [2 4]" {
		t.Fatalf("stored %s", got)
	}
	got := scrape(t, url)
	for _, want := range []string{
		`tsession_collector_ingest_partial_failures_total 2`,
		`tsession_collector_ingest_failed_sessions_total{cause="storage"} 2`,
		`tsession_collector_ingest_errors_total{cause="storage"} 2`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics lack %q:\n%s", want, got)
		}
	}
}

// With a zero window a session can fail after its events were buffered: its
// actor is loaded and the write fails. It is named, and none of its events
// are counted: the counts of a failed request are those of the sessions that
// did not fail, so a request in which every session failed never looks as if
// something was taken. The buffered events are written once the store is
// back, and a resend is a duplicate.
func TestIngestFailedSessionCountsNothing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	st := newPartStore()
	c, url := newPartC(t, cfg, st)
	if code, raw, _ := postRaw(t, url, sev("bad", 1)); code != 200 {
		t.Fatalf("status %d body %s", code, raw)
	}
	st.set("bad", true)
	code, raw, b := postRaw(t, url, sev("bad", 2), sev("bad", 3))
	if code != http.StatusInternalServerError || b.Accepted != 0 || b.Duplicates != 0 || b.FailedSessions["bad"] != "storage" {
		t.Fatalf("only the failing session: status %d body %s", code, raw)
	}
	code, raw, b = postRaw(t, url, sev("bad", 2), sev("bad", 3), sev("a", 4))
	if code != http.StatusInternalServerError || b.Accepted != 1 || b.Duplicates != 0 || len(b.FailedSessions) != 1 {
		t.Fatalf("with a healthy session: status %d body %s", code, raw)
	}
	st.set("bad", false)
	code, raw, b = postRaw(t, url, sev("bad", 2), sev("bad", 3), sev("a", 4))
	if code != 200 || b.Accepted+b.Duplicates != 3 || len(b.FailedSessions) != 0 {
		t.Fatalf("after recovery: status %d body %s", code, raw)
	}
	if got := fmt.Sprint(storedOf(t, c, "a"), storedOf(t, c, "bad")); got != "[4] [1 2 3]" {
		t.Fatalf("stored %s", got)
	}
}

// A session whose buffer is full answers 503 and is named with its cause; the
// other session of the request is accepted.
func TestIngestPartialBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = time.Hour // nothing leaves the buffer
	cfg.MaxBufferedEvents = 2
	_, url := newPartC(t, cfg, newPartStore())
	if code, raw, _ := postRaw(t, url, sev("full", 1), sev("full", 2)); code != 200 {
		t.Fatalf("filling the buffer: %d %s", code, raw)
	}
	code, raw, b := postRaw(t, url, sev("full", 3), sev("ok", 4))
	if code != http.StatusServiceUnavailable || b.Error != "session buffer full, retry later" {
		t.Fatalf("status %d body %s", code, raw)
	}
	if b.Accepted != 1 || len(b.FailedSessions) != 1 || b.FailedSessions["full"] != "backpressure" {
		t.Fatalf("body %s", raw)
	}
	if got := scrape(t, url); !strings.Contains(got, `tsession_collector_ingest_failed_sessions_total{cause="backpressure"} 1`) {
		t.Errorf("backpressure not counted per session:\n%s", got)
	}
}

// The status of a request with several failed sessions is that of the first
// one in id order, as before; each session carries its own cause.
func TestIngestPartialFailureMixedCauses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = time.Hour
	cfg.MaxBufferedEvents = 1
	_, url := newPartC(t, cfg, newPartStore("z-storage"))
	if code, raw, _ := postRaw(t, url, sev("a-full", 1)); code != 200 {
		t.Fatalf("filling the buffer: %d %s", code, raw)
	}
	code, raw, b := postRaw(t, url, sev("a-full", 2), sev("m-ok", 3), sev("z-storage", 4))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %s", code, raw)
	}
	if b.Accepted != 1 || fmt.Sprint(b.FailedSessions) != "map[a-full:backpressure z-storage:storage]" {
		t.Fatalf("body %s", raw)
	}
}

// The list of failed sessions is capped, and says so when it is, and no
// response carries anything of the events but their session ids.
func TestIngestFailedSessionsCappedAndWithoutEventData(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	st := newPartStore()
	var batch []core.Event
	longID := strings.Repeat("x", 100)
	for i := 0; i < maxFailedSessions+8; i++ {
		id := fmt.Sprintf("%s-%03d", longID, i)
		st.set(id, true)
		batch = append(batch, sev(id, uint64(i+1)))
	}
	batch = append(batch, sev("healthy", 1000))
	c, url := newPartC(t, cfg, st)
	code, raw, b := postRaw(t, url, batch...)
	if code != http.StatusInternalServerError {
		t.Fatalf("status %d body %s", code, raw)
	}
	if len(b.FailedSessions) != maxFailedSessions || !b.FailedSessionsTruncated || b.Accepted != 1 {
		t.Fatalf("%d sessions listed, truncated=%v, accepted=%d", len(b.FailedSessions), b.FailedSessionsTruncated, b.Accepted)
	}
	if max := maxFailedSessions*(128+16) + 256; len(raw) > max {
		t.Fatalf("the response is %d bytes long, want at most %d", len(raw), max)
	}
	if got := storedOf(t, c, "healthy"); len(got) != 1 {
		t.Fatalf("the healthy session stored %v", got)
	}
	for _, secret := range []string{"SECRET", "span-", "svc", "secret-attr"} {
		if strings.Contains(raw, secret) {
			t.Fatalf("the response echoes event data (%q): %s", secret, raw)
		}
	}
	// The same holds for an uncapped partial failure and for a success.
	st2 := newPartStore("bad")
	_, url2 := newPartC(t, cfg, st2)
	for _, evs := range [][]core.Event{{sev("bad", 1), sev("ok", 2)}, {sev("ok", 3)}} {
		_, raw, _ := postRaw(t, url2, evs...)
		for _, secret := range []string{"SECRET", "span-", "svc", "secret-attr"} {
			if strings.Contains(raw, secret) {
				t.Fatalf("the response echoes event data (%q): %s", secret, raw)
			}
		}
	}
}

// A poison event is not reported as a failed session: with a zero window the
// collector isolates and quarantines it inside the request and answers 200,
// with a reorder window it answers 200 at once and quarantines it later.
// Only a store that fails the session's reads as well makes the session fail.
func TestIngestPoisonEventIsNotAFailedSession(t *testing.T) {
	sp := &spyStore{poison: func(e core.Event) bool { return e.SessionID == "p" && e.LocalSeq == 2 }}
	cfg := DefaultConfig()
	cfg.Window = 0
	sp.Store = store.NewMem()
	c, url := newPartC(t, cfg, sp)
	code, raw, b := postRaw(t, url, sev("p", 1), sev("p", 2), sev("p", 3), sev("q", 4))
	if code != 200 || b.Accepted != 4 || len(b.FailedSessions) != 0 {
		t.Fatalf("status %d body %s", code, raw)
	}
	if st, _ := c.seq.Status(context.Background(), "p"); st.Quarantined != 1 || st.Pending != 0 || st.EventCount != 2 {
		t.Fatalf("session p: %+v", st)
	}
	if got := storedOf(t, c, "q"); len(got) != 1 {
		t.Fatalf("session q stored %v", got)
	}
}

// An ingest that races with shutdown names its sessions with the cause
// "shutdown" and still answers 503.
func TestIngestShutdownNamesSessions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 0
	c := New(store.NewMem(), cfg)
	url := newLoopbackServer(t, c.Handler())
	c.Close()
	code, raw, b := postRaw(t, url, sev("a", 1), sev("b", 2))
	if code != http.StatusServiceUnavailable || b.Error != "collector is shutting down, retry later" {
		t.Fatalf("status %d body %s", code, raw)
	}
	if fmt.Sprint(b.FailedSessions) != "map[a:shutdown b:shutdown]" {
		t.Fatalf("body %s", raw)
	}
}
