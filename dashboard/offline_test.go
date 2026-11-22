package dashboard

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func writeExport(t *testing.T, name string, evs ...core.Event) string {
	t.Helper()
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for i := range evs {
		if err := enc.Encode(&evs[i]); err != nil {
			panic(err)
		}
	}
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		panic(err)
	}
	return p
}

func TestOfflineServesAnExportReadOnly(t *testing.T) {
	no := false
	// Written out of order, as a concatenation of pages could be.
	file := writeExport(t, "checkout-failed.ndjson",
		core.Event{SessionID: "ts_run1", Seq: 2, SpanID: "b", ParentID: "a", Service: "api", Kind: core.KindDBWrite, Name: "INSERT INTO orders",
			StartNs: 2e9, EndNs: 3e9, TxID: "tx1",
			Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", PK: map[string]any{"id": 7}, Effective: &no}}},
		core.Event{SessionID: "ts_run1", Seq: 1, SpanID: "a", Service: "api", Kind: core.KindAPIIn, Name: "POST /orders", StartNs: 1e9, EndNs: 4e9, Err: "boom"},
		// The collector decided this commit failed; a naive re-resolution would call the insert effective.
		core.Event{SessionID: "ts_run1", Seq: 3, SpanID: "c", ParentID: "a", Service: "api", Kind: core.KindDBTxCommit, TxID: "tx1",
			StartNs: 3e9, EndNs: 3e9, Attrs: map[string]string{"commit_failed": "true"}},
	)
	h, err := New(Config{Open: []string{file}})
	if err != nil {
		panic(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	_, b := get(t, srv.URL, "/healthz")
	if !strings.Contains(string(b), `"offline":true`) || !strings.Contains(string(b), `"demo":false`) {
		panic(fmt.Sprintf("healthz: %s", b))
	}
	var list []Session
	_, b = get(t, srv.URL, "/api/cp/v1/sessions")
	if err := json.Unmarshal(b, &list); err != nil || len(list) != 1 {
		panic(fmt.Sprintf("sessions: %s %v", b, err))
	}
	if s := list[0]; s.ID != "ts_run1" || s.Name != "checkout-failed" || s.State != "SEALED" || s.InitiatedBy != "file" {
		panic(fmt.Sprintf("session: %+v", s))
	}

	var page struct {
		Events []core.Event `json:"events"`
		Sealed bool         `json:"sealed"`
	}
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/events")
	if err := json.Unmarshal(b, &page); err != nil || len(page.Events) != 3 || !page.Sealed {
		panic(fmt.Sprintf("events: %s %v", b, err))
	}
	for i, e := range page.Events {
		if e.Seq != uint64(i+1) {
			panic(fmt.Sprintf("events not in seq order: %d at %d", e.Seq, i))
		}
	}
	if eff := page.Events[1].Mutations[0].Effective; eff == nil || *eff {
		panic(fmt.Sprintf("the exported transaction outcome must be kept as recorded, got %v", eff))
	}
	var tr struct {
		Roots []*Node `json:"roots"`
	}
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/trace")
	if err := json.Unmarshal(b, &tr); err != nil || len(tr.Roots) != 1 || len(tr.Roots[0].Children) != 2 {
		panic(fmt.Sprintf("trace: %s %v", b, err))
	}
	var sum map[string]any
	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run1/summary")
	if err := json.Unmarshal(b, &sum); err != nil || sum["event_count"] != float64(3) {
		panic(fmt.Sprintf("summary: %s %v", b, err))
	}

	// Nothing can be changed.
	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/api/cp/v1/sessions"},
		{http.MethodPost, "/api/cp/v1/sessions/ts_run1/stop"},
		{http.MethodPatch, "/api/cp/v1/sessions/ts_run1"},
		{http.MethodDelete, "/api/cp/v1/sessions/ts_run1"},
	} {
		req, _ := http.NewRequest(c.method, srv.URL+c.path, strings.NewReader(`{"name":"x"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", srv.URL)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			panic(fmt.Sprintf("%s %s: status %d, want 405", c.method, c.path, resp.StatusCode))
		}
	}
	if _, b := get(t, srv.URL, "/api/cp/v1/sessions"); !strings.Contains(string(b), "ts_run1") {
		panic(fmt.Sprintf("session gone after refused changes: %s", b))
	}
}

func TestOfflineRejectsBadFiles(t *testing.T) {
	ev := core.Event{SessionID: "ts_a", Seq: 1, Kind: core.KindAPIIn}
	other := core.Event{SessionID: "ts_b", Seq: 2, Kind: core.KindAPIIn}
	good := writeExport(t, "a.ndjson", ev)
	notJSON := filepath.Join(t.TempDir(), "x.ndjson")
	os.WriteFile(notJSON, []byte("hello\n"), 0o600)
	for name, cfg := range map[string]Config{
		"missing file":       {Open: []string{filepath.Join(t.TempDir(), "nope.ndjson")}},
		"not json":           {Open: []string{notJSON}},
		"empty":              {Open: []string{writeExport(t, "empty.ndjson")}},
		"not an event":       {Open: []string{writeExport(t, "noseq.ndjson", core.Event{Name: "x"})}},
		"two sessions":       {Open: []string{writeExport(t, "mixed.ndjson", ev, other)}},
		"same session twice": {Open: []string{good, writeExport(t, "again.ndjson", ev)}},
		"with demo":          {Open: []string{good}, Demo: true},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := New(Config{Open: []string{good, writeExport(t, "b.ndjson", other)}}); err != nil {
		panic(fmt.Sprintf("two different sessions: %v", err))
	}
}

func TestOfflineToleratesATornLastLineOnly(t *testing.T) {
	a := core.Event{SessionID: "ts_t", Seq: 1, Kind: core.KindAPIIn, StartNs: 5e9, EndNs: 6e9}
	dropped := core.Event{SessionID: "ts_t", Seq: 2, Kind: core.KindSDKDropped, EndNs: 7e9, Attrs: map[string]string{"count": "-5"}}
	good := writeExport(t, "cut.ndjson", a, dropped)
	b, _ := os.ReadFile(good)
	if err := os.WriteFile(good, append(b, []byte(`{"session_id":"ts_t","seq":3,"kind":"db.re`)...), 0o600); err != nil {
		panic(err)
	}
	d, err := NewOffline([]string{good})
	if err != nil {
		panic(fmt.Sprintf("an export cut mid-line must still open: %v", err))
	}
	// Who must not judge a part of a recording is told that it is one.
	if fs := d.Files(); len(fs) != 1 || fs[0].Path != good || fs[0].SessionID != "ts_t" || fs[0].Truncated == nil ||
		!strings.Contains(fs[0].Truncated.Error(), "cut.ndjson:3: not an exported event") {
		panic(fmt.Sprintf("the files of a cut export: %+v", fs))
	}
	whole := writeExport(t, "whole.ndjson", core.Event{SessionID: "ts_w", Seq: 1, Kind: core.KindAPIIn})
	if d2, err := NewOffline([]string{whole, good}); err != nil {
		panic(err)
	} else if fs := d2.Files(); len(fs) != 2 || fs[0].Path != whole || fs[0].SessionID != "ts_w" || fs[0].Truncated != nil || fs[1].SessionID != "ts_t" {
		panic(fmt.Sprintf("the files, in the order given: %+v", fs))
	}
	if fs := NewDemo().Files(); fs != nil {
		panic(fmt.Sprintf("the demo has no files: %+v", fs))
	}
	s := d.sess[0]
	if len(s.events) != 2 || !s.Incomplete || s.DroppedEvents != 0 || !s.StartedAt.Equal(time.Unix(5, 0)) {
		panic(fmt.Sprintf("session from a cut file: events=%d incomplete=%v dropped=%d started=%s", len(s.events), s.Incomplete, s.DroppedEvents, s.StartedAt))
	}
	// A bad line in the middle is corruption, not an interrupted write.
	if err := os.WriteFile(good, append([]byte("{not json\n"), b...), 0o600); err != nil {
		panic(err)
	}
	if _, err := NewOffline([]string{good}); err == nil {
		panic("a malformed line before the end must be rejected")
	}
}

func TestOfflineRejectsDuplicateAndOutOfRangeSeq(t *testing.T) {
	e := core.Event{SessionID: "ts_d", Seq: 5, Kind: core.KindAPIIn}
	if _, err := NewOffline([]string{writeExport(t, "dup.ndjson", e, e)}); err == nil || !strings.Contains(err.Error(), "already used on line 1") {
		panic(fmt.Sprintf("duplicate seq: %v", err))
	}
	big := e
	big.Seq = 1 << 63
	if _, err := NewOffline([]string{writeExport(t, "big.ndjson", big)}); err == nil || !strings.Contains(err.Error(), "out of range") {
		panic(fmt.Sprintf("seq beyond int64: %v", err))
	}
}

// A recording shorter than a second keeps its length: whole-second
// timestamps showed its duration as zero.
func TestOfflineKeepsSubSecondTimes(t *testing.T) {
	a := core.Event{SessionID: "ts_ms", Seq: 1, Kind: core.KindAPIIn, StartNs: 5e9 + 10e6, EndNs: 5e9 + 132e6}
	d, err := NewOffline([]string{writeExport(t, "short.ndjson", a)})
	if err != nil {
		panic(err)
	}
	s := d.sess[0]
	if s.EndedAt.Sub(s.StartedAt) != 122*time.Millisecond {
		panic(fmt.Sprintf("started %s ended %s, want 122ms apart", s.StartedAt, s.EndedAt))
	}
	// And on the wire, where the control plane writes its times the same way.
	b, _ := json.Marshal(s.Session)
	if !strings.Contains(string(b), `"started_at":"1970-01-01T00:00:05.01Z"`) || !strings.Contains(string(b), `"ended_at":"1970-01-01T00:00:05.132Z"`) {
		panic(fmt.Sprintf("session: %s", b))
	}
}

// The demo and offline backends answer the entities endpoints themselves:
// they carry the same optional uncertainty fields as the collector, and an
// export written before events had such flags reads as nothing uncertain.
func TestOfflineEntitiesCarryUncertainty(t *testing.T) {
	stock := []core.Mutation{{Store: "postgres", Entity: "stock", Op: "update", PK: map[string]any{"sku": "w"}}}
	file := writeExport(t, "stock.ndjson",
		core.Event{SessionID: "ts_run2", Seq: 1, SpanID: "a", Service: "api", Kind: core.KindDBWrite, StartNs: 1e9, EndNs: 2e9,
			Flags: []string{"pk_not_unique", "after_unavailable"}, Mutations: stock},
		core.Event{SessionID: "ts_run2", Seq: 2, SpanID: "b", Service: "api", Kind: core.KindDBWrite, StartNs: 2e9, EndNs: 3e9, Mutations: stock},
		core.Event{SessionID: "ts_run2", Seq: 3, SpanID: "c", Service: "api", Kind: core.KindDBWrite, StartNs: 3e9, EndNs: 4e9,
			Flags: []string{"redacted"}, Mutations: []core.Mutation{{Store: "postgres", Entity: "orders", Op: "insert", PK: map[string]any{"id": 7}}}},
	)
	h, err := New(Config{Open: []string{file}})
	if err != nil {
		panic(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	_, b := get(t, srv.URL, "/api/col/v1/sessions/ts_run2/entities")
	var ents []map[string]any
	if err := json.Unmarshal(b, &ents); err != nil || len(ents) != 2 {
		panic(fmt.Sprintf("entities: %s %v", b, err))
	}
	// Sorted by store, entity and key, as the collector lists them.
	if ents[1]["entity"] != "stock" || ents[1]["mutation_count"] != 2.0 || ents[1]["uncertain"] != 1.0 {
		panic(fmt.Sprintf("stock: %s", b))
	}
	if _, has := ents[0]["uncertain"]; has {
		panic(fmt.Sprintf("an entity with nothing uncertain must not carry the field: %s", b))
	}

	_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_run2/entities/postgres/stock/%7B%22sku%22%3A%22w%22%7D")
	var doc struct {
		PK        string           `json:"pk"`
		Mutations []map[string]any `json:"mutations"`
	}
	if err := json.Unmarshal(b, &doc); err != nil || len(doc.Mutations) != 2 || doc.PK != `{"sku":"w"}` {
		panic(fmt.Sprintf("history: %s %v", b, err))
	}
	hist := doc.Mutations
	if f, _ := hist[0]["flags"].([]any); len(f) != 2 || f[0] != "pk_not_unique" || f[1] != "after_unavailable" {
		panic(fmt.Sprintf("flags of the first change: %s", b))
	}
	if _, has := hist[1]["flags"]; has {
		panic(fmt.Sprintf("a change without flags must not carry the field: %s", b))
	}
}

// An export carries the session's verdict on its first line (the control
// plane's: events that never reached the collector show in no event). The
// opened session has it, so what reads the file gives the verdict it gives
// the live session. The review's F4: `report --fail-on-incomplete` exited 3
// on a session and 0 on its export.
func TestOfflineKeepsTheVerdictOfTheExport(t *testing.T) {
	evs := []core.Event{
		{SessionID: "ts_v", Seq: 1, SpanID: "a", Service: "inventory", Kind: core.KindAPIIn, Name: "POST /reserve", StartNs: 1e9, EndNs: 2e9},
		{SessionID: "ts_v", Seq: 2, SpanID: "b", ParentID: "a", Service: "inventory", Kind: core.KindCacheRead, Name: "GET stock", StartNs: 1e9, EndNs: 2e9},
	}
	const reason = "service orders-api (instance o-1): 0 of 7 recorded events reached the collector, 7 dropped by the SDK"
	const warn = "service batch (instance b-1): the collector has 5 events, more than the 3 of the instance's last report"
	write := func(name string, meta *core.ExportSession, metaOn int) string {
		t.Helper()
		var b strings.Builder
		for i := range evs {
			m := meta
			if i != metaOn {
				m = nil
			}
			line, err := core.ExportLine(&evs[i], m)
			if err != nil {
				panic(err)
			}
			b.Write(line)
			b.WriteByte('\n')
		}
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
			panic(err)
		}
		return p
	}
	open := func(file string) Session {
		t.Helper()
		h, err := New(Config{Open: []string{file}})
		if err != nil {
			panic(err)
		}
		srv := httptest.NewServer(h)
		defer srv.Close()
		var s Session
		_, b := get(t, srv.URL, "/api/cp/v1/sessions/ts_v")
		if err := json.Unmarshal(b, &s); err != nil {
			panic(fmt.Sprintf("%s %v", b, err))
		}
		// The collector's own view of the events is not changed by it.
		_, b = get(t, srv.URL, "/api/col/v1/sessions/ts_v/summary")
		if !strings.Contains(string(b), `"incomplete":false`) || !strings.Contains(string(b), `"event_count":2`) {
			panic(fmt.Sprintf("summary: %s", b))
		}
		return s
	}

	meta := &core.ExportSession{State: "SEALED", Incomplete: true, IncompleteReasons: []string{reason}, DeliveryUnconfirmed: []string{warn},
		DrainTimedOut: true, MaxDrainSeconds: 60}
	s := open(write("verdict.ndjson", meta, 0))
	if !s.Incomplete || len(s.IncompleteReasons) != 1 || s.IncompleteReasons[0] != reason || len(s.DeliveryUnconfirmed) != 1 || s.DeliveryUnconfirmed[0] != warn ||
		!s.DrainTimedOut || s.MaxDrainSeconds != 60 || s.State != "SEALED" || s.Name != "verdict" {
		panic(fmt.Sprintf("session opened from an export with a verdict: %+v", s))
	}
	// The verdict is on the first line. One on another line is not the export's: the file is refused,
	// where it was read as if it were (and a second, contradicting one was passed over).
	if _, err := New(Config{Open: []string{write("late.ndjson", meta, 1)}}); err == nil || !strings.Contains(err.Error(), "late.ndjson:2: a second tsession_session") {
		panic(fmt.Sprintf("a verdict on the second line: %v", err))
	}
	// A session that was complete says so, and stays complete.
	if s := open(write("fine.ndjson", &core.ExportSession{State: "SEALED"}, 0)); s.Incomplete || len(s.IncompleteReasons)+len(s.DeliveryUnconfirmed) != 0 || s.DrainTimedOut {
		panic(fmt.Sprintf("a complete session: %+v", s))
	}
	// A seal that failed.
	if s := open(write("seal.ndjson", &core.ExportSession{State: "SEALED", Incomplete: true, SealError: "collector unavailable"}, 0)); !s.Incomplete || s.SealError != "collector unavailable" {
		panic(fmt.Sprintf("a failed seal: %+v", s))
	}
	// An export from before the verdict was carried: judged on its events.
	if s := open(writeExport(t, "old.ndjson", evs...)); s.Incomplete || len(s.IncompleteReasons) != 0 {
		panic(fmt.Sprintf("an export without a verdict: %+v", s))
	}
}

// The third review's F3: a session without events is the one the delivery
// verdict exists for (its only service could not deliver anything), and its
// export was an empty file that could not be opened. It is one line with the
// session's id and its verdict now, and opens as a session without events
// that says why.
func TestOfflineOpensASessionWithoutEvents(t *testing.T) {
	const reason = "service orders-api (instance o-1): 0 of 6 recorded events reached the collector (last error: connection refused)"
	started := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	line, err := core.ExportHeader("ts_none", &core.ExportSession{State: "SEALED", Incomplete: true, IncompleteReasons: []string{reason},
		StartedAt: started, EndedAt: started.Add(12 * time.Second)})
	if err != nil {
		panic(err)
	}
	file := filepath.Join(t.TempDir(), "nothing arrived.ndjson")
	if err := os.WriteFile(file, append(line, '\n'), 0o600); err != nil {
		panic(err)
	}
	h, err := New(Config{Open: []string{file}})
	if err != nil {
		panic(fmt.Sprintf("an export of a session without events does not open: %v", err))
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	var s Session
	_, b := get(t, srv.URL, "/api/cp/v1/sessions/ts_none")
	if err := json.Unmarshal(b, &s); err != nil || !s.Incomplete || len(s.IncompleteReasons) != 1 || s.IncompleteReasons[0] != reason ||
		s.State != "SEALED" || s.Name != "nothing arrived" || !s.StartedAt.Equal(started) || !s.EndedAt.Equal(started.Add(12*time.Second)) {
		panic(fmt.Sprintf("the session: %s %v", b, err))
	}
	// Every read of the collector answers, with nothing in it.
	for path, want := range map[string]string{
		"/summary": `"event_count":0`, "/events": `[]`, "/trace": `"session_id":"ts_none"`, "/entities": `[]`,
	} {
		code, b := get(t, srv.URL, "/api/col/v1/sessions/ts_none"+path)
		if code.StatusCode != http.StatusOK || !strings.Contains(string(b), want) {
			panic(fmt.Sprintf("GET %s: %d %s", path, code.StatusCode, b))
		}
	}
	if code, b := get(t, srv.URL, "/api/cp/v1/sessions"); code.StatusCode != http.StatusOK || !strings.Contains(string(b), `"ts_none"`) {
		panic(fmt.Sprintf("the list: %d %s", code.StatusCode, b))
	}

	// What does not open, and what it says. An empty file: nothing says which
	// session it was.
	bad := func(name, content, want string) {
		t.Helper()
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			panic(err)
		}
		if _, err := New(Config{Open: []string{p}}); err == nil || !strings.Contains(err.Error(), want) {
			panic(fmt.Sprintf("%s: %v, want an error that says %q", name, err, want))
		}
	}
	bad("empty.ndjson", "", "no events, and no session")
	bad("torn.ndjson", string(line[:len(line)-4]), "not an exported event")
	other, _ := core.ExportHeader("ts_other", &core.ExportSession{State: "SEALED"})
	bad("two.ndjson", string(line)+"\n"+string(other)+"\n", "two sessions (ts_none and ts_other) in one file")
	ev, _ := core.ExportLine(&core.Event{SessionID: "ts_other", Seq: 1, Kind: core.KindAPIIn}, nil)
	bad("mixed.ndjson", string(line)+"\n"+string(ev)+"\n", "events of two sessions (ts_none and ts_other) in one file")
	// A verdict's line followed by events of the same session is read like
	// an export whose first event carries it.
	ev, _ = core.ExportLine(&core.Event{SessionID: "ts_none", Seq: 1, Kind: core.KindAPIIn, StartNs: 1e9, EndNs: 2e9}, nil)
	p := filepath.Join(t.TempDir(), "both.ndjson")
	if err := os.WriteFile(p, []byte(string(line)+"\n"+string(ev)+"\n"), 0o600); err != nil {
		panic(err)
	}
	if _, err := New(Config{Open: []string{p}}); err != nil {
		panic(err)
	}
}

// An export is held in memory, several times its size, and a small gzip file
// can hold any amount: no more than maxExportBytes are read of one.
func TestOfflineRefusesAFileThatIsTooLarge(t *testing.T) {
	old := maxExportBytes
	maxExportBytes = 4 << 10
	defer func() { maxExportBytes = old }()
	var b strings.Builder
	for seq := 1; b.Len() < 16<<10; seq++ {
		fmt.Fprintf(&b, `{"session_id":"ts_big","seq":%d,"service":"api","kind":"api.in","name":"GET /x","start_ns":1,"end_ns":2}`+"\n", seq)
	}
	p := filepath.Join(t.TempDir(), "big.ndjson")
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		panic(err)
	}
	if _, err := NewOffline([]string{p}); err == nil || !strings.Contains(err.Error(), "too large to open") {
		panic(fmt.Sprintf("a file over the limit: %v", err))
	}
	// As gzip, where the file itself is small.
	var z bytes.Buffer
	zw := gzip.NewWriter(&z)
	_, _ = zw.Write([]byte(b.String()))
	_ = zw.Close()
	if z.Len() >= 4<<10 {
		panic(fmt.Sprintf("the gzip file is %d bytes: not smaller than the limit, so the test proves nothing", z.Len()))
	}
	pz := filepath.Join(t.TempDir(), "big.ndjson.gz")
	if err := os.WriteFile(pz, z.Bytes(), 0o600); err != nil {
		panic(err)
	}
	if _, err := NewOffline([]string{pz}); err == nil || !strings.Contains(err.Error(), "too large to open") {
		panic(fmt.Sprintf("a gzip file that expands over the limit: %v", err))
	}
}

// The size limit is on what is read: a file of exactly the limit opens, one
// byte more does not.
func TestOfflineSizeLimitIsExact(t *testing.T) {
	line := `{"session_id":"ts_big","seq":1,"service":"api","kind":"api.in","name":"GET /x","start_ns":1,"end_ns":2}` + "\n"
	old := maxExportBytes
	defer func() { maxExportBytes = old }()
	p := filepath.Join(t.TempDir(), "one.ndjson")
	if err := os.WriteFile(p, []byte(line), 0o600); err != nil {
		panic(err)
	}
	maxExportBytes = int64(len(line))
	if _, err := NewOffline([]string{p}); err != nil {
		panic(fmt.Sprintf("a file of exactly the limit: %v", err))
	}
	maxExportBytes = int64(len(line)) - 1
	if _, err := NewOffline([]string{p}); err == nil || !strings.Contains(err.Error(), "too large to open") {
		panic(fmt.Sprintf("a file one byte over the limit: %v", err))
	}
}

// What makes an opened session incomplete, each on its own and with the flag
// next to it saying complete: reasons, a seal that failed, a drain that ran
// out of time.
func TestOfflineIncompleteWhateverTheFlagSays(t *testing.T) {
	n := 1
	e := core.Event{SessionID: "ts_i", Seq: 1, Service: "api", Kind: core.KindAPIIn, Name: "GET /", StartNs: 1e9, EndNs: 2e9}
	for name, meta := range map[string]core.ExportSession{
		"reasons":   {State: "SEALED", Events: &n, IncompleteReasons: []string{"lost"}},
		"seal":      {State: "SEALED", Events: &n, SealError: "timeout"},
		"drain":     {State: "SEALED", Events: &n, DrainTimedOut: true, MaxDrainSeconds: 60},
		"the flag":  {State: "SEALED", Events: &n, Incomplete: true},
		"none (ok)": {State: "SEALED", Events: &n},
	} {
		line, err := core.ExportLine(&e, &meta)
		if err != nil {
			panic(err)
		}
		p := filepath.Join(t.TempDir(), "f.ndjson")
		if err := os.WriteFile(p, append(line, '\n'), 0o600); err != nil {
			panic(err)
		}
		d, err := NewOffline([]string{p})
		if err != nil {
			panic(err)
		}
		if got, want := d.find("ts_i").Incomplete, name != "none (ok)"; got != want {
			t.Errorf("%s: incomplete = %v, want %v", name, got, want)
		}
	}
}
