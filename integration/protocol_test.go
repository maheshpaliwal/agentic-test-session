package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// TestProtocol_FailedCommitAndRetryPending posts synthetic events to a real
// collector's /v1/ingest and checks the PROTOCOL rules: a failed commit (Mongo
// style, Err or commit_failed) leaves the tx's mutations not effective, and a
// worker.end with retry_pending does not close the pending-async ledger.
func TestProtocol_FailedCommitAndRetryPending(t *testing.T) {
	in := newInfra(t, store.NewMem())
	col := colAPI{in.colSrv.URL}
	s := in.start(t, "header")

	var lseq uint64
	var hlc uint64 = 1 << 40
	mk := func(kind core.Kind, name, span string, f func(*core.Event)) core.Event {
		lseq++
		hlc += 10
		e := core.Event{SessionID: s.ID, TraceID: "00000000000000000000000000000001", SpanID: span, Service: "synthetic",
			Instance: "synthetic-1", Kind: kind, Name: name, HLC: hlc, EndHLC: hlc + 1, LocalSeq: lseq,
			StartNs: time.Now().UnixNano(), EndNs: time.Now().UnixNano()}
		if f != nil {
			f(&e)
		}
		return e
	}
	write := func(span, tx, pk string) core.Event {
		return mk(core.KindDBWrite, "insertOne carts", span, func(e *core.Event) {
			e.TxID = tx
			e.Mutations = []core.Mutation{{Store: "mongo", Entity: "carts", PK: map[string]any{"_id": pk}, Op: "insert", Source: "sdk"}}
		})
	}
	post := func(evs ...core.Event) {
		t.Helper()
		var buf bytes.Buffer
		for _, e := range evs {
			b, _ := json.Marshal(e)
			buf.Write(append(b, '\n'))
		}
		req, _ := http.NewRequest("POST", in.colSrv.URL+"/v1/ingest", &buf)
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("X-TSession-Instance", "synthetic-1")
		resp, err := plain.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var r collector.IngestResult
		_ = json.NewDecoder(resp.Body).Decode(&r)
		if resp.StatusCode != 200 || r.Accepted != len(evs) || r.Rejected != 0 {
			t.Fatalf("ingest: %d %+v (want %d accepted)", resp.StatusCode, r, len(evs))
		}
	}
	effective := func(pk string) *bool {
		t.Helper()
		h := col.history(t, s.ID, "mongo", "carts", url.PathEscape(`{"_id":"`+pk+`"}`))
		if len(h.Mutations) != 1 {
			t.Fatalf("history %s: %+v", pk, h)
		}
		return h.Mutations[0].Effective
	}

	post(
		write("a1", "tx-ok", "ok"), mk(core.KindDBTxCommit, "commitTransaction", "a2", func(e *core.Event) { e.TxID = "tx-ok" }),
		write("b1", "tx-err", "err"), mk(core.KindDBTxCommit, "commitTransaction", "b2", func(e *core.Event) { e.TxID = "tx-err"; e.Err = "WriteConflict" }),
		write("c1", "tx-attr", "attr"), mk(core.KindDBTxCommit, "commitTransaction", "c2", func(e *core.Event) {
			e.TxID = "tx-attr"
			e.Attrs = map[string]string{"commit_failed": "true"}
		}),
		write("d1", "tx-rb", "rb"), mk(core.KindDBTxRollbck, "abortTransaction", "d2", func(e *core.Event) { e.TxID = "tx-rb" }),
		write("e1", "tx-open", "open"), // no outcome yet
		// two async jobs: P1 retries then finishes, P2 only ever has a retry_pending end
		mk(core.KindQueuePub, "job1", "P1", nil), mk(core.KindQueuePub, "job2", "P2", nil),
		mk(core.KindWorkerStart, "job1", "w1", func(e *core.Event) { e.LinkIDs = []string{"P1"}; e.Attempt = 1 }),
		mk(core.KindWorkerEnd, "job1", "w1e", func(e *core.Event) {
			e.LinkIDs = []string{"P1"}
			e.Attempt = 1
			e.Attrs = map[string]string{"retry_pending": "true"}
		}),
		mk(core.KindWorkerRetry, "job1", "w1r", func(e *core.Event) { e.LinkIDs = []string{"P1"}; e.Attempt = 1 }),
		mk(core.KindWorkerEnd, "job2", "w2e", func(e *core.Event) {
			e.LinkIDs = []string{"P2"}
			e.Attempt = 1
			e.Attrs = map[string]string{"retry_pending": "true"}
		}),
	)

	waitFor(t, 5*time.Second, "events visible", func() bool { return col.summary(t, s.ID).EventCount >= 14 })
	for pk, want := range map[string]bool{"ok": true, "err": false, "attr": false, "rb": false} {
		if got := effective(pk); got == nil || *got != want {
			t.Errorf("tx %s: effective=%v want %v", pk, got, want)
		}
	}
	if got := effective("open"); got != nil && !*got {
		t.Errorf("tx without outcome: effective=%v (must not be false before seal)", *got)
	}
	if sm := col.summary(t, s.ID); sm.PendingAsync != 2 {
		t.Fatalf("pending_async=%d ids=%v, want 2: retry_pending worker.end must not close the ledger", sm.PendingAsync, sm.PendingIDs)
	}

	// final outcome for P1 closes it; P2 stays open
	post(mk(core.KindWorkerEnd, "job1", "w1e2", func(e *core.Event) { e.LinkIDs = []string{"P1"}; e.Attempt = 2 }))
	waitFor(t, 5*time.Second, "P1 closed", func() bool { return col.summary(t, s.ID).PendingAsync == 1 })
	if sm := col.summary(t, s.ID); len(sm.PendingIDs) != 1 || sm.PendingIDs[0] != "P2" {
		t.Errorf("pending ids = %v, want [P2]", sm.PendingIDs)
	}

	// seal: P2 is orphaned AND listed as retry_pending; session incomplete
	resp, err := plain.Post(in.colSrv.URL+"/v1/internal/sessions/"+s.ID+"/seal", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var sr collector.SealResult
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if resp.StatusCode != 200 || !sr.Incomplete || len(sr.Orphaned) != 1 || sr.Orphaned[0] != "P2" ||
		len(sr.RetryPending) != 1 || sr.RetryPending[0] != "P2" {
		t.Errorf("seal: %d %+v, want incomplete, orphaned=[P2], retry_pending=[P2]", resp.StatusCode, sr)
	}
	var flagged bool
	for _, e := range col.events(t, s.ID) {
		if e.SpanID == "P2" {
			flagged = contains(e.Flags, "orphaned") && contains(e.Flags, "retry_pending")
		}
		if e.SpanID == "P1" && (contains(e.Flags, "orphaned") || contains(e.Flags, "retry_pending")) {
			t.Errorf("P1 (completed) flagged %v", e.Flags)
		}
	}
	if !flagged {
		t.Error("publisher P2 lacks orphaned+retry_pending flags")
	}
	// after seal, the open tx still isn't reported effective=false-by-surprise; failed ones stay false
	if got := effective("err"); got == nil || *got {
		t.Errorf("after seal tx-err effective=%v", got)
	}
}
