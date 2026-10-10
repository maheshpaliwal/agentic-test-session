package tmongo

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"tsession/core"
)

var reqID atomic.Int64

func setup(t *testing.T) (*event.CommandMonitor, *core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	return Monitor(), rec, ctx
}

// run drives Started then Succeeded for one command.
func run(t *testing.T, m *event.CommandMonitor, ctx context.Context, name, db string, cmd, reply bson.D) {
	t.Helper()
	id := reqID.Add(1)
	m.Started(ctx, &event.CommandStartedEvent{Command: mustRaw(t, cmd), CommandName: name, DatabaseName: db, RequestID: id})
	m.Succeeded(ctx, &event.CommandSucceededEvent{
		CommandFinishedEvent: event.CommandFinishedEvent{CommandName: name, RequestID: id}, Reply: mustRaw(t, reply)})
}

func mustRaw(t *testing.T, d bson.D) bson.Raw {
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestInsertAndStripNoise(t *testing.T) {
	m, rec, ctx := setup(t)
	run(t, m, ctx, "insert", "app", bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: 7}, {Key: "name", Value: "ann"}}}},
		{Key: "lsid", Value: bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: []byte{1, 2}}}}},
		{Key: "$db", Value: "app"},
	}, bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: int32(1)}})
	ev := rec.Events()
	if len(ev) != 1 || ev[0].Kind != core.KindDBWrite || ev[0].Name != "insert app.users" {
		t.Fatalf("%+v", ev)
	}
	req := string(ev[0].Req.Inline)
	if strings.Contains(req, "lsid") || strings.Contains(req, "$db") || !strings.Contains(req, "ann") {
		t.Fatalf("req: %s", req)
	}
	mu := ev[0].Mutations
	if len(mu) != 1 || mu[0].Store != "mongo" || mu[0].Entity != "users" || mu[0].Op != "insert" ||
		mu[0].PK["_id"] != int32(7) {
		t.Fatalf("%+v", mu)
	}
}

func TestFindReadSummary(t *testing.T) {
	m, rec, ctx := setup(t)
	run(t, m, ctx, "find", "app", bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "age", Value: 3}}}},
		bson.D{{Key: "ok", Value: 1.0}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)},
			{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: "a"}}, bson.D{{Key: "_id", Value: "b"}}}}}}})
	e := rec.Events()[0]
	if e.Kind != core.KindDBRead || len(e.Reads) != 1 || e.Reads[0].RowCount != 2 || e.Reads[0].PKs[1] != "b" {
		t.Fatalf("%+v", e)
	}
}

func TestUpdateDeleteAndNoMatch(t *testing.T) {
	m, rec, ctx := setup(t)
	upd := func(n int32) {
		run(t, m, ctx, "update", "app", bson.D{{Key: "update", Value: "users"}, {Key: "updates", Value: bson.A{
			bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 2}}}}}}}}},
			bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: n}, {Key: "nModified", Value: n}})
	}
	upd(0)
	upd(1)
	run(t, m, ctx, "delete", "app", bson.D{{Key: "delete", Value: "users"}, {Key: "deletes", Value: bson.A{
		bson.D{{Key: "q", Value: bson.D{{Key: "status", Value: "x"}}}, {Key: "limit", Value: 0}}}}},
		bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: int32(2)}})
	ev := rec.Events()
	if len(ev[0].Mutations) != 0 || len(ev[1].Mutations) != 1 || ev[1].Mutations[0].Op != "update" ||
		!strings.Contains(string(ev[1].Mutations[0].After.Inline), "$set") {
		t.Fatalf("%+v", ev[:2])
	}
	d := ev[2].Mutations[0]
	if d.Op != "delete_many" || d.PK["filter"] == nil {
		t.Fatalf("%+v", d)
	}
}

func TestFindAndModifyImages(t *testing.T) {
	m, rec, ctx := setup(t)
	fam := func(newDoc bool) {
		run(t, m, ctx, "findAndModify", "app", bson.D{{Key: "findAndModify", Value: "users"},
			{Key: "query", Value: bson.D{{Key: "_id", Value: 1}}}, {Key: "update", Value: bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}},
			{Key: "new", Value: newDoc}},
			bson.D{{Key: "ok", Value: 1.0}, {Key: "value", Value: bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: 5}}},
				{Key: "lastErrorObject", Value: bson.D{{Key: "n", Value: 1}, {Key: "updatedExisting", Value: true}}}})
	}
	fam(false)
	fam(true)
	ev := rec.Events()
	b := ev[0].Mutations[0]
	if !strings.Contains(string(b.Before.Inline), `"n":5`) {
		t.Fatalf("%+v", b)
	}
	a := ev[1].Mutations[0]
	if !strings.Contains(string(a.After.Inline), `"n":5`) || !a.Before.IsZero() {
		t.Fatalf("%+v", a)
	}
}

func TestFailedTransactionAndIgnore(t *testing.T) {
	m, rec, ctx := setup(t)
	id := reqID.Add(1)
	lsid := bson.D{{Key: "id", Value: bson.Binary{Subtype: 4, Data: []byte{0xab, 0xcd}}}}
	m.Started(ctx, &event.CommandStartedEvent{CommandName: "insert", DatabaseName: "app", RequestID: id, Command: mustRaw(t, bson.D{
		{Key: "insert", Value: "c"}, {Key: "documents", Value: bson.A{bson.D{{Key: "a", Value: 1}}}},
		{Key: "lsid", Value: lsid}, {Key: "txnNumber", Value: int64(3)}, {Key: "autocommit", Value: false}})})
	m.Failed(ctx, &event.CommandFailedEvent{CommandFinishedEvent: event.CommandFinishedEvent{RequestID: id}, Failure: errors.New("boom")})
	run(t, m, ctx, "ping", "admin", bson.D{{Key: "ping", Value: 1}}, bson.D{{Key: "ok", Value: 1.0}})
	run(t, m, context.Background(), "find", "app", bson.D{{Key: "find", Value: "c"}}, bson.D{{Key: "ok", Value: 1.0}})
	ev := rec.Events()
	if len(ev) != 1 || ev[0].Err != "boom" || ev[0].TxID != "abcd-3" || len(ev[0].Mutations) != 0 {
		t.Fatalf("%+v", ev)
	}
}

func TestApplyChainAndIdempotent(t *testing.T) {
	_, rec, ctx := setup(t)
	var called bool
	o := options.Client().SetMonitor(&event.CommandMonitor{Started: func(context.Context, *event.CommandStartedEvent) { called = true }})
	Apply(o)
	first := o.Monitor
	Apply(o)
	if o.Monitor != first {
		t.Fatal("Apply not idempotent")
	}
	o.Monitor.Started(ctx, &event.CommandStartedEvent{CommandName: "find", DatabaseName: "d", RequestID: reqID.Add(1),
		Command: mustRaw(t, bson.D{{Key: "find", Value: "c"}})})
	if !called {
		t.Fatal("previous monitor not chained")
	}
	_ = rec
	core.Configure(core.Config{})
	o2 := options.Client()
	if Apply(o2); o2.Monitor != nil {
		t.Fatal("disabled Apply must not install a monitor")
	}
}
