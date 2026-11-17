package tmongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func ids(ms []core.Mutation) string {
	var out []string
	for _, m := range ms {
		id := m.PK["_id"]
		if id == nil {
			id = asDoc(m.PK["filter"])["k"]
		}
		out = append(out, fmt.Sprintf("%s:%v", m.Op, id))
	}
	return strings.Join(out, " ")
}

func writeErrors(idx ...int32) bson.A {
	var a bson.A
	for _, i := range idx {
		a = append(a, bson.D{{Key: "index", Value: i}, {Key: "code", Value: int32(11000)}, {Key: "errmsg", Value: "dup"}})
	}
	return a
}

func TestOrderedWritesStopAtFirstError(t *testing.T) {
	docs := bson.A{bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "_id", Value: 3}}, bson.D{{Key: "_id", Value: 4}}}
	stmt := func(k int) bson.D { return bson.D{{Key: "k", Value: k}} }
	updates := bson.A{}
	deletes := bson.A{}
	for k := 1; k <= 4; k++ {
		updates = append(updates, bson.D{{Key: "q", Value: stmt(k)}, {Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}}}})
		deletes = append(deletes, bson.D{{Key: "q", Value: stmt(k)}, {Key: "limit", Value: int32(1)}})
	}
	for _, tc := range []struct {
		name    string
		cmd     bson.D
		errs    bson.A
		ordered any // nil = field absent (server default: ordered)
		want    string
	}{
		{"insert", bson.D{{Key: "insert", Value: "c"}, {Key: "documents", Value: docs}}, writeErrors(1), true, "insert:1"},
		{"insert", bson.D{{Key: "insert", Value: "c"}, {Key: "documents", Value: docs}}, writeErrors(1), nil, "insert:1"},
		{"insert", bson.D{{Key: "insert", Value: "c"}, {Key: "documents", Value: docs}}, writeErrors(1, 3), false, "insert:1 insert:3"},
		{"insert", bson.D{{Key: "insert", Value: "c"}, {Key: "documents", Value: docs}}, nil, true, "insert:1 insert:2 insert:3 insert:4"},
		{"update", bson.D{{Key: "update", Value: "c"}, {Key: "updates", Value: updates}}, writeErrors(2), true, "update:1 update:2"},
		{"update", bson.D{{Key: "update", Value: "c"}, {Key: "updates", Value: updates}}, writeErrors(2), false, "update:1 update:2 update:4"},
		{"delete", bson.D{{Key: "delete", Value: "c"}, {Key: "deletes", Value: deletes}}, writeErrors(0), true, ""},
		{"delete", bson.D{{Key: "delete", Value: "c"}, {Key: "deletes", Value: deletes}}, writeErrors(0), false, "delete:2 delete:3 delete:4"},
	} {
		m, rec, ctx := setup(t)
		cmd := tc.cmd
		if tc.ordered != nil {
			cmd = append(cmd, bson.E{Key: "ordered", Value: tc.ordered})
		}
		reply := bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: int32(1)}}
		if tc.errs != nil {
			reply = append(reply, bson.E{Key: "writeErrors", Value: tc.errs})
		}
		run(t, m, ctx, tc.name, "app", cmd, reply)
		if got := ids(rec.Events()[0].Mutations); got != tc.want {
			t.Errorf("%s ordered=%v errors=%v: mutations %q, want %q", tc.name, tc.ordered, tc.errs, got, tc.want)
		}
	}
}

// The reply shape follows the MongoDB 8 bulkWrite command documentation
// (errorsOnly cursor entries {ok: 0, idx: n}); it is not verified against a
// live server here.
func TestBulkWriteSkipsFailedAndUnattemptedOps(t *testing.T) {
	ins := func(id int) bson.D {
		return bson.D{{Key: "insert", Value: int32(0)}, {Key: "document", Value: bson.D{{Key: "_id", Value: id}}}}
	}
	for _, tc := range []struct {
		ordered bool
		want    string
	}{{true, "insert:1"}, {false, "insert:1 insert:3"}} {
		m, rec, ctx := setup(t)
		run(t, m, ctx, "bulkWrite", "admin", bson.D{
			{Key: "bulkWrite", Value: int32(1)},
			{Key: "ops", Value: bson.A{ins(1), ins(1), ins(3)}},
			{Key: "nsInfo", Value: bson.A{bson.D{{Key: "ns", Value: "app.users"}}}},
			{Key: "ordered", Value: tc.ordered},
		}, bson.D{{Key: "ok", Value: 1.0}, {Key: "nErrors", Value: int32(1)},
			{Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "firstBatch", Value: bson.A{
				bson.D{{Key: "ok", Value: 0.0}, {Key: "idx", Value: int32(1)}, {Key: "code", Value: int32(11000)}}}}}}})
		if got := ids(rec.Events()[0].Mutations); got != tc.want {
			t.Errorf("ordered=%v: mutations %q, want %q", tc.ordered, got, tc.want)
		}
	}
}

func TestDottedSensitivePathsRedacted(t *testing.T) {
	m, rec, ctx := setup(t)
	set := bson.D{{Key: "card.cvv", Value: "cvv-937"}, {Key: "user.pwd", Value: "pwd-leak-1"}, {Key: "id.ssn", Value: "ssn-leak-1"},
		{Key: "auth.password", Value: "pw-leak-1"}, {Key: "card.brand", Value: "visa"}}
	run(t, m, ctx, "update", "app", bson.D{
		{Key: "update", Value: "users"},
		{Key: "updates", Value: bson.A{bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: 1}, {Key: "card.cvv", Value: "cvv-filter"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: set}}}}}},
	}, bson.D{{Key: "ok", Value: 1.0}, {Key: "n", Value: int32(1)}, {Key: "nModified", Value: int32(1)}})
	run(t, m, ctx, "find", "app", bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{{Key: "id.ssn", Value: "ssn-find"}}}},
		bson.D{{Key: "ok", Value: 1.0}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "firstBatch", Value: bson.A{}}}}})
	var all strings.Builder
	for _, e := range rec.Events() {
		all.WriteString(string(e.Req.Inline))
		for _, mu := range e.Mutations {
			fmt.Fprint(&all, mu.PK, string(mu.After.Inline))
		}
		for _, r := range e.Reads {
			all.WriteString(string(r.Args.Inline))
		}
	}
	txt := all.String()
	for _, bad := range []string{"cvv-937", "pwd-leak-1", "ssn-leak-1", "pw-leak-1", "cvv-filter", "ssn-find"} {
		if strings.Contains(txt, bad) {
			t.Errorf("leaked %q in %s", bad, txt)
		}
	}
	if !strings.Contains(txt, "visa") || !strings.Contains(txt, "card.cvv") {
		t.Errorf("over-redacted: %s", txt)
	}
}

func pendingLen(tr *tracer) int {
	n := 0
	tr.pending.Range(func(_, _ any) bool { n++; return true })
	return n
}

func TestPendingBounded(t *testing.T) {
	_, rec, ctx := setup(t)
	tr := &tracer{ignore: map[string]bool{}, maxPending: 8, pendingAge: time.Hour}
	start := func(id int64) {
		tr.started(ctx, &event.CommandStartedEvent{Command: mustRaw(t, bson.D{{Key: "find", Value: "c"}}),
			CommandName: "find", DatabaseName: "app", RequestID: id})
	}
	for id := int64(0); id < 100; id++ {
		start(id)
	}
	if n := pendingLen(tr); n != 8 || tr.npending.Load() != 8 {
		t.Fatalf("pending=%d counter=%d, want 8", n, tr.npending.Load())
	}
	evs := rec.Events()
	if len(evs) != 92 || len(evs[0].Flags) != 1 || evs[0].Flags[0] != "unfinished" {
		t.Fatalf("untracked commands must be recorded as unfinished: %d events", len(evs))
	}
	// A normal completion frees its slot.
	tr.succeeded(ctx, &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "find", RequestID: 3},
		Reply: mustRaw(t, bson.D{{Key: "ok", Value: 1.0}})})
	if tr.npending.Load() != 7 {
		t.Fatalf("counter=%d after a completion", tr.npending.Load())
	}
	// Stale entries are swept (and recorded) once the table is full again.
	tr.pendingAge = 0
	tr.lastSweep.Store(0)
	time.Sleep(time.Millisecond)
	start(1000)
	start(1001)
	if n := pendingLen(tr); n != 1 || tr.npending.Load() != 1 {
		t.Fatalf("pending=%d counter=%d after sweep, want 1", n, tr.npending.Load())
	}
	if _, ok := tr.pending.Load(int64(1001)); !ok {
		t.Fatal("the new command must be tracked after the sweep")
	}
}

func TestUntracedCommandIsNotDecoded(t *testing.T) {
	m, rec, _ := setup(t)
	docs := bson.A{}
	for i := 0; i < 200; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "name", Value: "ann"}})
	}
	ev := &event.CommandStartedEvent{Command: mustRaw(t, bson.D{{Key: "insert", Value: "c"}, {Key: "documents", Value: docs}}),
		CommandName: "insert", DatabaseName: "app", RequestID: 1}
	bg := context.Background() // no session
	if allocs := testing.AllocsPerRun(20, func() { m.Started(bg, ev) }); allocs > 5 {
		t.Fatalf("untraced Started allocates %.0f times", allocs)
	}
	if len(rec.Events()) != 0 {
		t.Fatal("untraced command recorded")
	}
}

// Real MongoDB, gated: TSESSION_TEST_MONGO_URI=mongodb://127.0.0.1:57017
func TestRealMongoOrderedInsert(t *testing.T) {
	uri := os.Getenv("TSESSION_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("TSESSION_TEST_MONGO_URI not set")
	}
	_, rec, ctx := setup(t)
	client, err := mongo.Connect(Apply(options.Client().ApplyURI(uri).SetServerSelectionTimeout(3 * time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	bg := context.Background()
	db := client.Database(fmt.Sprintf("tsession_tmongo_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = db.Drop(bg); _ = client.Disconnect(bg) })
	coll := db.Collection("users")

	many := []any{bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 3}},
		bson.D{{Key: "_id", Value: 4}, {Key: "card.cvv", Value: "cvv-live"}}}
	if _, err := coll.InsertMany(ctx, many); err == nil {
		t.Fatal("expected a duplicate key error")
	}
	if _, err := coll.InsertMany(ctx, many, options.InsertMany().SetOrdered(false)); err == nil {
		t.Fatal("expected a duplicate key error")
	}
	evs := rec.Events()
	if len(evs) != 2 {
		t.Fatalf("events=%d", len(evs))
	}
	if got := ids(evs[0].Mutations); got != "insert:1" {
		t.Errorf("ordered: mutations %q, want insert:1", got)
	}
	if got := ids(evs[1].Mutations); got != "insert:3 insert:4" {
		t.Errorf("unordered: mutations %q, want insert:3 insert:4", got)
	}
	if n, _ := coll.CountDocuments(bg, bson.D{}); n != 3 {
		t.Fatalf("documents in collection=%d", n)
	}
	if strings.Contains(string(evs[1].Req.Inline)+string(evs[1].Mutations[1].After.Inline), "cvv-live") {
		t.Errorf("leaked: %s", evs[1].Req.Inline)
	}
}
