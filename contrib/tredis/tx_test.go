package tredis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// wantTx checks the transaction attributes of the events of one transaction,
// which start at index from.
func wantTx(t *testing.T, evs []core.Event, from int, outcome string, names ...string) {
	t.Helper()
	if len(evs) < from+len(names) {
		panic(fmt.Sprintf("%d events, want %d from #%d on", len(evs), len(names), from))
	}
	evs = evs[from : from+len(names)]
	for i, e := range evs {
		a := e.Attrs
		if e.Name != names[i] || a["transaction"] != "true" || a["transaction_outcome"] != outcome ||
			a["pipeline"] != "true" || a["pipeline_size"] != itoa(len(names)) || a["pipeline_index"] != itoa(i) {
			t.Errorf("%q, want %q %s: attrs %v", e.Name, names[i], outcome, a)
		}
	}
}

func itoa(n int) string { return string(rune('0' + n)) }

func TestTransactionIsMarked(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("tx:*"))
		c.HSet(context.Background(), "tx:hash", "f", "v")

		if _, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "tx:a", "1", 0)
			p.Incr(ctx, "tx:n")
			return nil
		}); err != nil {
			panic(err)
		}
		evs := rec.Events()
		wantTx(t, evs, 0, "committed", "SET tx:a", "INCR tx:n")
		if len(evs[0].Mutations) != 1 || len(evs[1].Mutations) != 1 || len(evs[0].Mutations[0].Diff) != 1 {
			t.Errorf("committed writes: %+v", evs)
		}

		// Redis does not roll back: the command that fails is the only one undone.
		c.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "tx:a", "2", 0)
			p.Incr(ctx, "tx:hash") // WRONGTYPE
			return nil
		})
		evs = rec.Events()
		wantTx(t, evs, 2, "committed", "SET tx:a", "INCR tx:hash")
		if len(evs[2].Mutations) != 1 || evs[3].Err == "" || len(evs[3].Mutations) != 0 {
			t.Errorf("one command failed: %+v", evs[2:])
		}

		// A plain pipeline is not a transaction.
		c.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "tx:a", "3", 0)
			return nil
		})
		e := rec.Events()[4]
		if _, ok := e.Attrs["transaction"]; ok || e.Attrs["transaction_outcome"] != "" || e.Attrs["pipeline_size"] != "1" {
			t.Errorf("pipeline: %v", e.Attrs)
		}
	})
}

func TestAbortedTransactionRecordsNoMutation(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("tx:*"))
		other := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})
		defer other.Close()
		bg := context.Background()
		other.Set(bg, "tx:w", "0", 0)

		err := c.Watch(ctx, func(tx *redis.Tx) error {
			other.Set(bg, "tx:w", "changed", 0) // the watched key changes before EXEC
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, "tx:w", "1", 0)
				p.Incr(ctx, "tx:n")
				return nil
			})
			return err
		}, "tx:w")
		if !errors.Is(err, redis.TxFailedErr) {
			panic(fmt.Sprintf("err=%v, want TxFailedErr", err))
		}
		if v := other.Get(bg, "tx:w").Val(); v != "changed" {
			panic(fmt.Sprintf("the transaction ran: tx:w=%q", v))
		}
		evs := rec.Events()
		if evs[0].Name != "WATCH tx:w" || evs[0].Kind != core.KindCacheRead {
			t.Errorf("%s %q", evs[0].Kind, evs[0].Name)
		}
		wantTx(t, evs, 1, "aborted", "SET tx:w", "INCR tx:n")
		for _, e := range evs[1:3] { // go-redis v8 ends with an UNWATCH
			if len(e.Mutations) != 0 || e.Err == "" {
				t.Errorf("%q: err=%q mutations=%+v", e.Name, e.Err, e.Mutations)
			}
		}
	})
}

// miniredis does not answer EXECABORT; redisx tests the outcome without a server.
func TestRefusedTransaction(t *testing.T) {
	realServer(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr)
		if _, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "tx:a", "1", 0)
			p.Do(ctx, "NOSUCHCOMMAND", "x") // refused when queued: EXEC answers EXECABORT
			return nil
		}); err == nil {
			panic("want an error")
		}
		for _, e := range rec.Events() {
			if e.Attrs["transaction_outcome"] != "failed" || len(e.Mutations) != 0 {
				t.Errorf("%q: outcome %q, mutations %+v", e.Name, e.Attrs["transaction_outcome"], e.Mutations)
			}
		}
	})
}

func TestPublishIsNotAMutation(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("*"))
		ch := &countHook{}
		c.AddHook(ch)
		if err := c.Publish(ctx, "walk-chan", `{"id":1}`).Err(); err != nil {
			panic(err)
		}
		if n := ch.n.Load(); n != 1 {
			t.Errorf("round trips=%d: the channel was read as a key", n)
		}
		evs := rec.Events()
		if len(evs) != 1 {
			panic(fmt.Sprintf("events=%d", len(evs)))
		}
		e := evs[0]
		if e.Kind != core.KindCacheWrite || e.Name != "PUBLISH walk-chan" || len(e.Mutations) != 0 {
			t.Errorf("%s %q mutations=%+v", e.Kind, e.Name, e.Mutations)
		}
		if e.Attrs["system"] != "redis" || e.Attrs["queue"] != "walk-chan" || string(e.Resp.Inline) != "0" {
			t.Errorf("attrs %v resp %s", e.Attrs, e.Resp.Inline)
		}
	})
}
