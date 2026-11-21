package collector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

func windowed(t *testing.T, window time.Duration) (*Collector, *store.Mem) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Window = window
	st := store.NewMem()
	c := New(st, cfg)
	t.Cleanup(c.Close)
	return c, st
}

func ingest(t *testing.T, c *Collector, evs ...core.Event) {
	t.Helper()
	if _, err := c.Ingest(context.Background(), "i1", evs); err != nil {
		panic(err)
	}
}

func storedSpans(t *testing.T, st *store.Mem) []string {
	t.Helper()
	evs, err := st.Events(context.Background(), sid, 0, 0)
	if err != nil {
		panic(err)
	}
	spans := make([]string, len(evs))
	for i, e := range evs {
		if e.Seq != uint64(i+1) {
			panic(fmt.Sprintf("seq gap at %d: %d", i, e.Seq))
		}
		spans[i] = e.SpanID
	}
	return spans
}

func waitStored(t *testing.T, st *store.Mem, n int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		spans := storedSpans(t, st)
		if len(spans) >= n {
			return spans
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("only %v stored after %v, want %d events", spans, within, n))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Every event gets its own window: a straggler that arrives shortly after an
// event which sorts later must still be sequenced before it, even when the
// timer armed by an older event fires in between.
func TestWindowIsPerEvent(t *testing.T) {
	const window = 300 * time.Millisecond
	c, st := windowed(t, window)
	ingest(t, c, ev("a", core.KindDBRead, 100, 1)) // t=0, arms the timer for t=300ms
	time.Sleep(240 * time.Millisecond)
	ingest(t, c, ev("x", core.KindDBRead, 200, 2)) // t=240ms
	time.Sleep(120 * time.Millisecond)             // the timer fired at t=300ms: only "a" is due
	if got := storedSpans(t, st); len(got) != 1 || got[0] != "a" {
		panic(fmt.Sprintf("after the first window: %v, want [a]", got))
	}
	ingest(t, c, ev("y", core.KindDBRead, 150, 3)) // t=360ms, sorts before x
	got := waitStored(t, st, 3, 3*time.Second)
	if got[0] != "a" || got[1] != "y" || got[2] != "x" {
		panic(fmt.Sprintf("stored order %v, want [a y x]", got))
	}
}

// An event that keeps having younger events sort before it is released after
// two windows at the latest, with everything that sorts before it.
func TestWindowHoldIsBounded(t *testing.T) {
	const window = 150 * time.Millisecond
	c, st := windowed(t, window)
	start := time.Now()
	ingest(t, c, ev("late-clock", core.KindDBRead, 1_000_000, 1)) // sorts after everything below
	for i := 0; time.Since(start) < 2*window+100*time.Millisecond; i++ {
		ingest(t, c, ev("e", core.KindDBRead, uint64(100+i), uint64(2+i)))
		time.Sleep(20 * time.Millisecond)
		for _, s := range storedSpans(t, st) {
			if s == "late-clock" {
				if held := time.Since(start); held < window {
					panic(fmt.Sprintf("released after %v, before its window of %v passed", held, window))
				}
				return
			}
		}
	}
	panic(fmt.Sprintf("event still held %v after it arrived (window %v)", time.Since(start), window))
}

// In-order traffic keeps its order and is all released without a seal.
func TestWindowKeepsOrderUnderSteadyTraffic(t *testing.T) {
	const window = 100 * time.Millisecond
	c, st := windowed(t, window)
	for i := uint64(1); i <= 30; i++ {
		ingest(t, c, ev("s", core.KindDBRead, 100+i, i))
		time.Sleep(10 * time.Millisecond)
	}
	waitStored(t, st, 30, 2*time.Second)
	evs, _ := st.Events(context.Background(), sid, 0, 0)
	for i, e := range evs {
		if e.LocalSeq != uint64(i+1) {
			panic(fmt.Sprintf("in-order traffic was reordered at %d: local seq %d", i, e.LocalSeq))
		}
	}
}

// Seal and Close release everything at once, window or not.
func TestSealIgnoresTheWindow(t *testing.T) {
	c, st := windowed(t, time.Hour)
	ingest(t, c, ev("b", core.KindDBRead, 200, 1), ev("a", core.KindDBRead, 100, 2))
	if got := storedSpans(t, st); len(got) != 0 {
		panic(fmt.Sprintf("stored inside the window: %v", got))
	}
	if _, err := c.Seal(context.Background(), sid); err != nil {
		panic(err)
	}
	if got := storedSpans(t, st); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		panic(fmt.Sprintf("after seal: %v, want [a b]", got))
	}
}
