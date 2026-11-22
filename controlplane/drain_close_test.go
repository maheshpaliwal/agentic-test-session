package controlplane

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// lastPanicCollector is a collector whose drain-status panics every time;
// from the call that uses up the drain's panics on, it first waits until the
// drain is cancelled.
type lastPanicCollector struct {
	*flakyCollector
	calls   atomic.Int32
	waiting chan struct{} // closed when the last call waits
}

func (c *lastPanicCollector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	if c.calls.Add(1) == maxDrainPanics {
		close(c.waiting)
		<-ctx.Done()
	}
	panic("drain-status broke")
}

// The drain's last panic ends the session incomplete: the control plane
// failed. Not when the drain was cancelled before it panicked, because the
// control plane is closing: the session then stays DRAINING in the store,
// and the next start drains it anew. A session sealed by a control plane on
// its way out would be final, incomplete, and without the wait for its
// events that a drain is.
func TestADrainWhoseLastPanicComesWhileTheControlPlaneClosesLeavesTheSessionDraining(t *testing.T) {
	col := &lastPanicCollector{flakyCollector: newFlaky(), waiting: make(chan struct{})}
	col.failing.Store(false)
	store := NewMemStore()
	m := NewManager(Config{Store: store, Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: (&logBuffer{}).logger()})
	s, err := m.Start(StartRequest{Name: "p", MaxDrainSeconds: 1000})
	if err != nil {
		panic(err)
	}
	if _, err := m.Stop(s.ID); err != nil {
		panic(err)
	}
	select {
	case <-col.waiting:
	case <-time.After(30 * time.Second):
		panic(fmt.Sprintf("the drain was not started again: %d runs", col.calls.Load()))
	}
	m.Close() // cancels the drain, which panics for the last time, and waits for it
	if n := col.calls.Load(); n != maxDrainPanics {
		panic(fmt.Sprintf("the drain ran %d times", n))
	}
	got, _ := store.Get(s.ID)
	if got.State != StateDraining || got.Incomplete || len(got.IncompleteReasons) != 0 || !got.EndedAt.IsZero() || got.SealPending {
		panic(fmt.Sprintf("a drain that was cancelled ended its session: %+v", got))
	}
	if !panicMetric(t, m, "drain", maxDrainPanics) {
		panic("the last panic is not counted:\n" + scrapeMetrics(t, m))
	}
}
