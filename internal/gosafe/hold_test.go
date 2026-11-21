package gosafe

import (
	"testing"
	"time"
)

// Work that something else starts anew after a panic holds the service
// degraded until its next attempt is due, or until one went through.
func TestWatchHoldIsDegradedUntilItsTimeOrARelease(t *testing.T) {
	const prefix, suffix = "a background goroutine panicked and waits to be restarted: ", "; the log has the panic"
	var none *Watch
	none.Hold("x", time.Now().Add(time.Hour)) // a nil Watch watches nothing
	none.Release("x")

	w := NewWatch(nil)
	w.Hold("seal_retry", time.Now().Add(time.Hour))
	w.Hold("seal_retry", time.Now()) // never moved back
	w.Hold("compaction", time.Now().Add(time.Hour))
	if got := w.Degraded(); got != prefix+"compaction, seal_retry"+suffix {
		panic(got)
	}
	w.Release("compaction")
	w.wait("seal_retry", 1) // waiting and held: named once
	w.wait("drain", 1)
	if got := w.Degraded(); got != prefix+"drain, seal_retry"+suffix {
		panic(got)
	}
	w.wait("seal_retry", -1)
	w.wait("drain", -1)
	w.Release("seal_retry")
	if got := w.Degraded(); got != "" {
		panic(got)
	}
	w.Hold("seal_retry", time.Now().Add(20*time.Millisecond))
	if w.Degraded() == "" {
		panic("not degraded before the time")
	}
	for deadline := time.Now().Add(10 * time.Second); w.Degraded() != ""; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("degraded after the time: " + w.Degraded())
		}
	}
}
