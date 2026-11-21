package gosafe

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collect is an OnPanic that keeps what it is told.
type collect struct {
	mu sync.Mutex
	ps []Panic
	at []time.Time
}

func (c *collect) on(p Panic) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ps, c.at = append(c.ps, p), append(c.at, time.Now())
}

func (c *collect) all() []Panic {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Panic(nil), c.ps...)
}

func wait(done <-chan struct{}, what string) {
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("timed out waiting for " + what)
	}
}

// settled waits for the goroutines started since before to be gone: the
// leak check the collector's tests use.
func settled(before int) {
	for deadline := time.Now().Add(5 * time.Second); runtime.NumGoroutine() > before; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			panic(fmt.Sprintf("%d goroutines, %d before:\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)]))
		}
	}
}

//go:noinline
func blowUp(secret string) {
	var s []int
	_ = s[len(secret)] // a runtime panic, with a secret among the arguments on the stack
}

// A panic is recovered, reported once, and the goroutine ends (Restarts 0);
// Done runs once. The report carries no argument of the panicking call.
func TestGoRecoversAndReportsOnce(t *testing.T) {
	before := runtime.NumGoroutine()
	var c collect
	var dones atomic.Int32
	done := make(chan struct{})
	Go(Options{Name: "worker", OnPanic: c.on, Done: func() { dones.Add(1); close(done) }}, func() { blowUp("hunter2-secret") })
	wait(done, "the goroutine to end")
	settled(before)
	ps := c.all()
	if len(ps) != 1 || dones.Load() != 1 {
		panic(fmt.Sprintf("%d reports, Done ran %d times", len(ps), dones.Load()))
	}
	p := ps[0]
	if p.Name != "worker" || !p.Runtime || p.N != 1 || p.RestartIn != 0 || !strings.Contains(p.Value, "index out of range") || p.Type == "" {
		panic(fmt.Sprintf("%+v", p))
	}
	if !strings.HasPrefix(p.Stack, "runtime.") || !strings.Contains(p.Stack, "gosafe.blowUp (") || !strings.Contains(p.Stack, "gosafe_test.go:") {
		t.Errorf("the stack does not lead to the panic:\n%s", p.Stack)
	}
	if strings.Contains(fmt.Sprintf("%+v", p), "hunter2") || strings.Contains(p.Stack, "0x") {
		t.Errorf("the report carries arguments:\n%+v", p)
	}
}

// A function that returns is not run again, whatever the policy.
func TestGoEndsWhenTheFunctionReturns(t *testing.T) {
	var c collect
	var runs atomic.Int32
	done := make(chan struct{})
	Go(Options{Name: "loop", OnPanic: c.on, Restarts: -1, Done: func() { close(done) }}, func() { runs.Add(1) })
	wait(done, "the goroutine to end")
	if runs.Load() != 1 || len(c.all()) != 0 {
		panic(fmt.Sprintf("%d runs, %d reports", runs.Load(), len(c.all())))
	}
}

// Restarts N: the function is run N more times, each after a doubled
// backoff capped at MaxBackoff, and after the panic that follows the last
// restart the goroutine ends, saying so (RestartIn 0).
func TestGoRestartsWithBackoffThenEnds(t *testing.T) {
	before := runtime.NumGoroutine()
	var c collect
	var runs atomic.Int32
	done := make(chan struct{})
	const base = 20 * time.Millisecond
	Go(Options{Name: "loop", OnPanic: c.on, Restarts: 4, Backoff: base, MaxBackoff: 3 * base, Done: func() { close(done) }},
		func() { runs.Add(1); panic(errors.New("boom")) })
	wait(done, "the goroutine to give up")
	settled(before)
	ps := c.all()
	if runs.Load() != 5 || len(ps) != 5 {
		panic(fmt.Sprintf("%d runs, %d reports, want 5 and 5", runs.Load(), len(ps)))
	}
	want := []time.Duration{base, 2 * base, 3 * base, 3 * base, 0}
	for i, p := range ps {
		if p.N != i+1 || p.RestartIn != want[i] || p.Runtime || p.Value != "boom" {
			t.Errorf("panic %d: N=%d RestartIn=%v Runtime=%v Value=%q, want RestartIn %v", i+1, p.N, p.RestartIn, p.Runtime, p.Value, want[i])
		}
		if i > 0 {
			if gap := c.at[i].Sub(c.at[i-1]); gap < want[i-1] {
				t.Errorf("restart %d came after %v, before its backoff of %v", i, gap, want[i-1])
			}
		}
	}
}

// Restarts below zero: without limit, until Stop. Stop also ends a wait for
// a restart at once, and nothing is left behind.
func TestGoRestartsWithoutLimitUntilStopped(t *testing.T) {
	before := runtime.NumGoroutine()
	var c collect
	var runs atomic.Int32
	stop, done := make(chan struct{}), make(chan struct{})
	Go(Options{Name: "loop", OnPanic: c.on, Restarts: -1, Backoff: time.Millisecond, MaxBackoff: time.Millisecond, Stop: stop, Done: func() { close(done) }},
		func() { runs.Add(1); panic("again") })
	for deadline := time.Now().Add(10 * time.Second); runs.Load() < 20; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("only %d runs", runs.Load()))
		}
	}
	close(stop)
	wait(done, "the goroutine to stop")
	settled(before)
	n := runs.Load()
	time.Sleep(10 * time.Millisecond)
	if runs.Load() != n {
		panic("the function ran after Stop")
	}

	// A long backoff does not hold up a stop.
	var c2 collect
	stop, done = make(chan struct{}), make(chan struct{})
	Go(Options{Name: "slow", OnPanic: c2.on, Restarts: -1, Backoff: time.Hour, Stop: stop, Done: func() { close(done) }}, func() { panic("once") })
	for deadline := time.Now().Add(10 * time.Second); len(c2.all()) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("no report")
		}
	}
	close(stop)
	wait(done, "the waiting goroutine to stop")
	settled(before)
}

// What is logged of a panic value is bounded and free of URL credentials
// and control characters; an OnPanic that panics does not undo the recovery.
func TestPanicValueIsCleanedAndBounded(t *testing.T) {
	var c collect
	done := make(chan struct{})
	Go(Options{Name: "x", OnPanic: func(p Panic) { c.on(p); panic("the logger broke") }, Done: func() { close(done) }}, func() {
		panic("cannot reach http://user:hunter2@10.0.0.1:9/v1/ingest?token=tok123\n\x1b[31m" + strings.Repeat("x", 5000))
	})
	wait(done, "the goroutine to end")
	p := c.all()[0]
	if len(p.Value) > maxValue || len(p.Stack) > maxStack || strings.ContainsAny(p.Value, "\n\x1b") || p.Runtime || p.Type != "string" {
		panic(fmt.Sprintf("%+v", p))
	}
	for _, secret := range []string{"hunter2", "tok123", "user:"} {
		if strings.Contains(p.Value, secret) {
			t.Errorf("the value shows %q: %s", secret, p.Value)
		}
	}
}

// Slog logs every panic once, at error level, and counts it by name.
func TestSlogLogsAtErrorLevelAndCounts(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	counts := map[string]int{}
	log := slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, nil))
	done := make(chan struct{})
	Go(Options{Name: "sweep", Restarts: 1, Backoff: time.Millisecond, Done: func() { close(done) },
		OnPanic: Slog(log, func(name string) { mu.Lock(); counts[name]++; mu.Unlock() })}, func() { blowUp("hunter2-secret") })
	wait(done, "the goroutine to end")
	mu.Lock()
	defer mu.Unlock()
	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || counts["sweep"] != 2 {
		panic(fmt.Sprintf("%d lines, counted %v:\n%s", len(lines), counts, out))
	}
	if !strings.Contains(lines[0], "level=ERROR") || !strings.Contains(lines[0], "is restarted") || !strings.Contains(lines[0], "restart_in=1ms") ||
		!strings.Contains(lines[1], "level=ERROR") || !strings.Contains(lines[1], "has ended") || !strings.Contains(out, "goroutine=sweep") {
		t.Errorf("log:\n%s", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("the log carries an argument:\n%s", out)
	}
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// The helper costs something where a goroutine starts and nothing after:
// the function it runs is called directly.
func BenchmarkGo(b *testing.B) {
	b.Run("bare", func(b *testing.B) {
		var wg sync.WaitGroup
		for range b.N {
			wg.Add(1)
			go func() { defer wg.Done() }()
		}
		wg.Wait()
	})
	b.Run("gosafe", func(b *testing.B) {
		var wg sync.WaitGroup
		o := Options{Name: "bench", Done: wg.Done}
		for range b.N {
			wg.Add(1)
			Go(o, func() {})
		}
		wg.Wait()
	})
}
