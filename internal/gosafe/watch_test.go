package gosafe

import (
	"bytes"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// hostError is a type of the application that calls itself a runtime error
// and carries the application's data in its text.
type hostError struct{ text string }

func (e hostError) Error() string { return e.text }
func (hostError) RuntimeError()   {}

// Only a panic value whose type is one of package runtime counts as a panic
// of the Go runtime, whose text may be logged: a type of the application
// that implements runtime.Error does not, by value or by pointer.
func TestOnlyTypesOfPackageRuntimeAreRuntimePanics(t *testing.T) {
	var nilMap map[string]int
	var nilPtr *struct{ n int }
	var any1 any = "text"
	zero := 0
	for name, tc := range map[string]struct {
		fn   func()
		want bool
		text string
	}{
		"index":          {func() { blowUp("x") }, true, "index out of range"},
		"nil map":        {func() { nilMap["k"] = 1 }, true, "assignment to entry in nil map"},
		"nil pointer":    {func() { nilPtr.n++ }, true, "nil pointer dereference"},
		"divide":         {func() { _ = 1 / zero }, true, "divide by zero"},
		"type assertion": {func() { _ = any1.(int) }, true, "interface conversion"},
		"panic(nil)":     {func() { panic(nil) }, true, "nil"},
		"host value":     {func() { panic(hostError{"order of customer hunter2"}) }, false, ""},
		"host pointer":   {func() { panic(&hostError{"order of customer hunter2"}) }, false, ""},
		"plain error":    {func() { panic(fmt.Errorf("boom")) }, false, ""},
	} {
		var c collect
		done := make(chan struct{})
		Go(Options{Name: name, OnPanic: c.on, Done: func() { close(done) }}, tc.fn)
		wait(done, name)
		p := c.all()[0]
		if p.Runtime != tc.want || !strings.Contains(p.Value, tc.text) {
			t.Errorf("%s: Runtime=%v (type %s, value %q), want %v with %q", name, p.Runtime, p.Type, p.Value, tc.want, tc.text)
		}
	}
}

// aTypeWhoseNameIsLongEnoughThatThirtyTwoFramesOfItsMethodAreMoreThanFourKibibytesOfStackWhateverThePathOfTheSourceFileIs
// makes the frames of a stack long.
type aTypeWhoseNameIsLongEnoughThatThirtyTwoFramesOfItsMethodAreMoreThanFourKibibytesOfStackWhateverThePathOfTheSourceFileIs struct{}

//go:noinline
func (x aTypeWhoseNameIsLongEnoughThatThirtyTwoFramesOfItsMethodAreMoreThanFourKibibytesOfStackWhateverThePathOfTheSourceFileIs) descend(n int) int {
	if n == 0 {
		panic("deep")
	}
	return x.descend(n-1) + 1
}

// The value of a panic is cut to 256 bytes and its stack to 32 frames and
// 4 KiB: what reaches a log is bounded whatever panicked, and however deep.
func TestPanicValueAndStackAreCutToTheirBounds(t *testing.T) {
	var c collect
	done := make(chan struct{})
	Go(Options{Name: "x", OnPanic: c.on, Done: func() { close(done) }}, func() { panic(strings.Repeat("some words ", 2000)) })
	wait(done, "the goroutine to end")
	if p := c.all()[0]; len(p.Value) != maxValue || !strings.HasPrefix(p.Value, "some words some") {
		panic(fmt.Sprintf("the value has %d bytes, want it cut to %d: %q", len(p.Value), maxValue, p.Value))
	}

	var c2 collect
	done = make(chan struct{})
	Go(Options{Name: "x", OnPanic: c2.on, Done: func() { close(done) }}, func() {
		aTypeWhoseNameIsLongEnoughThatThirtyTwoFramesOfItsMethodAreMoreThanFourKibibytesOfStackWhateverThePathOfTheSourceFileIs{}.descend(5000)
	})
	wait(done, "the goroutine to end")
	p := c2.all()[0]
	if len(p.Stack) != maxStack || strings.Count(p.Stack, "\n") >= maxFrames || !strings.Contains(p.Stack, ".descend (") {
		panic(fmt.Sprintf("the stack has %d bytes in %d lines, want it cut to %d bytes and fewer than %d lines:\n%s",
			len(p.Stack), strings.Count(p.Stack, "\n")+1, maxStack, maxFrames, p.Stack))
	}
}

// While a goroutine waits for its restart the service is degraded, and says
// which goroutine it is; once it runs again, or is stopped, it is not.
func TestWatchIsDegradedWhileAGoroutineWaitsForItsRestart(t *testing.T) {
	w := NewWatch(nil)
	if w.Degraded() != "" || (*Watch)(nil).Degraded() != "" {
		panic("degraded before anything happened")
	}
	var runs atomic.Int32
	release, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	Go(Options{Name: "sweep", Restarts: -1, Backoff: 150 * time.Millisecond, Watch: w, Stop: stop, Done: func() { close(done) }}, func() {
		if runs.Add(1) == 1 {
			panic("once")
		}
		<-release
	})
	for deadline := time.Now().Add(10 * time.Second); w.Degraded() == ""; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("never degraded")
		}
	}
	if got := w.Degraded(); !strings.Contains(got, "waits to be restarted: sweep") || runs.Load() != 1 {
		panic(fmt.Sprintf("%q after %d runs", got, runs.Load()))
	}
	for deadline := time.Now().Add(10 * time.Second); w.Degraded() != "" || runs.Load() != 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("still %q after %d runs", w.Degraded(), runs.Load()))
		}
	}
	close(release)
	wait(done, "the goroutine to end")

	// Stopped while it waits: not degraded for a goroutine that is gone.
	done = make(chan struct{})
	Go(Options{Name: "drain", Restarts: -1, Backoff: time.Hour, Watch: w, Stop: stop, Done: func() { close(done) }}, func() { panic("always") })
	for deadline := time.Now().Add(10 * time.Second); w.Degraded() == ""; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			panic("never degraded")
		}
	}
	close(stop)
	wait(done, "the goroutine to end")
	if w.Degraded() != "" {
		panic("degraded for a goroutine that was stopped: " + w.Degraded())
	}
}

// A loop that panics every time uses up the panic budget: it is not started
// again, the service is told once, after the last panic was reported, and
// stays degraded. Panics that do not count (Budget unset) never get there,
// and neither do panics that are spread over more than the window.
func TestPanicBudgetEndsTheServiceOnce(t *testing.T) {
	before := runtime.NumGoroutine()
	var spent atomic.Int32
	var reason atomic.Value
	var c collect
	w := NewWatch(func(r string) {
		if n := len(c.all()); n != PanicBudget {
			panic(fmt.Sprintf("told after %d reported panics, want %d", n, PanicBudget))
		}
		reason.Store(r)
		spent.Add(1)
	})
	done := make(chan struct{})
	Go(Options{Name: "sweep", Restarts: -1, Backoff: time.Millisecond, MaxBackoff: time.Millisecond, Watch: w, Budget: true, OnPanic: c.on,
		Done: func() { close(done) }}, func() { panic("always") })
	wait(done, "the loop to end")
	ps := c.all()
	if len(ps) != PanicBudget || spent.Load() != 1 || !ps[PanicBudget-1].BudgetSpent || ps[PanicBudget-1].RestartIn != 0 || ps[0].BudgetSpent {
		panic(fmt.Sprintf("%d panics, told %d times: %+v", len(ps), spent.Load(), ps))
	}
	if r, _ := reason.Load().(string); !strings.Contains(r, "the panic budget is spent: 5 panics of background goroutines within 10m0s") || w.Degraded() != r {
		panic(fmt.Sprintf("reason %q, degraded %q", r, w.Degraded()))
	}
	// Another loop of the service that panics now is not started again
	// either, and the service is not told twice.
	done = make(chan struct{})
	Go(Options{Name: "janitor", Restarts: -1, Backoff: time.Millisecond, Watch: w, Budget: true, OnPanic: c.on, Done: func() { close(done) }},
		func() { panic("too") })
	wait(done, "the other loop to end")
	if spent.Load() != 1 || len(c.all()) != PanicBudget+1 {
		panic(fmt.Sprintf("told %d times, %d panics", spent.Load(), len(c.all())))
	}
	settled(before)

	// Panics that do not count.
	w = NewWatch(func(string) { panic("a goroutine without Budget spent the budget") })
	done = make(chan struct{})
	Go(Options{Name: "drain", Restarts: 2 * PanicBudget, Backoff: time.Microsecond, Watch: w, Done: func() { close(done) }}, func() { panic("always") })
	wait(done, "the goroutine to end")
	if w.Degraded() != "" {
		panic(w.Degraded())
	}

	// Panics outside the window are forgotten.
	w = NewWatch(nil)
	now := time.Now()
	every := PanicBudgetWindow/(PanicBudget-1) + time.Second
	for i := range 3 * PanicBudget {
		if w.count(now.Add(time.Duration(i) * every)) {
			panic(fmt.Sprintf("panic %d, one every %s, spent the budget", i+1, every))
		}
	}
	for i := range PanicBudget {
		if got := w.count(now.Add(24*time.Hour + time.Duration(i)*time.Second)); got != (i == PanicBudget-1) {
			panic(fmt.Sprintf("panic %d within the window: spent=%v", i+1, got))
		}
	}
}

// Slog says which panic was the last one.
func TestSlogSaysThatTheBudgetIsSpent(t *testing.T) {
	var buf bytes.Buffer
	Slog(slog.New(slog.NewTextHandler(&buf, nil)), nil)(Panic{Name: "sweep", N: 5, BudgetSpent: true})
	if out := buf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "the panic budget of the service is spent") {
		panic(out)
	}
}
