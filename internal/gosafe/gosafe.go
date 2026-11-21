// Package gosafe starts goroutines that a panic does not take the process
// down with. A bare `go f()` that panics ends the whole process: for the SDK
// that is the application it is embedded in, which recording must never
// break; for a service it is every session it holds.
//
// The helper works at the start of the goroutine only: nothing is added to
// what the function does while it runs.
//
// It is not for goroutines whose panic must travel on: one that re-raises a
// panic to the host after recording it, or one whose callers would wait
// forever, or act on half-changed state, if it carried on. Those stay bare,
// with a comment that says why.
package gosafe

import (
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/ident"
	"github.com/maheshpaliwal/agentic-test-session/internal/scrub"
)

const (
	// DefaultBackoff is the wait before the first restart; it doubles up to
	// DefaultMaxBackoff.
	DefaultBackoff    = 100 * time.Millisecond
	DefaultMaxBackoff = 30 * time.Second

	maxFrames = 32      // frames of a stack that are kept
	maxStack  = 4 << 10 // bytes of a stack that are kept
	maxValue  = 256     // bytes of a panic value that are kept
)

// Panic is a recovered panic, as it may be logged.
type Panic struct {
	// Name is the goroutine's name (Options.Name).
	Name string
	// Type is the Go type of the value panicked with.
	Type string
	// Value is that value as text: printable, without the credentials of
	// URLs, at most 256 bytes. It may still quote what the panicking code put
	// into it; a caller that must not log that prints Type, and Value only
	// when Runtime is set.
	Value string
	// Runtime is set for a panic of the Go runtime (nil dereference, index
	// out of range, ...), whose text names no data. It goes by the type of
	// the value, which must be one of package runtime: a type of the
	// application that implements runtime.Error is not believed.
	Runtime bool
	// Stack is where it panicked: function, file and line of at most 32
	// frames, at most 4 KiB. No arguments, unlike debug.Stack.
	Stack string
	// N counts the panics of this goroutine, this one included.
	N int
	// RestartIn is how long the goroutine waits before it runs the function
	// again; zero when it ends instead.
	RestartIn time.Duration
	// BudgetSpent is set on the panic that used up the panic budget of the
	// service (Options.Watch): the goroutine ends, and so does the service.
	BudgetSpent bool
}

// Options say how a goroutine is run.
type Options struct {
	// Name names the goroutine in a Panic. It is a constant of the code,
	// never data.
	Name string
	// OnPanic is told of every recovered panic, on the goroutine, before the
	// wait for a restart: it logs and counts. A panic of its own is dropped.
	// Nil drops the panic silently, which only a test wants.
	OnPanic func(Panic)
	// Restarts is how many times the function is run again after a panic: 0
	// never (the goroutine ends, the policy for work that belongs to one
	// request), below zero without limit (the policy for a loop a service
	// cannot do without).
	Restarts int
	// Backoff is the wait before the first restart; it doubles with every
	// panic up to MaxBackoff and is never taken back, so a loop that keeps
	// panicking settles at one attempt per MaxBackoff. Zero means
	// DefaultBackoff and DefaultMaxBackoff.
	Backoff, MaxBackoff time.Duration
	// Stop ends the wait for a restart: closed, the goroutine ends instead of
	// running the function again. Nil never does.
	Stop <-chan struct{}
	// Done runs once when the goroutine ends, however it ends: where a bare
	// goroutine would `defer close(done)` or `defer wg.Done()`.
	Done func()
	// Watch, if set, is told while the goroutine waits for its restart: the
	// service is degraded for that long. With Budget its panics also count
	// against the Watch's panic budget, and the one that uses it up ends the
	// goroutine whatever Restarts says.
	Watch  *Watch
	Budget bool
}

// Go runs fn on a new goroutine. When fn returns, the goroutine ends. When
// fn panics, the panic is recovered and given to o.OnPanic, and fn is run
// again after a backoff, or the goroutine ends, as o.Restarts says.
func Go(o Options, fn func()) {
	go func() {
		if o.Done != nil {
			defer o.Done()
		}
		for n := 1; ; n++ {
			p, panicked := run(fn)
			if !panicked {
				return
			}
			p.Name, p.N = o.Name, n
			p.BudgetSpent = o.Budget && o.Watch.count(time.Now())
			if !p.BudgetSpent && (o.Restarts < 0 || n <= o.Restarts) {
				p.RestartIn = o.backoff(n)
			}
			notify(o.OnPanic, p)
			if p.BudgetSpent {
				o.Watch.giveUp() // after the panic was logged
			}
			if p.RestartIn == 0 || !o.Watch.sleep(o.Name, p.RestartIn, o.Stop) {
				return
			}
		}
	}()
}

// backoff is the wait after the nth panic.
func (o Options) backoff(n int) time.Duration {
	d, limit := o.Backoff, o.MaxBackoff
	if d <= 0 {
		d = DefaultBackoff
	}
	if limit <= 0 {
		limit = DefaultMaxBackoff
	}
	for i := 1; i < n && d < limit; i++ {
		d *= 2
	}
	return min(d, max(limit, 1))
}

// sleep waits d and reports whether it was not cut short by stop.
func sleep(d time.Duration, stop <-chan struct{}) bool {
	select {
	case <-stop: // also when it was closed before the panic
		return false
	default:
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-stop:
		return false
	}
}

func run(fn func()) (p Panic, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			p, panicked = describe(r), true
		}
	}()
	fn()
	return Panic{}, false
}

func notify(f func(Panic), p Panic) {
	if f == nil {
		return
	}
	defer func() { _ = recover() }() // a logger that panics must not undo the recovery
	f(p)
}

// describe is called by the function that recovered r, so the stack still
// holds the frames that panicked.
func describe(r any) Panic {
	p := Panic{Type: fmt.Sprintf("%T", r)}
	var text string
	switch v := r.(type) {
	case error:
		p.Runtime, text = runtimeError(v), v.Error()
	default:
		text = fmt.Sprint(v)
	}
	p.Value = scrub.Text(ident.CleanN(text, 4*maxValue))
	if len(p.Value) > maxValue {
		p.Value = p.Value[:maxValue]
	}
	p.Stack = stack()
	return p
}

// runtimeError reports whether e is an error the Go runtime itself panics
// with. The interface runtime.Error says nothing: any type can implement it,
// and the text of such a type is the application's. The types of package
// runtime are what counts (boundsError, errorString, TypeAssertionError,
// PanicNilError, ...); no other package can have that path.
func runtimeError(e error) bool {
	if _, ok := e.(runtime.Error); !ok {
		return false
	}
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.PkgPath() == "runtime"
}

// stack lists the frames from the one that panicked outwards.
func stack() string {
	pcs := make([]uintptr, 2*maxFrames)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	var lines []string
	for {
		f, more := frames.Next()
		if f.Function == "runtime.gopanic" {
			lines = lines[:0] // what came before is the recovering side
		} else if f.Function != "" {
			lines = append(lines, fmt.Sprintf("%s (%s:%d)", f.Function, f.File, f.Line))
		}
		if !more {
			break
		}
	}
	if len(lines) > maxFrames {
		lines = lines[:maxFrames]
	}
	s := strings.Join(lines, "\n")
	if len(s) > maxStack {
		s = s[:maxStack]
	}
	return s
}

// Slog returns an OnPanic for a service: every panic is logged at error
// level and counted (count may be nil). The services log the value: it is
// their own code's word, and they hold no application data in it.
func Slog(log *slog.Logger, count func(name string)) func(Panic) {
	return func(p Panic) {
		if count != nil {
			count(p.Name)
		}
		args := []any{"goroutine", p.Name, "panic", p.Value, "type", p.Type, "panics", p.N, "stack", p.Stack}
		if p.BudgetSpent {
			log.Error("a background goroutine panicked and is not restarted: the panic budget of the service is spent", args...)
			return
		}
		if p.RestartIn > 0 {
			log.Error("a background goroutine panicked and is restarted", append(args, "restart_in", p.RestartIn)...)
			return
		}
		log.Error("a background goroutine panicked and has ended", args...)
	}
}
