package gosafe

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
)

const (
	// PanicBudget panics of a service's loops within PanicBudgetWindow end
	// the service. A loop that is started again after every panic keeps a
	// service alive that no longer does its work: nothing expires, nothing
	// is evicted, and only a counter says so. Ended, the service is started
	// again with clean state by whatever supervises it, and an operator sees
	// it. A panic now and then never gets there: the backoff alone spaces
	// the panics of one loop that panics every time at 100ms, 200ms, 400ms,
	// 800ms, so five of them take a second and a half.
	PanicBudget       = 5
	PanicBudgetWindow = 10 * time.Minute
)

// Watch is what the goroutines of one service say about its health: which of
// them wait for a restart after a panic (the service is degraded meanwhile,
// see Degraded), and how many panics its loops have had lately (see
// PanicBudget). A nil Watch watches nothing.
type Watch struct {
	budget int
	window time.Duration
	spent  func(reason string)

	mu      sync.Mutex
	recent  []time.Time    // the panics that count, oldest first, at most budget
	waiting map[string]int // goroutines that wait for their restart, by name
	gaveUp  bool
	reason  string // why the budget is spent; "" while it is not
}

// NewWatch returns a Watch with the panic budget of a service. spent is
// called once, on the goroutine whose panic used the budget up, after that
// panic was reported: it ends the service. It may be nil, for a service
// that is part of another process; the Watch then stays degraded.
func NewWatch(spent func(reason string)) *Watch {
	return &Watch{budget: PanicBudget, window: PanicBudgetWindow, spent: spent, waiting: map[string]int{}}
}

// Degraded says why the service is not healthy, "" when it is: a goroutine
// waits for its restart after a panic, or the panic budget is spent.
func (w *Watch) Degraded() string {
	if w == nil {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.reason != "" {
		return w.reason
	}
	if len(w.waiting) == 0 {
		return ""
	}
	names := mapx.SortedKeys(w.waiting)
	return "a background goroutine panicked and waits to be restarted: " + strings.Join(names, ", ") + "; the log has the panic"
}

// count notes a panic that counts against the budget and reports whether it
// used the budget up.
func (w *Watch) count(now time.Time) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.reason != "" {
		return true // spent before: no loop is started again
	}
	w.recent = slices.DeleteFunc(w.recent, func(t time.Time) bool { return now.Sub(t) > w.window })
	w.recent = append(w.recent, now)
	if len(w.recent) < w.budget {
		return false
	}
	w.reason = fmt.Sprintf("the panic budget is spent: %d panics of background goroutines within %s; the service ends so that it is started again with clean state",
		w.budget, w.window)
	return true
}

// giveUp tells the service, once, that the budget is spent.
func (w *Watch) giveUp() {
	w.mu.Lock()
	first, reason := !w.gaveUp, w.reason
	w.gaveUp = true // a flag and two reads: cannot panic
	w.mu.Unlock()
	if first && w.spent != nil {
		w.spent(reason)
	}
}

// sleep is the wait for a restart (see the function sleep), during which
// the goroutine name counts as waiting.
func (w *Watch) sleep(name string, d time.Duration, stop <-chan struct{}) bool {
	if w == nil {
		return sleep(d, stop)
	}
	w.wait(name, 1)
	defer w.wait(name, -1)
	return sleep(d, stop)
}

func (w *Watch) wait(name string, by int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waiting[name] += by; w.waiting[name] <= 0 {
		delete(w.waiting, name)
	}
}
