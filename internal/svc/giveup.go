package svc

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"
)

// GiveUpGrace is how long a service that gives up has to shut down in order
// before the process is ended as it is.
const GiveUpGrace = 20 * time.Second

// GiveUp is how a service ends itself when its background loops keep
// panicking (its panic budget is spent, see gosafe.PanicBudget). A service
// that looks alive and no longer does its work is worse than one that is
// gone: ended with a status other than zero, it is started again with clean
// state by whatever supervises it, and an operator sees it.
//
// It shuts down in order first (what a signal does: the listener closes,
// buffered work is flushed, the store is closed), so nothing is lost that a
// crash would have lost; if that does not finish within GiveUpGrace the
// process is ended as it is.
type GiveUp struct {
	log   *slog.Logger
	stop  context.CancelFunc
	grace time.Duration
	exit  func(code int) // os.Exit; a test replaces it

	mu     sync.Mutex
	reason string
	timer  *time.Timer
}

// NewGiveUp returns a GiveUp for a service whose shutdown stop begins (the
// cancel function of its SignalContext).
func NewGiveUp(log *slog.Logger, stop context.CancelFunc) *GiveUp {
	return &GiveUp{log: log, stop: stop, grace: GiveUpGrace, exit: os.Exit}
}

// Now begins the end of the service, once. It is what a service passes as
// OnPanicBudgetSpent.
func (g *GiveUp) Now(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reason != "" {
		return
	}
	g.reason = reason
	g.log.Error("the service gives up and exits with status 1", "reason", reason, "shutdown_within", g.grace)
	g.timer = time.AfterFunc(g.grace, func() {
		g.log.Error("the shutdown did not finish in time; the process ends as it is", "after", g.grace)
		g.exit(1)
	})
	g.stop()
}

// ExitCode is what the process exits with once it has shut down: 1 when the
// service gave up, 0 otherwise.
func (g *GiveUp) ExitCode() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reason == "" {
		return 0
	}
	g.timer.Stop()
	return 1
}
