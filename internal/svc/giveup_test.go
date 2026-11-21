package svc

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// A service that gives up begins its shutdown once, says why at error level,
// and exits 1 when the shutdown is done; one that did not give up exits 0.
func TestGiveUpShutsDownOnceAndExitsWith1(t *testing.T) {
	var logs lockedBuffer
	var stops, exits atomic.Int32
	g := NewGiveUp(slog.New(slog.NewTextHandler(&logs, nil)), func() { stops.Add(1) })
	g.exit = func(int) { exits.Add(1) }
	if g.ExitCode() != 0 {
		panic("status 1 before anything happened")
	}
	g.Now("the panic budget is spent")
	g.Now("again")
	if stops.Load() != 1 || g.ExitCode() != 1 {
		panic(fmt.Sprintf("stopped %d times, status %d", stops.Load(), g.ExitCode()))
	}
	out := logs.String()
	if strings.Count(out, "level=ERROR") != 1 || !strings.Contains(out, "the service gives up and exits with status 1") ||
		!strings.Contains(out, `reason="the panic budget is spent"`) {
		panic(out)
	}
	if exits.Load() != 0 {
		panic("the process was ended although the shutdown finished")
	}
}

// A shutdown that does not finish in time does not keep the process: it is
// ended as it is.
func TestGiveUpEndsAProcessWhoseShutdownHangs(t *testing.T) {
	var logs lockedBuffer
	exited := make(chan int, 1)
	g := NewGiveUp(slog.New(slog.NewTextHandler(&logs, nil)), func() {})
	g.grace, g.exit = 10*time.Millisecond, func(code int) { exited <- code }
	g.Now("the panic budget is spent")
	select {
	case code := <-exited:
		if code != 1 || !strings.Contains(logs.String(), "the shutdown did not finish in time") {
			panic(fmt.Sprintf("status %d\n%s", code, logs.String()))
		}
	case <-time.After(10 * time.Second):
		panic("the process was not ended")
	}
}
