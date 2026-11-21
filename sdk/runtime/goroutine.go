package runtime

import (
	"log"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/internal/gosafe"
)

// loopRestarts is how many times a background loop of the SDK is started
// again after a panic. Then it stays stopped: a loop that panics every time
// must not go on burning the host's CPU and filling its log.
const loopRestarts = 5

// loopBackoff is the wait before the first restart; it doubles up to
// loopMaxBackoff. Variables for the tests.
var (
	loopBackoff    = time.Second
	loopMaxBackoff = 30 * time.Second
)

// startLoop runs one of the SDK's background loops (the subscriber, the
// exporter, the delivery reporter). They run inside the host application, so
// a panic in one must not end it (the rule of core.Recover, for goroutines):
// the panic is logged, at most loopRestarts+1 times in the life of the loop,
// and the loop is started again with backoff. When the restarts are used up
// it stays stopped and the log says what no longer works; the host goes on
// untouched.
//
// stop is the loop's own stop signal, done runs when its goroutine ends.
// panicked (it may be nil) is told of every panic, before it is logged, so
// that a log that panics cannot keep it from being told; final says that the
// loop stays stopped.
//
// The log names the type of the panic value and, for a panic of the Go
// runtime, its text; any other value is not printed, since it may quote
// what an encoder or a transport of the host put into it. The stack has no
// arguments.
func startLoop(name, consequence string, stop <-chan struct{}, loop, done func(), panicked func(final bool)) {
	gosafe.Go(gosafe.Options{
		Name: name, Restarts: loopRestarts, Backoff: loopBackoff, MaxBackoff: loopMaxBackoff, Stop: stop, Done: done,
		OnPanic: func(p gosafe.Panic) {
			if panicked != nil {
				func() {
					defer func() { _ = recover() }() // the log below is still written
					panicked(p.RestartIn == 0)
				}()
			}
			what := p.Type
			if p.Runtime {
				what += ": " + p.Value
			}
			if p.RestartIn > 0 {
				log.Printf("tsession: error: the %s goroutine panicked (%s) and is restarted in %s (restart %d of %d); the application is not affected. "+
					"This is a bug in tsession, please report it with this stack:\n%s", name, what, p.RestartIn, p.N, loopRestarts, p.Stack)
				return
			}
			log.Printf("tsession: error: the %s goroutine panicked %d times (%s) and stays stopped: %s until the process is restarted; "+
				"the application is not affected. This is a bug in tsession, please report it with this stack:\n%s", name, p.N, what, consequence, p.Stack)
		},
	}, loop)
}
