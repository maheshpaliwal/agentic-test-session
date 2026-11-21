package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// ingestGate stands between the services and the collector's ingest: it can
// answer for a collector that is down, or one that is slow. The control
// plane talks to the collector directly, as it does when only the path from
// a service is in trouble.
type ingestGate struct {
	next  http.Handler
	down  atomic.Bool
	delay atomic.Int64 // nanoseconds, before the collector sees the request
	// lo and hi, when hi is set, delay every request by a time between them.
	lo, hi atomic.Int64
	// faults are what is wrong for the services that reach the collector
	// through the path /g/<id>/ only (see fault).
	faults sync.Map
}

// gateFault is what is wrong on the way from one service to the collector.
type gateFault struct {
	down   atomic.Bool
	status atomic.Int32 // answer every request with this status
}

// fault returns the faults of the services configured with path /g/<id>.
func (g *ingestGate) fault(id string) *gateFault {
	f, _ := g.faults.LoadOrStore(id, &gateFault{})
	return f.(*gateFault)
}

func (g *ingestGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/g/"); ok {
		id, path, _ := strings.Cut(rest, "/")
		r.URL.Path = "/" + path
		f := g.fault(id)
		if f.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		if code := f.status.Load(); code != 0 {
			http.Error(w, "refused", int(code))
			return
		}
	}
	if g.down.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	if d := time.Duration(g.delay.Load()); d > 0 {
		time.Sleep(d + time.Duration(rand.Int64N(int64(d)/4+1)))
	}
	if lo, hi := g.lo.Load(), g.hi.Load(); hi > 0 {
		time.Sleep(time.Duration(lo + rand.Int64N(hi-lo+1)))
	}
	g.next.ServeHTTP(w, r)
}

// deliveryEnv is the walkthrough that found the defect, with real code and
// real settings: a collector and a control plane (seconds are seconds, the
// drain's grace is the default 10s), and two processes of a service that
// adopted tsession by importing autoinit. "orders-api" handles a request and
// calls "inventory". It needs no Postgres or Redis.
type deliveryEnv struct {
	t        *testing.T
	gate     *ingestGate
	colURL   string // the collector, directly
	gateURL  string // the collector through the gate
	cpURL    string
	orders   string // address of orders-api
	tracectl string
	app      string // the deliveryapp binary
	procs    []*proc
}

// newDeliveryEnv starts everything. ordersCollector is the collector URL
// orders-api is configured with ("" = the right one); inventory always has
// the right one.
func newDeliveryEnv(t *testing.T, ordersCollector string) *deliveryEnv {
	t.Helper()
	e := newDeliveryBase(t)
	inventory := freeAddr(t)
	e.orders = freeAddr(t)
	e.start(e.app, "inventory", inventory, e.gateURL, "")
	if ordersCollector == "" {
		ordersCollector = e.gateURL
	}
	e.start(e.app, "orders-api", e.orders, ordersCollector, "http://"+inventory)
	return e
}

// newDeliveryBase starts the collector, its gate and the control plane, and
// builds the binaries. It starts no service.
func newDeliveryBase(t *testing.T) *deliveryEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	dir := t.TempDir()
	app := buildBinary(t, dir, "deliveryapp", "integration", "./cmd/deliveryapp")
	e := &deliveryEnv{t: t, app: app, tracectl: buildBinary(t, dir, "tracectl", ".", "./cmd/tracectl")}

	col := collector.New(store.NewMem(), collector.Config{Window: 100 * time.Millisecond})
	colSrv := httptest.NewServer(col.Handler()) // 127.0.0.1
	e.gate = &ingestGate{next: col.Handler()}
	gateSrv := httptest.NewServer(e.gate)
	mgr := controlplane.NewManager(controlplane.Config{
		Collector:    &controlplane.HTTPCollector{BaseURL: colSrv.URL},
		PollInterval: 100 * time.Millisecond, HeartbeatPeriod: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Run(ctx)
	cpSrv := httptest.NewServer(mgr.Handler())
	t.Cleanup(func() {
		cancel()
		cpSrv.Close()
		mgr.Close()
		gateSrv.Close()
		colSrv.Close()
		col.Close()
		// The connections to servers that are gone now: give their ports back.
		deliveryHTTP.CloseIdleConnections()
	})
	e.colURL, e.gateURL, e.cpURL = colSrv.URL, gateSrv.URL, cpSrv.URL
	return e
}

func (e *deliveryEnv) start(bin, service, addr, collectorURL, downstream string) {
	e.t.Helper()
	e.startVia(bin, service, addr, collectorURL, downstream, e.cpURL)
}

// startVia is start for a service that reaches the control plane at cpURL.
func (e *deliveryEnv) startVia(bin, service, addr, collectorURL, downstream, cpURL string) *proc {
	e.t.Helper()
	p := &proc{name: service, cmd: exec.Command(bin), out: &bytes.Buffer{}}
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	p.cmd.Env = append(os.Environ(), "TSESSION_ENABLED=true", "TSESSION_SERVICE="+service,
		"TSESSION_CONTROLPLANE_URL="+cpURL, "TSESSION_COLLECTOR_URL="+collectorURL,
		"DELIVERYAPP_LISTEN="+addr, "DELIVERYAPP_DOWNSTREAM_URL="+downstream)
	if err := p.cmd.Start(); err != nil {
		panic(fmt.Sprintf("start %s: %v", service, err))
	}
	e.t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
		if e.t.Failed() {
			e.t.Logf("%s output:\n%s", service, p.out)
		}
	})
	waitHealthy(e.t, p, addr)
	e.procs = append(e.procs, p)
	return p
}

// session starts a session and waits until both services were told of it.
func (e *deliveryEnv) session(ctx context.Context, name string) (*client.Session, error) {
	s, err := client.Start(ctx, client.Options{BaseURL: e.cpURL, CollectorURL: e.colURL, Name: name, HTTPClient: deliveryHTTP,
		QuiesceSeconds: 1, PollInterval: 50 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	for {
		n, err := s.WaitPropagated(ctx)
		if err != nil {
			return nil, err
		}
		if n >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond) // a service is still connecting
	}
	time.Sleep(100 * time.Millisecond) // "sent" is not yet "applied"
	return s, nil
}

// request sends the traced request: orders-api records the request it got and
// the one it made, inventory the one it got.
func (e *deliveryEnv) request(ctx context.Context, s *client.Session) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+e.orders+"/work", nil)
	if err != nil {
		return err
	}
	s.Inject(req.Header)
	resp, err := deliveryHTTP.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST /work: %s", resp.Status)
	}
	return nil
}

// eventsPerRequest is what one request records: api.in and api.out in
// orders-api, api.in in inventory.
const eventsPerRequest = 3

// run records one request in a new session, stops it at once (the events are
// still on their way) and returns how it ended.
func (e *deliveryEnv) run(ctx context.Context, name string) (*client.Session, client.Info, client.Summary, error) {
	s, err := e.session(ctx, name)
	if err != nil {
		return nil, client.Info{}, client.Summary{}, err
	}
	if err := e.request(ctx, s); err != nil {
		return s, client.Info{}, client.Summary{}, err
	}
	info, err := s.StopAndWait(ctx)
	if err != nil {
		return s, info, client.Summary{}, err
	}
	sum, err := s.Summary(ctx)
	return s, info, sum, err
}

// tracectl runs the CLI against this environment and returns its exit code
// and output.
func (e *deliveryEnv) runTracectl(args ...string) (int, string) {
	e.t.Helper()
	cmd := exec.Command(e.tracectl, args...)
	cmd.Env = append(os.Environ(), "TSESSION_CONTROLPLANE_URL="+e.cpURL, "TSESSION_COLLECTOR_URL="+e.colURL)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &ee):
		return ee.ExitCode(), string(out)
	}
	panic(fmt.Sprintf("tracectl %v: %v\n%s", args, err, out))

}

// complete fails the test unless the session ended complete, with every
// event of n requests in the collector and nothing flagged.
func complete(t *testing.T, name string, info client.Info, sum client.Summary, n int) {
	t.Helper()
	if info.State != "SEALED" || info.Incomplete || sum.Incomplete || len(info.IncompleteReasons) > 0 || len(info.DeliveryUnconfirmed) > 0 ||
		info.DrainTimedOut || info.SealError != "" {
		t.Errorf("%s: a healthy session was flagged: state=%s incomplete=%v/%v reasons=%q unconfirmed=%q delivery=%+v",
			name, info.State, info.Incomplete, sum.Incomplete, info.IncompleteReasons, info.DeliveryUnconfirmed, info.Delivery)
	}
	if sum.EventCount != n*eventsPerRequest {
		t.Errorf("%s: %d events in the collector, want %d (by service %v); delivery=%+v", name, sum.EventCount, n*eventsPerRequest, sum.ByService, info.Delivery)
	}
	if len(info.Delivery) != 2 {
		t.Errorf("%s: delivery reports of %d instances, want 2: %+v", name, len(info.Delivery), info.Delivery)
	}
	for _, d := range info.Delivery {
		if d.Buffered != 0 || d.Dropped != 0 || d.Recorded != d.Delivered || d.Recorded == 0 {
			t.Errorf("%s: %s reported %+v", name, d.Service, d)
		}
	}
}

// The walkthrough: orders-api is configured with a collector URL where
// nothing listens, inventory with the right one. Before delivery accounting
// the session sealed with inventory's event only and every gate passed. Now
// orders-api tells the control plane what it could not deliver, and the
// session ends incomplete with a reason that names it.
func TestDelivery_WrongCollectorURLFailsTheGates(t *testing.T) {
	e := newDeliveryEnv(t, "http://"+freeAddr(t)) // a closed port on 127.0.0.1
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	began := time.Now()
	s, info, sum, err := e.run(ctx, "wrong-collector-url")
	if err != nil {
		panic(fmt.Sprintf("run: %v (info %+v)", err, info))
	}
	t.Logf("sealed after %s: incomplete=%v reasons=%q", time.Since(began).Round(100*time.Millisecond), info.Incomplete, info.IncompleteReasons)

	// The collector has no idea: it got inventory's event and nothing looks wrong to it.
	if sum.Incomplete || sum.EventCount != 1 || sum.ByService["inventory"] != 1 {
		t.Errorf("collector summary: incomplete=%v events=%d by service %v; want a complete-looking session with inventory's event only",
			sum.Incomplete, sum.EventCount, sum.ByService)
	}
	if !info.Incomplete || info.DrainTimedOut || info.SealError != "" {
		panic(fmt.Sprintf("the session must end incomplete, by the delivery reports alone: %+v", info))
	}
	if len(info.IncompleteReasons) != 1 || !strings.Contains(info.IncompleteReasons[0], "service orders-api") ||
		!strings.Contains(info.IncompleteReasons[0], "0 of 2 recorded events reached the collector") ||
		!strings.Contains(info.IncompleteReasons[0], "connection refused") {
		t.Errorf("incomplete_reasons = %q; want one reason naming orders-api, its counts and the error", info.IncompleteReasons)
	}
	if strings.Contains(strings.Join(info.IncompleteReasons, " "), "inventory") || len(info.DeliveryUnconfirmed) > 0 {
		t.Errorf("inventory delivered everything and must not be named: reasons=%q unconfirmed=%q", info.IncompleteReasons, info.DeliveryUnconfirmed)
	}

	code, out := e.runTracectl("summary", s.ID, "--fail-on-incomplete")
	if code != 3 || !strings.Contains(out, "orders-api") || !strings.Contains(out, "session is incomplete") {
		t.Errorf("tracectl summary --fail-on-incomplete: exit %d, want 3 and the reason\n%s", code, out)
	}
	// The command of the walkthrough, which printed "Passed" and exited 0.
	code, out = e.runTracectl("report", s.ID, "--fail-on-error", "--fail-on-incomplete", "--fail-on-empty")
	if code != 3 || !strings.Contains(out, "orders-api") || strings.Contains(out, "**Passed**") {
		t.Errorf("tracectl report with every gate: exit %d, want 3 and the reason\n%s", code, out)
	}
}

// healthyRuns runs n sessions of one request each, a few at a time, and
// requires every one of them to end complete: a false "incomplete" on a
// healthy run is what makes a team turn the gate off.
func healthyRuns(t *testing.T, e *deliveryEnv, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)
	var mu sync.Mutex
	var first *client.Session
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			time.Sleep(time.Duration(rand.Int64N(int64(300 * time.Millisecond)))) // spread the sessions over the SDKs' timers
			name := fmt.Sprintf("healthy-%d", i)
			s, info, sum, err := e.run(ctx, name)
			if err != nil {
				t.Errorf("%s: %v", name, err)
				return
			}
			complete(t, name, info, sum, 1)
			mu.Lock()
			if first == nil {
				first = s
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if first != nil {
		if code, out := e.runTracectl("summary", first.ID, "--fail-on-incomplete", "--fail-on-empty"); code != 0 {
			t.Errorf("tracectl summary of a healthy session: exit %d\n%s", code, out)
		}
	}
}

// The same scenario with both URLs right, twenty times: complete every time.
func TestDelivery_HealthyRunsAreNeverFlagged(t *testing.T) {
	healthyRuns(t, newDeliveryEnv(t, ""), 20)
}

// A slow collector: every ingest takes longer than the session's quiet
// window (1s), though less than the SDK's request timeout (2s). The sessions
// are stopped while their events are in flight. They must not be flagged, and
// the drain must wait for the events instead of sealing without them.
func TestDelivery_SlowCollectorIsNotFlagged(t *testing.T) {
	e := newDeliveryEnv(t, "")
	e.gate.delay.Store(int64(1200 * time.Millisecond))
	healthyRuns(t, e, 20)
}

// The collector is down for the services when the session is stopped and
// comes back during the drain, well within its grace: the events arrive late
// and the session is complete.
func TestDelivery_CollectorBackDuringDrainIsNotFlagged(t *testing.T) {
	e := newDeliveryEnv(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i, outage := range []time.Duration{3 * time.Second, 6 * time.Second} {
		name := fmt.Sprintf("outage-%s", outage)
		s, err := e.session(ctx, name)
		if err != nil {
			panic(err)
		}
		e.gate.down.Store(true)
		if err := e.request(ctx, s); err != nil {
			panic(err)
		}
		time.AfterFunc(outage, func() { e.gate.down.Store(false) })
		info, err := s.StopAndWait(ctx)
		if err != nil {
			panic(fmt.Sprintf("%s: %v", name, err))
		}
		sum, err := s.Summary(ctx)
		if err != nil {
			panic(err)
		}
		complete(t, name, info, sum, 1)
		took := info.EndedAt.Sub(info.StartedAt)
		t.Logf("run %d: collector down for %s from the request on; sealed %s after the start", i, outage, took.Round(100*time.Millisecond))
		// The SDK retries every second during a drain: the events arrive soon
		// after the collector is back, not after the exporter's pause (5s).
		if took > outage+4*time.Second {
			t.Errorf("%s: sealed %s after the collector came back; the SDK should have noticed within about a second", name, took-outage)
		}
	}
}
