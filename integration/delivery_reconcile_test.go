package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The scenarios below are the ones an adversarial review of delivery
// accounting ran against the real binaries. Each is repeated: a false
// "incomplete" that shows up once in fifty runs is still what makes a team
// turn the gate off. TSESSION_DELIVERY_RUNS sets how often (default 4, which
// keeps the suite short; the numbers in the changelog are from 40 and more).
func deliveryRuns() int {
	if n, err := strconv.Atoi(os.Getenv("TSESSION_DELIVERY_RUNS")); err == nil && n > 0 {
		return n
	}
	return 4
}

// deliveryHTTP is the HTTP client of the scenarios' own requests. It keeps
// its connections: hundreds of sessions that poll at once would otherwise
// open one per request and run the machine out of local ports.
var deliveryHTTP = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConns: 4096, MaxIdleConnsPerHost: 1024}}

// deliveryRun is how one session of a scenario ended.
type deliveryRun struct {
	name   string
	info   client.Info
	expect int // events the services recorded for it
	got    int // events the collector has
	drain  time.Duration
	err    error
}

// short: events are missing from the collector.
func (r deliveryRun) short() bool { return r.got < r.expect }

// falseIncomplete: flagged although the collector has every event.
func (r deliveryRun) falseIncomplete() bool { return r.info.Incomplete && !r.short() }

// miss: events are missing and the session says nothing.
func (r deliveryRun) miss() bool { return r.short() && !r.info.Incomplete }

func (r deliveryRun) String() string {
	return fmt.Sprintf("%s: state=%s incomplete=%v timed_out=%v events %d of %d, drain %s, reasons=%q unconfirmed=%q delivery=%+v err=%v",
		r.name, r.info.State, r.info.Incomplete, r.info.DrainTimedOut, r.got, r.expect, r.drain.Round(10*time.Millisecond),
		r.info.IncompleteReasons, r.info.DeliveryUnconfirmed, r.info.Delivery, r.err)
}

// repeat runs a scenario n times, par at a time, and logs what came of it.
func repeat(t *testing.T, scenario string, n, par int, run func(i int) deliveryRun) []deliveryRun {
	t.Helper()
	out := make([]deliveryRun, n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, par)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			time.Sleep(time.Duration(rand.Int64N(int64(300 * time.Millisecond)))) // spread the sessions over the SDKs' timers
			out[i] = run(i)
			out[i].name = fmt.Sprintf("%s-%d", scenario, i)
		}()
	}
	wg.Wait()
	var inc, fp, miss, unc, errs int
	drains := make([]time.Duration, 0, n)
	for _, r := range out {
		drains = append(drains, r.drain)
		if r.err != nil || r.info.State != "SEALED" {
			errs++
			t.Errorf("%s", r)
		}
		if r.info.Incomplete {
			inc++
		}
		if r.falseIncomplete() {
			fp++
		}
		if r.miss() {
			miss++
		}
		if len(r.info.DeliveryUnconfirmed) > 0 {
			unc++
		}
	}
	slices.Sort(drains)
	t.Logf("%-18s N=%d incomplete=%d (false=%d) miss=%d unconfirmed=%d errors=%d drain p50=%s p95=%s max=%s", scenario, n, inc, fp, miss, unc, errs,
		drains[n/2].Round(10*time.Millisecond), drains[min(n-1, n*95/100)].Round(10*time.Millisecond), drains[n-1].Round(10*time.Millisecond))
	return out
}

// svcApp is one deliveryapp process of a scenario.
type svcApp struct {
	*proc
	addr   string
	gateID string // its own way to the collector, see ingestGate.fault
}

var svcSeq atomic.Int64

type svcOpts struct {
	collector string // "" = through the gate, on a path of its own
	cp        string // "" = the control plane, directly
}

// startSvc starts a deliveryapp. It does not wait for it to follow the
// control plane: see connected.
func (e *deliveryEnv) startSvc(service string, o svcOpts) *svcApp {
	e.t.Helper()
	a := &svcApp{addr: freeAddr(e.t), gateID: fmt.Sprintf("a%d", svcSeq.Add(1))}
	col := e.gateURL + "/g/" + a.gateID
	if o.collector != "" {
		col = o.collector
	}
	cp := e.cpURL
	if o.cp != "" {
		cp = o.cp
	}
	a.proc = e.startVia(e.app, service, a.addr, col, "", cp)
	return a
}

// kill ends the process the way an orchestrator ends a pod that does not
// handle the signal: no flush, no last report.
func (a *svcApp) kill() {
	_ = a.cmd.Process.Kill()
	_ = a.cmd.Wait()
}

// connected waits until a service of that name follows the control plane.
func (e *deliveryEnv) connected(service string) {
	e.t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		subs, err := client.Subscribers(context.Background(), client.Options{BaseURL: e.cpURL})
		if err == nil && slices.ContainsFunc(subs, func(s client.Subscriber) bool { return s.Service == service }) {
			return
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("service %s never followed the control plane: %v", service, err))
		}
	}
}

// begin starts a session and gives the connected services the time to learn
// of it.
func (e *deliveryEnv) begin(maxDrain int) (*client.Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := client.Start(ctx, client.Options{BaseURL: e.cpURL, CollectorURL: e.colURL, Name: "s", HTTPClient: deliveryHTTP,
		QuiesceSeconds: 1, MaxDrainSeconds: maxDrain, PollInterval: 50 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	if _, err := s.WaitPropagated(ctx); err != nil {
		return nil, err
	}
	time.Sleep(150 * time.Millisecond) // "sent" is not yet "applied"
	return s, nil
}

// burst makes a service record n+1 events in the session: the request and n
// more.
func (a *svcApp) burst(s *client.Session, n int) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/burst?n=%d", a.addr, n), nil)
	if err != nil {
		return err
	}
	s.Inject(req.Header)
	resp, err := deliveryHTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST /burst: %s", resp.Status)
	}
	return nil
}

// finish stops the session and reports how it ended.
func (e *deliveryEnv) finish(s *client.Session, expect int) deliveryRun {
	r := deliveryRun{expect: expect}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	began := time.Now()
	r.info, r.err = s.StopAndWait(ctx)
	r.drain = time.Since(began)
	if sum, err := s.Summary(ctx); err != nil {
		if r.err == nil {
			r.err = err
		}
	} else {
		r.got = sum.EventCount
	}
	return r
}

// failed is a run that could not be set up.
func failed(err error) deliveryRun { return deliveryRun{err: err} }

// cpProxy is the control plane as a service sees it through something that
// refuses some requests: block returns the status to answer with, 0 to pass
// the request on.
func (e *deliveryEnv) cpProxy(block func(r *http.Request) int) string {
	target, err := url.Parse(e.cpURL)
	if err != nil {
		panic(err)
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1 // the active-set stream
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := block(r); code != 0 {
			http.Error(w, "<html>blocked by policy</html>", code)
			return
		}
		r.Host = target.Host
		rp.ServeHTTP(w, r)
	}))
	e.t.Cleanup(srv.Close)
	return srv.URL
}

// wantNoFalseVerdict fails the test for every run that was flagged with all
// its events in the collector, and for every run that lost events silently.
func wantNoFalseVerdict(t *testing.T, runs []deliveryRun) {
	t.Helper()
	for _, r := range runs {
		if r.falseIncomplete() {
			t.Errorf("flagged although the collector has every event: %s", r)
		}
		if r.miss() {
			t.Errorf("events are missing and the session does not say so: %s", r)
		}
	}
}

// wantAllComplete fails the test for every run that is not complete with
// every event in the collector and nothing flagged.
func wantAllComplete(t *testing.T, runs []deliveryRun) {
	t.Helper()
	for _, r := range runs {
		if r.info.Incomplete || r.short() || len(r.info.IncompleteReasons) > 0 || len(r.info.DeliveryUnconfirmed) > 0 || r.info.DrainTimedOut {
			t.Errorf("a session that lost nothing was flagged, or lost something: %s", r)
		}
	}
}

// F1: the collector is down for a service for 1.3s (a restart), the service
// then delivers everything, and is killed before its next periodic report.
// The control plane's last word from it is "0 delivered, HTTP 503". The
// collector has every event: the session is complete, and the drain does not
// wait the grace for an instance that owes nothing.
func TestDelivery_BlipThenKillIsNotFlagged(t *testing.T) {
	e := newDeliveryBase(t)
	runs := repeat(t, "blip-then-kill", deliveryRuns(), 4, func(int) deliveryRun {
		svc := fmt.Sprintf("svc-%d", svcSeq.Add(1))
		a := e.startSvc(svc, svcOpts{})
		e.connected(svc)
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		f := e.gate.fault(a.gateID)
		f.down.Store(true)
		time.AfterFunc(1300*time.Millisecond, func() { f.down.Store(false) })
		if err := a.burst(s, 20); err != nil {
			return failed(err)
		}
		time.Sleep(4 * time.Second) // reported while failing, then delivered
		a.kill()
		return e.finish(s, 21)
	})
	wantAllComplete(t, runs)
	stale := 0
	for _, r := range runs {
		// Nearly every run: now and then the first report is sent only after
		// the collector is back, and then there is nothing stale about it.
		if len(r.info.Delivery) == 1 && !r.info.Delivery[0].Final && r.info.Delivery[0].Delivered == 0 && r.info.Delivery[0].LastError != "" {
			stale++
		}
		if r.drain > 6*time.Second {
			t.Errorf("the drain waited for an instance that owed nothing: %s", r)
		}
	}
	if stale*2 < len(runs) {
		t.Errorf("test setup: only %d of %d runs ended on the stale, failing report this test is about", stale, len(runs))
	}
}

// F2: every ingest takes longer than the SDK waits for the answer (2s), so
// the collector stores a batch and the SDK never learns of it; and a
// collector whose latency is anywhere between 0.5s and 3s. A session whose
// events the collector has is complete whatever its services believe; one
// that is flagged really lacks events.
func TestDelivery_SlowCollectorIsNeverFalselyFlagged(t *testing.T) {
	for _, lat := range []struct {
		name   string
		lo, hi time.Duration
	}{{"2.3s-2.7s", 2300 * time.Millisecond, 2700 * time.Millisecond}, {"0.5s-3s", 500 * time.Millisecond, 3 * time.Second}} {
		t.Run(lat.name, func(t *testing.T) {
			t.Parallel()
			e := newDeliveryBase(t)
			apps := []*svcApp{e.startSvc("orders-api", svcOpts{}), e.startSvc("inventory", svcOpts{})}
			e.connected("orders-api")
			e.connected("inventory")
			e.gate.lo.Store(int64(lat.lo))
			e.gate.hi.Store(int64(lat.hi))
			runs := repeat(t, "slow-"+lat.name, deliveryRuns(), 8, func(int) deliveryRun {
				s, err := e.begin(0)
				if err != nil {
					return failed(err)
				}
				for _, a := range apps {
					if err := a.burst(s, 5); err != nil {
						return failed(err)
					}
				}
				return e.finish(s, 12)
			})
			wantNoFalseVerdict(t, runs)
		})
	}
}

// F3: twelve sessions of 601 events each in one service, stopped together,
// and a collector that takes 1.0s to 1.2s per request. A session's events
// wait behind those of the others for much longer than the grace. None is
// sealed while the service is working through the backlog.
func TestDelivery_BacklogOfManySessionsIsNotFlagged(t *testing.T) {
	e := newDeliveryBase(t)
	a := e.startSvc("orders-api", svcOpts{})
	e.connected("orders-api")
	e.gate.lo.Store(int64(time.Second))
	e.gate.hi.Store(int64(1200 * time.Millisecond))
	runs := repeat(t, "backlog-12x601", max(12, deliveryRuns()), 12, func(int) deliveryRun {
		s, err := e.begin(600)
		if err != nil {
			return failed(err)
		}
		if err := a.burst(s, 600); err != nil {
			return failed(err)
		}
		return e.finish(s, 601)
	})
	wantAllComplete(t, runs)
}

// The cost of not taking silence for evidence, pinned so that a change to it
// changes the docs: a service whose collector URL is wrong AND whose
// delivery reports are refused on the way (a proxy that lets GET through and
// answers 403 to POST). Nothing of it reaches anybody: no event at the
// collector, no report at the control plane. The control plane knows that
// the service follows it and that none of its reports ever arrived, and
// cannot know whether it took part in this session; an idle service behind
// the same proxy looks the same. So the session is not incomplete. It says
// so under delivery_unconfirmed, the drain is not held, the service's log
// says what is wrong, once, and a gate that asks for it fails:
// --fail-on-unconfirmed, tsessiontest.RequireDeliveryConfirmed.
//
// With a collector that the service reaches, the session is incomplete: see
// TestDelivery_RefusedReportsOfAServiceThatTookPart.
//
// The same with a proxy that answers 404 (the third review's F4: one that
// has no route for the POST). The control plane says in the active set that
// it takes reports, so the SDK does not take the 404 for "a control plane
// without the endpoint" and go quiet: such a session was complete, without a
// line anywhere.
func TestDelivery_RefusedReportsAndAWrongCollectorAreUnconfirmed(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprintf("HTTP %d", code), func(t *testing.T) {
			t.Parallel()
			refusedReportsAndAWrongCollector(t, code)
		})
	}
}

func refusedReportsAndAWrongCollector(t *testing.T, code int) {
	e := newDeliveryBase(t)
	blocked := e.cpProxy(func(r *http.Request) int {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delivery") {
			return code
		}
		return 0
	})
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{collector: "http://" + freeAddr(t), cp: blocked})
	e.connected("inventory")
	e.connected("orders-api")
	runs := repeat(t, fmt.Sprintf("report-%d-wrong-url", code), deliveryRuns(), 8, func(int) deliveryRun {
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		for _, a := range []*svcApp{inventory, orders} {
			if err := a.burst(s, 5); err != nil {
				return failed(err)
			}
		}
		return e.finish(s, 12)
	})
	for _, r := range runs {
		if r.info.Incomplete || len(r.info.IncompleteReasons) != 0 || r.got != 6 || len(r.info.DeliveryUnconfirmed) != 1 ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "service orders-api (instance ") ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "no delivery report of it has arrived since it connected (its log says why); the collector has no events from it for this session") ||
			strings.Contains(r.info.DeliveryUnconfirmed[0], "inventory") {
			t.Errorf("the documented limit no longer holds as documented: %s", r)
		}
		if r.drain > 5*time.Second {
			t.Errorf("the drain was held for an instance nothing is known of: %s", r)
		}
	}
	// The strict gate fails on it, the default one does not.
	id := runs[0].info.ID
	if code, out := e.runTracectl("summary", id, "--fail-on-incomplete", "--fail-on-empty"); code != 0 {
		t.Errorf("summary --fail-on-incomplete: exit %d\n%s", code, out)
	}
	if code, out := e.runTracectl("summary", id, "--fail-on-unconfirmed"); code != 3 || !strings.Contains(out, "check failed: delivery is unconfirmed (service orders-api") {
		t.Errorf("summary --fail-on-unconfirmed: exit %d\n%s", code, out)
	}
	if code, out := e.runTracectl("doctor"); code != 0 || !strings.Contains(out, "NO DELIVERY REPORTS") {
		t.Errorf("doctor does not name the service whose reports do not arrive: exit %d\n%s", code, out)
	}
	orders.kill()
	log := orders.out.String()
	if n := strings.Count(log, "cannot send delivery reports to the control plane"); n != 1 || !strings.Contains(log, fmt.Sprintf("HTTP %d", code)) || strings.Contains(log, "blocked by policy") {
		t.Errorf("the service's log should say once that its reports are refused, without the answer's body (%d lines):\n%s", n, log)
	}
}

// F5: a service that cannot open the stream (it polls the active set) and
// cannot reach the collector. It learns of a drain late or never, so it
// reports what it holds at once; the session is incomplete.
func TestDelivery_PollingServiceIsNotAMiss(t *testing.T) {
	e := newDeliveryBase(t)
	noStream := e.cpProxy(func(r *http.Request) int {
		if strings.HasSuffix(r.URL.Path, "/stream") {
			return http.StatusBadGateway
		}
		return 0
	})
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{collector: "http://" + freeAddr(t), cp: noStream})
	e.connected("inventory")
	runs := repeat(t, "poll-only", deliveryRuns(), 8, func(int) deliveryRun {
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		time.Sleep(6 * time.Second) // the polling service learns of the session
		for _, a := range []*svcApp{inventory, orders} {
			if err := a.burst(s, 5); err != nil {
				return failed(err)
			}
		}
		return e.finish(s, 12)
	})
	wantNoFalseVerdict(t, runs)
	for _, r := range runs {
		if !r.info.Incomplete || len(r.info.IncompleteReasons) != 1 || !strings.Contains(r.info.IncompleteReasons[0], "service orders-api (instance ") ||
			!strings.Contains(r.info.IncompleteReasons[0], "0 of 6 recorded events reached the collector") {
			t.Errorf("want the session incomplete for orders-api: %s", r)
		}
	}
}

// F5: a service is killed while it holds events it could not deliver.
func TestDelivery_KilledHoldingEvents(t *testing.T) {
	e := newDeliveryBase(t)
	sigkill := func(hold time.Duration) func(int) deliveryRun {
		return func(int) deliveryRun {
			svc := fmt.Sprintf("svc-%d", svcSeq.Add(1))
			a := e.startSvc(svc, svcOpts{})
			e.connected(svc)
			s, err := e.begin(0)
			if err != nil {
				return failed(err)
			}
			if err := a.burst(s, 1); err != nil {
				return failed(err)
			}
			time.Sleep(1600 * time.Millisecond) // delivered and reported
			e.gate.fault(a.gateID).down.Store(true)
			if err := a.burst(s, 500); err != nil {
				return failed(err)
			}
			time.Sleep(hold)
			a.kill()
			return e.finish(s, 503)
		}
	}
	// It lived long enough to report the events it held: the collector has
	// fewer than it said it recorded, and it is gone. Incomplete.
	t.Run("after it reported them", func(t *testing.T) {
		t.Parallel()
		runs := repeat(t, "kill-reported", deliveryRuns(), 6, sigkill(5600*time.Millisecond))
		wantNoFalseVerdict(t, runs)
		for _, r := range runs {
			if !r.info.Incomplete || len(r.info.IncompleteReasons) != 1 || !strings.Contains(r.info.IncompleteReasons[0], "2 of 503 recorded events reached the collector") ||
				!strings.Contains(r.info.IncompleteReasons[0], "stopped reporting") {
				t.Errorf("want the session incomplete: %s", r)
			}
		}
	})
	// The limit, stated in docs/PROTOCOL.md: it is killed before the report
	// that would have said so. Its last report was in order and agrees with
	// the collector; no count shows the 501 events. This test pins what
	// happens, so that a change that makes it detectable updates the docs.
	t.Run("before it reported them: not detectable", func(t *testing.T) {
		t.Parallel()
		runs := repeat(t, "kill-unreported", deliveryRuns(), 6, sigkill(300*time.Millisecond))
		for _, r := range runs {
			if r.info.Incomplete || len(r.info.DeliveryUnconfirmed) != 0 || r.got != 2 {
				t.Errorf("the known limit no longer holds as documented: %s", r)
			}
		}
	})
}

// A collector that refuses a service's events (HTTP 400: a wrong URL that
// reaches some other server) drops them at once; that is never a miss.
func TestDelivery_RejectedEventsAreNotAMiss(t *testing.T) {
	e := newDeliveryBase(t)
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{})
	e.gate.fault(orders.gateID).status.Store(http.StatusBadRequest)
	e.connected("inventory")
	e.connected("orders-api")
	runs := repeat(t, "reject-400", deliveryRuns(), 8, func(int) deliveryRun {
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		for _, a := range []*svcApp{inventory, orders} {
			if err := a.burst(s, 5); err != nil {
				return failed(err)
			}
		}
		return e.finish(s, 12)
	})
	wantNoFalseVerdict(t, runs)
	for _, r := range runs {
		if !r.info.Incomplete || len(r.info.IncompleteReasons) != 1 || !strings.Contains(r.info.IncompleteReasons[0], "service orders-api") ||
			!strings.Contains(r.info.IncompleteReasons[0], "0 of 6 recorded events reached the collector, 6 dropped by the SDK") {
			t.Errorf("want the session incomplete for orders-api: %s", r)
		}
	}
}

// The healthy case, repeated: two services, nothing wrong. Never flagged,
// and the drain takes what it always took.
func TestDelivery_HealthyRunsRepeated(t *testing.T) {
	e := newDeliveryBase(t)
	apps := []*svcApp{e.startSvc("orders-api", svcOpts{}), e.startSvc("inventory", svcOpts{}), e.startSvc("idle", svcOpts{})}
	for _, svc := range []string{"orders-api", "inventory", "idle"} {
		e.connected(svc)
	}
	runs := repeat(t, "healthy", max(8, deliveryRuns()), 16, func(int) deliveryRun {
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		for _, a := range apps[:2] {
			if err := a.burst(s, 5); err != nil {
				return failed(err)
			}
		}
		return e.finish(s, 12)
	})
	wantAllComplete(t, runs)
	for _, r := range runs {
		// The service that recorded nothing sent no report, and is not listed.
		if len(r.info.Delivery) != 2 || r.drain > 5*time.Second {
			t.Errorf("healthy run: %s", r)
		}
	}
}
