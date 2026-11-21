//go:build unix

package integration

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The scenarios of the second review of delivery accounting, with the real
// binaries. They are the ones in which a healthy session was marked
// incomplete: an environment has many connected services that take no part
// in a session, and anything that kept one of them from saying so in time
// (a pause, a dead connection, a slow or briefly unavailable report
// endpoint, many sessions ending at once) failed the session. The bar for
// every one of them is zero sessions marked incomplete.
//
// TSESSION_DELIVERY_RUNS sets how often a scenario runs (default 4; the
// numbers in the changelog are from 40 and more).

// freezer is a TCP proxy to the control plane that can stop forwarding while
// it keeps every connection open: a half-open connection, as the control
// plane sees it. Nothing arrives, nothing is closed.
type freezer struct {
	l      net.Listener
	frozen atomic.Bool
}

func newFreezer(t *testing.T, target string) *freezer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	f := &freezer{l: l}
	t.Cleanup(func() { f.frozen.Store(false); l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				up, err := net.Dial("tcp", target)
				if err != nil {
					c.Close()
					return
				}
				pipe := func(dst, src net.Conn) {
					buf := make([]byte, 32<<10)
					for {
						n, err := src.Read(buf)
						for f.frozen.Load() {
							time.Sleep(20 * time.Millisecond) // hold: neither forward nor close
						}
						if n > 0 {
							_, _ = dst.Write(buf[:n])
						}
						if err != nil {
							dst.Close()
							return
						}
					}
				}
				go pipe(up, c)
				pipe(c, up)
			}()
		}
	}()
	return f
}

func (f *freezer) url() string { return "http://" + f.l.Addr().String() }

// heard waits until a delivery report of a service has reached the control
// plane: the service sends one when it has opened the stream.
func (e *deliveryEnv) heard(service string) {
	e.t.Helper()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		subs, err := client.Subscribers(e.t.Context(), client.Options{BaseURL: e.cpURL})
		if err == nil && slices.ContainsFunc(subs, func(s client.Subscriber) bool { return s.Service == service && !s.LastDeliveryReport.IsZero() }) {
			return
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("no delivery report of service %s reached the control plane: %v", service, err))
		}
	}
}

// two starts the two services every scenario records with.
func (e *deliveryEnv) two(o svcOpts) []*svcApp {
	e.t.Helper()
	apps := []*svcApp{e.startSvc("orders-api", o), e.startSvc("inventory", o)}
	e.connected("orders-api")
	e.connected("inventory")
	return apps
}

// record starts a session and makes each of apps record six events in it.
func (e *deliveryEnv) record(apps []*svcApp, maxDrain int) (*client.Session, int, error) {
	s, err := e.begin(maxDrain)
	if err != nil {
		return nil, 0, err
	}
	for _, a := range apps {
		if err := a.burst(s, 5); err != nil {
			return nil, 0, err
		}
	}
	return s, 6 * len(apps), nil
}

// wantUntouched fails the test for every run that is not complete, with
// every event and without a word about its delivery, or whose drain took
// longer than a healthy one does.
func wantUntouched(t *testing.T, runs []deliveryRun, maxDrain time.Duration) {
	t.Helper()
	wantAllComplete(t, runs)
	for _, r := range runs {
		if r.drain > maxDrain {
			t.Errorf("the drain was held for %v (at most %v is a healthy one): %s", r.drain.Round(10*time.Millisecond), maxDrain, r)
		}
	}
}

// F1: an idle instance is frozen for 12 seconds (SIGSTOP: a debugger, a
// container paused for a snapshot, a host that swaps). It took no part in
// the session, so the session is not held for it and not marked. It used to
// be incomplete in 50 of 50 runs, after a drain of 10 seconds.
func TestDelivery_FrozenIdleInstanceDoesNotFailSessions(t *testing.T) {
	e := newDeliveryBase(t)
	apps := e.two(svcOpts{})
	const frozenFor = 12 * time.Second
	runs := repeat(t, "sigstop-12s", deliveryRuns(), 8, func(int) deliveryRun {
		svc := fmt.Sprintf("idle-%d", svcSeq.Add(1))
		idle := e.startSvc(svc, svcOpts{})
		defer idle.kill()
		e.heard(svc)
		s, n, err := e.record(apps, 0)
		if err != nil {
			return failed(err)
		}
		began := time.Now()
		if err := idle.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
			return failed(err)
		}
		time.Sleep(100 * time.Millisecond)
		r := e.finish(s, n)
		time.Sleep(time.Until(began.Add(frozenFor))) // it stays frozen for all of the 12s
		_ = idle.cmd.Process.Signal(syscall.SIGCONT)
		return r
	})
	wantUntouched(t, runs, 5*time.Second)
}

// F1: the stream of an idle instance is half-open (a NAT that forgot the
// connection, a peer that vanished without a FIN). The control plane still
// lists the instance as connected, and none of its reports can arrive.
func TestDelivery_HalfOpenStreamOfAnIdleInstanceDoesNotFailSessions(t *testing.T) {
	e := newDeliveryBase(t)
	apps := e.two(svcOpts{})
	runs := repeat(t, "half-open", deliveryRuns(), 8, func(int) deliveryRun {
		svc := fmt.Sprintf("idle-%d", svcSeq.Add(1))
		fz := newFreezer(t, strings.TrimPrefix(e.cpURL, "http://"))
		idle := e.startSvc(svc, svcOpts{cp: fz.url()})
		defer idle.kill()
		e.heard(svc)
		s, n, err := e.record(apps, 0)
		if err != nil {
			return failed(err)
		}
		fz.frozen.Store(true)
		r := e.finish(s, n)
		if subs, err := client.Subscribers(t.Context(), client.Options{BaseURL: e.cpURL}); err != nil ||
			!slices.ContainsFunc(subs, func(sub client.Subscriber) bool { return sub.Service == svc }) {
			r.err = fmt.Errorf("test setup: the control plane no longer lists %s as connected (%v)", svc, err)
		}
		return r
	})
	wantUntouched(t, runs, 5*time.Second)
}

// reportProxy is the control plane as the services of a scenario reach it,
// with something wrong on the way of their delivery reports only: every
// report takes lo..hi longer, or is answered 503 while down is set. It
// counts the reports that got through.
type reportProxy struct {
	url    string
	lo, hi time.Duration
	down   atomic.Bool
	posts  atomic.Int64
}

func (e *deliveryEnv) reportProxy(lo, hi time.Duration) *reportProxy {
	p := &reportProxy{lo: lo, hi: hi}
	p.url = e.cpProxy(func(r *http.Request) int {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/delivery") {
			return 0
		}
		if p.down.Load() {
			_, _ = io.Copy(io.Discard, r.Body)
			return http.StatusServiceUnavailable
		}
		p.posts.Add(1)
		if p.hi > 0 {
			time.Sleep(p.lo + time.Duration(rand.Int64N(int64(p.hi-p.lo)+1)))
		}
		return 0
	})
	return p
}

// idleInstances starts n services that take part in no session.
func (e *deliveryEnv) idleInstances(n int, o svcOpts) {
	e.t.Helper()
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.startSvc(fmt.Sprintf("idle-%d", i), o)
		}()
	}
	wg.Wait()
	for i := range n {
		e.connected(fmt.Sprintf("idle-%d", i))
	}
}

func envInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return def
}

// F2: many sessions that stop together, in an environment with twenty
// services that take no part, and a round trip of 25ms or 50ms to the
// control plane's report endpoint. With a request per session and service
// the reports did not get in within the grace: 141 of 500 sessions were
// marked incomplete at 25ms and 331 at 50ms. Now a service sends the reports
// of all its sessions in a few requests, and the idle ones send none.
//
// TSESSION_DELIVERY_BULK sets the sessions per round (default 500) and
// TSESSION_DELIVERY_ROUNDS the rounds (default 1).
func TestDelivery_ManySessionsStoppingTogether(t *testing.T) {
	sessions, rounds := envInt("TSESSION_DELIVERY_BULK", 500), envInt("TSESSION_DELIVERY_ROUNDS", 1)
	for _, lat := range []time.Duration{25 * time.Millisecond, 50 * time.Millisecond} {
		t.Run(lat.String(), func(t *testing.T) {
			e := newDeliveryBase(t)
			px := e.reportProxy(lat, lat)
			apps := e.two(svcOpts{cp: px.url})
			e.idleInstances(20, svcOpts{cp: px.url})
			for round := range rounds {
				var ready sync.WaitGroup
				ready.Add(sessions)
				release := make(chan struct{})
				go func() { ready.Wait(); close(release) }()
				before := px.posts.Load()
				runs := repeat(t, fmt.Sprintf("bulk-%d-%s", sessions, lat), sessions, sessions, func(int) deliveryRun {
					s, n, err := e.record(apps, 0)
					ready.Done()
					if err != nil {
						return failed(err)
					}
					<-release // all of them at once
					return e.finish(s, n)
				})
				// Well under the grace: nothing was waited for until it ran out.
				wantUntouched(t, runs, 8*time.Second)
				t.Logf("round %d: %d report requests for %d sessions of 2 services, 20 more services connected", round+1, px.posts.Load()-before, sessions)
			}
		})
	}
}

// F2: the report endpoint takes a second, or one to three, per request (a
// control plane under load, a slow proxy), with sixteen sessions at a time.
// 54 of 64 sessions were marked incomplete. The grace now counts from the
// last report of an instance that arrived, and a request carries the
// reports of every session.
func TestDelivery_SlowReportEndpointDoesNotFailSessions(t *testing.T) {
	for _, lat := range []struct {
		name   string
		lo, hi time.Duration
	}{{"1s", time.Second, time.Second}, {"1s-3s", time.Second, 3 * time.Second}} {
		t.Run(lat.name, func(t *testing.T) {
			t.Parallel()
			e := newDeliveryBase(t)
			px := e.reportProxy(lat.lo, lat.hi)
			apps := e.two(svcOpts{cp: px.url})
			e.idleInstances(2, svcOpts{cp: px.url})
			runs := repeat(t, "report-slow-"+lat.name, max(16, deliveryRuns()), 16, func(int) deliveryRun {
				s, n, err := e.record(apps, 0)
				if err != nil {
					return failed(err)
				}
				return e.finish(s, n)
			})
			wantAllComplete(t, runs)
		})
	}
}

// F2: the report endpoint is down for 3, 6 or 9 seconds, from just before
// the sessions stop (a control plane behind a proxy that restarts). The
// collector has every event; the reports arrive when the endpoint is back,
// within the grace. 47 of 50 sessions were marked incomplete after 9
// seconds: the SDK had backed off to a retry every 4 seconds. While the
// report of a draining session waits it now retries every 250ms.
func TestDelivery_ReportEndpointOutageWithinTheGraceDoesNotFailSessions(t *testing.T) {
	for _, down := range []time.Duration{3 * time.Second, 6 * time.Second, 9 * time.Second} {
		t.Run(down.String(), func(t *testing.T) {
			t.Parallel()
			e := newDeliveryBase(t)
			px := e.reportProxy(0, 0)
			apps := e.two(svcOpts{cp: px.url})
			e.idleInstances(2, svcOpts{cp: px.url})
			const perOutage = 4
			for range (deliveryRuns() + perOutage - 1) / perOutage {
				var ready sync.WaitGroup
				ready.Add(perOutage)
				release := make(chan struct{})
				go func() {
					ready.Wait()
					px.down.Store(true)
					time.AfterFunc(down, func() { px.down.Store(false) })
					close(release)
				}()
				runs := repeat(t, "report-down-"+down.String(), perOutage, perOutage, func(int) deliveryRun {
					s, n, err := e.record(apps, 0)
					ready.Done()
					if err != nil {
						return failed(err)
					}
					<-release
					return e.finish(s, n)
				})
				wantAllComplete(t, runs)
				time.Sleep(1500 * time.Millisecond) // the services have noticed that the endpoint is back
			}
		})
	}
}

// Something between a service and the control plane refuses its delivery
// reports (a proxy that lets GET through and answers 403 to POST), and the
// service delivers its events to the collector. The collector has events of
// an instance that follows the control plane, and no report of it arrives:
// the session is incomplete, naming the service. Nothing says whether what
// the collector has is all the instance recorded.
func TestDelivery_RefusedReportsOfAServiceThatTookPart(t *testing.T) {
	e := newDeliveryBase(t)
	blocked := e.cpProxy(func(r *http.Request) int {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delivery") {
			return http.StatusForbidden
		}
		return 0
	})
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{cp: blocked})
	e.idleInstances(1, svcOpts{cp: blocked}) // blocked as well, and it takes no part: not named
	e.connected("inventory")
	e.connected("orders-api")
	runs := repeat(t, "report-403-delivers", deliveryRuns(), 8, func(int) deliveryRun {
		s, n, err := e.record([]*svcApp{inventory, orders}, 0)
		if err != nil {
			return failed(err)
		}
		return e.finish(s, n)
	})
	for _, r := range runs {
		if r.short() {
			t.Errorf("test setup: the collector should have every event: %s", r)
		}
		if !r.info.Incomplete || len(r.info.IncompleteReasons) != 1 || !strings.Contains(r.info.IncompleteReasons[0], "service orders-api (instance ") ||
			!strings.Contains(r.info.IncompleteReasons[0], "the collector has 6 events from it, but none of its delivery reports for the session arrived") {
			t.Errorf("want the session incomplete for orders-api: %s", r)
		}
		if len(r.info.DeliveryUnconfirmed) != 1 || !strings.Contains(r.info.DeliveryUnconfirmed[0], "service idle-0 (instance ") ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "no delivery report of it has arrived since it connected") {
			t.Errorf("want the idle, blocked service under delivery_unconfirmed only: %s", r)
		}
	}
}

// The review's F5: a service whose name ends in a space (a YAML value, an
// env file). HTTP trims the name on the stream's header; the reports and
// the events carried it as written, so the control plane saw one instance
// that followed it and never reported, and another that reported: every
// session was marked incomplete.
func TestDelivery_ServiceNameWithATrailingSpace(t *testing.T) {
	e := newDeliveryBase(t)
	a := e.startSvc("orders-api ", svcOpts{})
	e.connected("orders-api")
	runs := repeat(t, "name-with-space", deliveryRuns(), 8, func(int) deliveryRun {
		s, n, err := e.record([]*svcApp{a}, 0)
		if err != nil {
			return failed(err)
		}
		return e.finish(s, n)
	})
	wantUntouched(t, runs, 5*time.Second)
	for _, r := range runs {
		if len(r.info.Delivery) != 1 || r.info.Delivery[0].Service != "orders-api" {
			t.Errorf("delivery %+v", r.info.Delivery)
		}
	}
}
