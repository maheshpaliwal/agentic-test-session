//go:build unix

package integration

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// The scenarios of the fifth review of delivery accounting, with the real
// binaries: two ways in which a service whose reports stopped arriving, and
// whose collector URL is wrong, left its sessions complete without a line.

// wantNamedAsNeverHeard fails the test for every run that does not name
// orders-api, and nothing else, as a connection of which no report arrived.
func wantNamedAsNeverHeard(t *testing.T, runs []deliveryRun) {
	t.Helper()
	for _, r := range runs {
		if r.info.Incomplete || len(r.info.IncompleteReasons) != 0 || r.got != 6 || len(r.info.DeliveryUnconfirmed) != 1 ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "service orders-api (instance ") ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "no delivery report of it has arrived since it connected") ||
			!strings.Contains(r.info.DeliveryUnconfirmed[0], "the collector has no events from it for this session") {
			t.Errorf("want orders-api under delivery_unconfirmed, and nothing else: %s", r)
		}
		if r.drain > 5*time.Second {
			t.Errorf("the drain was held for an instance nothing is known of: %s", r)
		}
	}
}

// streamsOf counts the connections of a service to the active set, as the
// control plane lists them.
func (e *deliveryEnv) streamsOf(service string) (n int) {
	e.t.Helper()
	subs, err := client.Subscribers(e.t.Context(), client.Options{BaseURL: e.cpURL, HTTPClient: deliveryHTTP})
	if err != nil {
		panic(err)
	}
	for _, sub := range subs {
		if sub.Service == service {
			n++
		}
	}
	return n
}

// lostWord runs sessions in which inventory and orders-api record five
// events each, and orders-api, whose events reach nobody, is held for longer
// than the grace before the session is stopped.
func lostWord(t *testing.T, e *deliveryEnv, scenario string, apps []*svcApp) []deliveryRun {
	t.Helper()
	return repeat(t, scenario, deliveryRuns(), 8, func(int) deliveryRun {
		s, err := e.begin(0)
		if err != nil {
			return failed(err)
		}
		for _, a := range apps {
			if err := a.burst(s, 5); err != nil {
				return failed(err)
			}
		}
		// The requests of orders-api fail for the grace (10s), it opens its
		// stream anew, and is named a second after it connected.
		time.Sleep(13 * time.Second)
		return e.finish(s, 12)
	})
}

// The first report of a service arrives at the control plane, and its answer
// is lost on the way back (a gateway that timed out); nothing of the service
// arrives since. The control plane has heard of it; the service has not had
// an answer on its stream. It used to wait for one before it would open the
// stream anew, so it stayed "heard of, and silent now", which is named only
// where something ties the instance to the session: 0 of 6 sessions said
// anything. A request that got no answer may have arrived, so the stream is
// opened anew for it as well, once.
func TestDelivery_AFirstReportWhoseAnswerIsLostIsNotASilentMiss(t *testing.T) {
	e := newDeliveryBase(t)
	target, err := url.Parse(e.cpURL)
	if err != nil {
		panic(err)
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1 // the active-set stream
	var posts atomic.Int64
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = target.Host
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delivery") {
			if posts.Add(1) == 1 {
				rp.ServeHTTP(httptest.NewRecorder(), r) // it arrives; its answer does not
			}
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(gateway.Close)
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{collector: "http://" + freeAddr(t), cp: gateway.URL})
	e.connected("inventory")
	e.connected("orders-api")
	e.heard("orders-api") // the control plane has its first report
	wantNamedAsNeverHeard(t, lostWord(t, e, "report-answer-lost", []*svcApp{inventory, orders}))
	// One stream opened anew, and no more for as long as the outage lasts:
	// the list of followers has orders-api once (the gateway closes the old
	// stream with the client's).
	n := e.streamsOf("orders-api")
	if n != 1 {
		t.Errorf("orders-api follows the control plane %d times, want once", n)
	}
}

// lingerProxy is a TCP proxy to addr that keeps the upstream side of a
// connection open for linger after the client closed it, as a load balancer
// with connection draining does: the control plane goes on seeing a stream
// the SDK ended.
func lingerProxy(t *testing.T, addr string, linger time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done); ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				up, err := net.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer up.Close()
				go func() { _, _ = io.Copy(c, up); c.Close() }()
				_, _ = io.Copy(up, c)
				select {
				case <-time.After(linger):
				case <-done:
				}
			}()
		}
	}()
	return "http://" + ln.Addr().String()
}

// The reports of a service arrive when it connects and are refused from then
// on, while its collector URL is wrong
// (TestDelivery_ReportsThatStopArrivingAreNotASilentMiss), and what is
// between it and the control plane keeps the stream it ended open for a
// minute. The control plane then has two connections of the instance: the
// old one, heard of and silent, and the new one, of which nothing has
// arrived. It went by the one heard of last, the old one, and said nothing:
// 0 of 5 sessions. It goes by the newest.
func TestDelivery_AStreamThatLingersDoesNotHideTheOneOpenedAnew(t *testing.T) {
	e := newDeliveryBase(t)
	var block atomic.Bool
	refusing := e.cpProxy(func(r *http.Request) int {
		if block.Load() && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/delivery") {
			return http.StatusForbidden
		}
		return 0
	})
	proxy := lingerProxy(t, strings.TrimPrefix(refusing, "http://"), time.Minute)
	inventory := e.startSvc("inventory", svcOpts{})
	orders := e.startSvc("orders-api", svcOpts{collector: "http://" + freeAddr(t), cp: proxy})
	e.connected("inventory")
	e.connected("orders-api")
	e.heard("orders-api") // its first request got through
	block.Store(true)
	runs := lostWord(t, e, "reports-stop-stream-lingers", []*svcApp{inventory, orders})
	// The setup is the one meant: the stream orders-api ended is still there.
	n := e.streamsOf("orders-api")
	if n != 2 {
		t.Errorf("test setup: orders-api follows the control plane %d times, want twice (the stream it ended lingers)", n)
	}
	wantNamedAsNeverHeard(t, runs)
}
