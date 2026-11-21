package tsessiontest_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

func reportDelivery(t *testing.T, s *client.Session, base, body string) {
	t.Helper()
	resp, err := http.Post(base+"/v1/sessions/"+s.ID+"/delivery", "application/json", strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("delivery report: %s", resp.Status))
	}
}

// A service recorded events and could not deliver them. The collector's
// summary shows nothing wrong (it never saw them); RequireNoFailures fails
// on the control plane's verdict and says which service it was.
func TestRequireNoFailuresWithUndeliveredEvents(t *testing.T) {
	e := newEnv(t)
	var s *client.Session
	failed, out := check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"inventory","instance":"i-1","recorded":1,"delivered":1,"final":true}`)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"orders-api","instance":"o-1","recorded":7,"dropped":7,"last_error":"connection refused","final":true}`)
		tsessiontest.RequireNoFailures(tb, s)
	})
	info := s.Info()
	if !info.Incomplete || len(info.IncompleteReasons) != 1 || len(info.Delivery) != 2 {
		panic(fmt.Sprintf("test setup: expected a delivery shortfall, got %+v", info))
	}
	if !failed || !strings.Contains(out, "is incomplete: service orders-api (instance o-1): 0 of 7 recorded events") || !strings.Contains(out, "7 dropped") ||
		strings.Contains(out, "inventory") {
		panic(fmt.Sprintf("RequireNoFailures: failed=%v output %q", failed, out))
	}

	// Every event delivered: nothing to report.
	failed, out = check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"orders-api","instance":"o-1","recorded":7,"delivered":7,"final":true}`)
		tsessiontest.RequireNoFailures(tb, s)
	})
	if failed || len(s.Info().Delivery) != 1 {
		panic(fmt.Sprintf("a healthy session failed: %q (info %+v)", out, s.Info()))
	}
}

// A delivery the control plane could not confirm does not fail
// RequireNoFailures, which logs it; RequireDeliveryConfirmed fails on it.
func TestRequireDeliveryConfirmed(t *testing.T) {
	e := newEnv(t)
	var s *client.Session
	failed, out := check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		// It held events when it last reported, and was not heard of again.
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"batch","instance":"b-1","recorded":5,"delivered":3,"buffered":2}`)
		tsessiontest.RequireNoFailures(tb, s)
	})
	if failed || !strings.Contains(out, "delivery unconfirmed: service batch (instance b-1): 3 of 5") {
		panic(fmt.Sprintf("RequireNoFailures: failed=%v output %q (info %+v)", failed, out, s.Info()))
	}
	failed, out = check(t, func(tb testing.TB) { tsessiontest.RequireDeliveryConfirmed(tb, s) })
	if !failed || !strings.Contains(out, "delivery unconfirmed: service batch (instance b-1): 3 of 5") {
		panic(fmt.Sprintf("RequireDeliveryConfirmed: failed=%v output %q", failed, out))
	}

	// The report says that everything was delivered, and nothing but the
	// report: the collector is one that does not count per instance. That is
	// not a confirmation.
	failed, out = check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"batch","instance":"b-1","recorded":5,"delivered":5,"final":true}`)
		tsessiontest.RequireDeliveryConfirmed(tb, s)
	})
	if !failed || !strings.Contains(out, "the collector does not count events per instance") {
		panic(fmt.Sprintf("RequireDeliveryConfirmed with a collector that does not count: failed=%v output %q", failed, out))
	}

	// The collector has what the report says: confirmed.
	e.counts.Store(&[]controlplane.InstanceCount{{Service: "batch", Instance: "b-1", Events: 5}})
	failed, out = check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"batch","instance":"b-1","recorded":5,"delivered":5,"final":true}`)
		tsessiontest.RequireDeliveryConfirmed(tb, s)
	})
	if failed {
		panic(fmt.Sprintf("RequireDeliveryConfirmed on a healthy session: %q", out))
	}
}
