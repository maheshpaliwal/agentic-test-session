package tsessiontest_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client/tsessiontest"
)

func reportDelivery(t *testing.T, s *client.Session, base, body string) {
	t.Helper()
	resp, err := http.Post(base+"/v1/sessions/"+s.ID+"/delivery", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delivery report: %s", resp.Status)
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
		t.Fatalf("test setup: expected a delivery shortfall, got %+v", info)
	}
	if !failed || !strings.Contains(out, "is incomplete: service orders-api (instance o-1): 0 of 7 recorded events reached the collector") ||
		strings.Contains(out, "inventory") {
		t.Fatalf("RequireNoFailures: failed=%v output %q", failed, out)
	}

	// Every event delivered: nothing to report.
	failed, out = check(t, func(tb testing.TB) {
		s = tsessiontest.Run(t, e.opts)
		reportDelivery(t, s, e.opts.BaseURL, `{"service":"orders-api","instance":"o-1","recorded":7,"delivered":7,"final":true}`)
		tsessiontest.RequireNoFailures(tb, s)
	})
	if failed || len(s.Info().Delivery) != 1 {
		t.Fatalf("a healthy session failed: %q (info %+v)", out, s.Info())
	}
}
