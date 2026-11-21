package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A rejected Host is named in the response, as plain text, so the operator
// sees what to add to the allow-list.
func TestGuardNamesTheRejectedHost(t *testing.T) {
	h := LoopbackGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), GuardConfig{})
	req := httptest.NewRequest(http.MethodGet, "/v1/active", nil)
	req.Host = "controlplane:7070"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"controlplane:7070"`) ||
		!strings.Contains(rec.Body.String(), "--allowed-hosts") {
		t.Fatalf("%d %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("the echoed host must be served as plain text, got %q", ct)
	}
}
