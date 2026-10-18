package runtime

import (
	"net"
	"net/http"
	"strconv"
	"strings"

	"tsession/core"
)

// Transport is a minimal core-aware http.RoundTripper: it records an api.out
// span for requests made inside an active session and injects the session
// headers. Requests that already carry session headers (e.g. from thttp) pass
// through untouched.
//
// Header propagation can be restricted: when PropagateHosts is non-empty only
// hosts matching one of its suffixes receive session headers; hosts matching
// NoPropagate never do (it always wins). Excluded requests are still recorded.
type Transport struct {
	Base           http.RoundTripper
	PropagateHosts []string
	NoPropagate    []string
}

// hostMatches reports whether host (port ignored) equals or is a subdomain of
// any entry in list. Entries may carry a leading "." or "*.".
func hostMatches(host string, list []string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.Trim(host, "[]"))
	for _, e := range list {
		e = strings.ToLower(strings.TrimSpace(e))
		e = strings.TrimPrefix(strings.TrimPrefix(e, "*"), ".")
		if e == "" {
			continue
		}
		if host == e || strings.HasSuffix(host, "."+e) {
			return true
		}
	}
	return false
}

func (t *Transport) propagateTo(host string) bool {
	if hostMatches(host, t.NoPropagate) {
		return false
	}
	return len(t.PropagateHosts) == 0 || hostMatches(host, t.PropagateHosts)
}

func (t *Transport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return fallbackTransport
}

var fallbackTransport = newPlainTransport()

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get(core.KeySession) != "" {
		return t.base().RoundTrip(req)
	}
	ctx, span := core.Begin(req.Context(), core.KindAPIOut, req.Method+" "+req.URL.Host+req.URL.Path)
	if span == nil {
		return t.base().RoundTrip(req)
	}
	out := req.Clone(ctx)
	if t.propagateTo(req.URL.Host) {
		core.Inject(ctx, core.HTTPHeaderCarrier(out.Header))
	} else {
		span.SetAttr("propagated", "false")
	}
	span.SetReq(map[string]any{"method": req.Method, "url": core.RedactURLBound(req.URL), "headers": core.RedactHeadersBound(req.Header)})
	resp, err := t.base().RoundTrip(out)
	if err != nil {
		span.SetErr(err)
	} else {
		span.SetAttr("http.status", strconv.Itoa(resp.StatusCode))
		span.SetResp(map[string]any{"status": resp.StatusCode})
	}
	span.End()
	return resp, err
}
