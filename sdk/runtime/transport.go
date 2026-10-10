package runtime

import (
	"net/http"
	"strconv"

	"tsession/core"
)

// Transport is a minimal core-aware http.RoundTripper: it records an api.out
// span for requests made inside an active session and injects the session
// headers. Requests that already carry session headers (e.g. from thttp) pass
// through untouched.
type Transport struct{ Base http.RoundTripper }

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
	core.Inject(ctx, core.HTTPHeaderCarrier(out.Header))
	span.SetReq(map[string]any{"method": req.Method, "url": req.URL.Redacted(), "headers": core.RedactHeaders(req.Header)})
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
