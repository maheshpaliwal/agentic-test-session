package thttp

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// tracedTransport is the idempotency marker for client-side wrapping.
type tracedTransport struct{ rt http.RoundTripper }

func (t tracedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := core.Begin(req.Context(), core.KindAPIOut, req.Method+" "+req.URL.Path)
	if span == nil {
		return t.rt.RoundTrip(req)
	}

	out := req.Clone(ctx)
	core.Inject(ctx, core.HTTPHeaderCarrier(out.Header))
	reqCap := newLimitBuf()
	if req.Body != nil && req.Body != http.NoBody {
		out.Body = &teeBody{rc: req.Body, cl: req.Body.Close, cap: reqCap}
	}
	span.SetAttr("host", req.URL.Host)

	finish := func(resp *http.Response, respCap *limitBuf, err error) {
		span.SetReq(message{
			Method:   req.Method,
			URL:      core.RedactURLBound(req.URL),
			Headers:  redactHeaders(req.Header),
			Body:     reqCap.body(req.Header.Get("Content-Type")),
			BodySize: reqCap.size(),
		})
		if err != nil {
			span.SetErr(err)
		} else {
			if resp.StatusCode >= 500 {
				span.SetErr(fmt.Errorf("HTTP %d", resp.StatusCode))
			}
			span.SetResp(message{Status: resp.StatusCode, Headers: redactHeaders(resp.Header), Body: respCap.body(resp.Header.Get("Content-Type")), BodySize: respCap.size()})
		}
		if reqCap.truncated || (respCap != nil && respCap.truncated) {
			span.AddFlag("truncated")
		}
		span.End()
	}

	resp, err := t.rt.RoundTrip(out)
	if err != nil {
		finish(nil, nil, err)
		return nil, err
	}
	if resp.StatusCode == http.StatusSwitchingProtocols || resp.Body == nil {
		finish(resp, nil, nil)
		return resp, nil
	}
	respCap := newLimitBuf()
	var once sync.Once
	done := func() { once.Do(func() { finish(resp, respCap, nil) }) }
	orig := resp.Body
	resp.Body = &respBody{
		teeBody: teeBody{rc: orig, cl: func() error { err := orig.Close(); done(); return err }, cap: respCap},
		done:    done,
	}
	return resp, nil
}

// respBody ends the span once the body is fully read or closed.
type respBody struct {
	teeBody
	done func()
}

func (b *respBody) Read(p []byte) (int, error) {
	n, err := b.teeBody.Read(p)
	if err == io.EOF {
		b.done()
	}
	return n, err
}

// Transport wraps rt (http.DefaultTransport if nil) to record api.out spans and
// propagate session context. Returns rt unchanged when disabled or already wrapped.
func Transport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	if !core.Enabled() {
		return rt
	}
	if _, ok := rt.(tracedTransport); ok {
		return rt
	}
	return tracedTransport{rt}
}

// Client returns a copy of c whose transport is traced. A nil c yields a new client.
func Client(c *http.Client) *http.Client {
	if c == nil {
		c = &http.Client{}
	}
	if !core.Enabled() {
		return c
	}
	nc := *c
	nc.Transport = Transport(c.Transport)
	return &nc
}
