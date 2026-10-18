package dashboard

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// Request headers never forwarded upstream (identity / ambient authority).
var stripRequest = []string{
	"Cookie", "Authorization", "Proxy-Authorization", "Origin", "Referer",
	"X-Api-Key", "X-Auth-Token", "X-Csrf-Token", "X-Forwarded-For", "X-Forwarded-Host",
	"X-Forwarded-Proto", "Forwarded",
}

// Response headers never returned to the browser.
var stripResponse = []string{
	"Set-Cookie", "Set-Cookie2", "Www-Authenticate",
	"Access-Control-Allow-Origin", "Access-Control-Allow-Credentials",
	"Access-Control-Allow-Headers", "Access-Control-Allow-Methods",
	"Access-Control-Expose-Headers", "Access-Control-Max-Age",
}

// blocked upstream path prefixes: internal endpoints and the global kill switch
// are not part of the dashboard's surface.
var blockedPrefixes = []string{"/v1/internal", "/v1/kill"}

const maxBody = 1 << 20

func newProxy(raw, prefix string) (http.Handler, error) {
	t, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if (t.Scheme != "http" && t.Scheme != "https") || t.Host == "" {
		return nil, errors.New("must be an absolute http(s) URL: " + raw)
	}
	base := strings.TrimRight(t.EscapedPath(), "/")
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			ep := strings.TrimPrefix(pr.In.URL.EscapedPath(), prefix)
			up, err := url.PathUnescape(ep)
			if err != nil {
				up = ep
			}
			out := pr.Out.URL
			out.Scheme, out.Host = t.Scheme, t.Host
			out.Path = strings.TrimRight(t.Path, "/") + up
			out.RawPath = base + ep
			out.RawQuery = pr.In.URL.RawQuery
			pr.Out.Host = t.Host // upstream Host/Origin guards expect the upstream's own host
			pr.Out.Header.Del("Origin")
			if pr.In.Method == http.MethodPost || pr.In.Method == http.MethodPatch {
				// Upstream guards require a JSON content type even on empty-body POSTs.
				pr.Out.Header.Set("Content-Type", "application/json")
			}
			for _, h := range stripRequest {
				pr.Out.Header.Del(h)
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			for _, h := range stripResponse {
				resp.Header.Del(h)
			}
			return nil
		},
		FlushInterval: -1, // stream SSE immediately
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 20 * time.Second,
			MaxIdleConns:          10,
			IdleConnTimeout:       30 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			fmt.Fprintf(w, `{"error":"upstream unavailable","upstream":%q}`, t.Host)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodPost:
		case http.MethodPatch:
			// rename only: exactly /v1/sessions/{id} on the control plane
			if !renamePath(strings.TrimPrefix(r.URL.Path, prefix)) || prefix != "/api/cp" {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, prefix)
		for _, seg := range strings.Split(rest, "/") {
			if seg == ".." || seg == "." {
				http.Error(w, "bad path", http.StatusBadRequest)
				return
			}
		}
		for _, b := range blockedPrefixes {
			if rest == b || strings.HasPrefix(rest, b+"/") {
				http.Error(w, "endpoint not exposed by dashboard", http.StatusForbidden)
				return
			}
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		}
		rp.ServeHTTP(w, r)
	}), nil
}

// renamePath reports whether p is exactly /v1/sessions/{id} (one non-empty, non-dot segment).
func renamePath(p string) bool {
	const pre = "/v1/sessions/"
	if !strings.HasPrefix(p, pre) {
		return false
	}
	id := p[len(pre):]
	return id != "" && id != "." && id != ".." && !strings.Contains(id, "/")
}
