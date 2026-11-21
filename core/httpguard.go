package core

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// GuardConfig configures LoopbackGuard.
type GuardConfig struct {
	// AllowedHosts are extra Host header values (host or host:port) accepted
	// besides loopback names/IPs (needed e.g. for container service names).
	AllowedHosts []string
	// AllowedOrigins are browser origins (scheme://host[:port]) allowed to call
	// state-changing endpoints. Requests without an Origin header (non-browser
	// clients) are always allowed.
	AllowedOrigins []string
	// BodyContentTypes are accepted Content-Types for requests with a body
	// (default: application/json).
	BodyContentTypes []string
	// OnBadContentType, if set, writes the 415 response for a body with a
	// disallowed Content-Type (default: a plain-text error), so a service can
	// answer in its own error shape.
	OnBadContentType func(w http.ResponseWriter, r *http.Request)
}

// LoopbackGuard defends unauthenticated local control endpoints against
// DNS-rebinding and browser CSRF: it validates Host, rejects cross-origin
// browser requests, and requires an allowed (default JSON) Content-Type on POST/PUT/PATCH/DELETE
// (which forces a CORS preflight that we never answer favourably).
func LoopbackGuard(next http.Handler, cfg GuardConfig) http.Handler {
	ct := cfg.BodyContentTypes
	if len(ct) == 0 {
		ct = []string{"application/json"}
	}
	hostOK := func(h string) bool {
		name := h
		if hh, _, err := net.SplitHostPort(h); err == nil {
			name = hh
		}
		name = strings.Trim(name, "[]")
		if name == "localhost" || name == "127.0.0.1" || name == "::1" {
			return true
		}
		for _, a := range cfg.AllowedHosts {
			if a == h || a == name {
				return true
			}
		}
		return false
	}
	originOK := func(o string) bool {
		if o == "" {
			return true
		}
		if u, err := url.Parse(o); err == nil && hostOK(u.Host) {
			for _, a := range cfg.AllowedOrigins {
				if a == o {
					return true
				}
			}
		}
		for _, a := range cfg.AllowedOrigins {
			if a == o {
				return true
			}
		}
		return false
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hostOK(r.Host) {
			// Name the host and the remedy: a client in another container
			// otherwise sees a bare 403 and an empty session.
			http.Error(w, fmt.Sprintf("forbidden host %q: this server accepts loopback names only; add the host to its allowed hosts (--allowed-hosts)", r.Host), http.StatusForbidden)
			return
		}
		if !originOK(r.Header.Get("Origin")) {
			http.Error(w, "forbidden origin", http.StatusForbidden)
			return
		}
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			if r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
				mt := strings.ToLower(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]))
				ok := false
				for _, c := range ct {
					if mt == c {
						ok = true
					}
				}
				if !ok {
					if cfg.OnBadContentType != nil {
						cfg.OnBadContentType(w, r)
						return
					}
					http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
