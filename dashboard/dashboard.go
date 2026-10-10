// Package dashboard serves the tsession web UI (embedded, no build step) and
// reverse-proxies the control plane and collector APIs same-origin, or serves
// a built-in synthetic session in demo mode.
package dashboard

import (
	"embed"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strings"
)

//go:embed web
var webFS embed.FS

// WebFS returns the embedded static assets rooted at web/.
func WebFS() fs.FS {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	return sub
}

// Config configures the dashboard handler.
type Config struct {
	ControlPlaneURL string // default http://127.0.0.1:7070
	CollectorURL    string // default http://127.0.0.1:7071
	Demo            bool   // serve synthetic data instead of proxying
	// AllowedHosts are extra Host header values (host or host:port) accepted
	// in addition to loopback names (DNS-rebinding protection).
	AllowedHosts []string
}

const csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"

// New builds the dashboard handler.
func New(cfg Config) (http.Handler, error) {
	if cfg.ControlPlaneURL == "" {
		cfg.ControlPlaneURL = "http://127.0.0.1:7070"
	}
	if cfg.CollectorURL == "" {
		cfg.CollectorURL = "http://127.0.0.1:7071"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"demo":%t}`, cfg.Demo)
	})
	if cfg.Demo {
		d := NewDemo()
		mux.Handle("/api/cp/", http.StripPrefix("/api/cp", d.ControlPlane()))
		mux.Handle("/api/col/", http.StripPrefix("/api/col", d.Collector()))
	} else {
		cp, err := newProxy(cfg.ControlPlaneURL, "/api/cp")
		if err != nil {
			return nil, fmt.Errorf("control plane url: %w", err)
		}
		col, err := newProxy(cfg.CollectorURL, "/api/col")
		if err != nil {
			return nil, fmt.Errorf("collector url: %w", err)
		}
		mux.Handle("/api/cp/", cp)
		mux.Handle("/api/col/", col)
	}
	mux.Handle("/", http.FileServerFS(WebFS()))
	return secure(cfg, mux), nil
}

// secure adds security headers, a Host allow-list and a same-origin check on
// state-changing requests.
func secure(cfg Config, next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, h := range cfg.AllowedHosts {
		allowed[strings.ToLower(h)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hostAllowed(r.Host, allowed) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Cache-Control", "no-cache")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if o := r.Header.Get("Origin"); o != "" {
				u, err := url.Parse(o)
				if err != nil || !strings.EqualFold(u.Host, r.Host) {
					http.Error(w, "cross-origin request refused", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hostAllowed(hostport string, extra map[string]bool) bool {
	hostport = strings.ToLower(hostport)
	if extra[hostport] {
		return true
	}
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if extra[host] {
		return true
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
