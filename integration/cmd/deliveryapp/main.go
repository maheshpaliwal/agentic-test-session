// Command deliveryapp is a service for the delivery-accounting integration
// tests: an HTTP handler that calls a downstream one, traced the way a user
// adopts tsession (the autoinit import and thttp). Unlike exampleapp it needs
// no database, so the tests that run two of them need no Postgres or Redis.
package main

import (
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/maheshpaliwal/agentic-test-session/sdk/autoinit"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/thttp"
)

func main() {
	listen := os.Getenv("DELIVERYAPP_LISTEN")
	if h, _, err := net.SplitHostPort(listen); err != nil || h != "127.0.0.1" {
		log.Fatalf("refusing to listen on %q: DELIVERYAPP_LISTEN must be 127.0.0.1:<port>", listen)
	}
	downstream := os.Getenv("DELIVERYAPP_DOWNSTREAM_URL") // empty: this is the last service of the chain
	hc := thttp.Client(&http.Client{Timeout: 5 * time.Second})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("POST /work", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if downstream != "" {
			req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, downstream+"/work", nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resp, err := hc.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		_, _ = io.WriteString(w, "done\n")
	})
	// POST /burst?n=N records N more events in the request's session, for the
	// tests that need a session with many events (N+1 with the request's own).
	mux.HandleFunc("POST /burst", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		for range min(max(n, 0), 100000) {
			_, sp := core.Begin(r.Context(), core.KindCacheRead, "burst")
			sp.End()
		}
		_, _ = io.WriteString(w, "done\n")
	})
	srv := &http.Server{Addr: listen, Handler: thttp.Handler(mux), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("deliveryapp listening on http://%s", listen)
	log.Fatal(srv.ListenAndServe())
}
