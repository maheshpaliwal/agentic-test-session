// Command dashboard serves the tsession web UI.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/dashboard"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

func main() {
	listen := flag.String("listen", svc.Env("TSESSION_DASHBOARD_ADDR", "127.0.0.1:7072"), "listen address (keep on loopback)")
	cpURL := flag.String("control-plane", svc.Env("TSESSION_CONTROLPLANE_URL", "http://127.0.0.1:7070"), "control plane base URL")
	colURL := flag.String("collector", svc.Env("TSESSION_COLLECTOR_URL", "http://127.0.0.1:7071"), "collector base URL")
	demo := flag.Bool("demo", os.Getenv("TSESSION_DASHBOARD_DEMO") == "true", "serve built-in synthetic data; no other services needed")
	hosts := flag.String("allowed-hosts", svc.Env("TSESSION_DASHBOARD_ALLOWED_HOSTS", ""), "extra comma-separated Host values to accept (default: loopback only)")
	var open []string
	flag.Func("open", "serve a session exported with `tracectl export -o FILE`, read-only, without other services (repeatable)",
		func(p string) error { open = append(open, p); return nil })
	showVersion := svc.VersionFlag("tsession-dashboard")
	flag.Parse()
	showVersion()

	h, err := dashboard.New(dashboard.Config{ControlPlaneURL: *cpURL, CollectorURL: *colURL, Demo: *demo, Open: open, AllowedHosts: svc.SplitCSV(*hosts)})
	if err != nil {
		log.Fatalf("dashboard: %v", err)
	}
	if svc.IsWildcardAddr(*listen) {
		log.Fatalf("dashboard: refusing to listen on %q; bind a specific address such as 127.0.0.1:7072", *listen)
	}
	srv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := svc.SignalContext()
	defer stop()
	mode := "proxying " + *cpURL + " and " + *colURL
	if *demo {
		mode = "DEMO mode (synthetic data)"
	}
	if len(open) > 0 {
		mode = fmt.Sprintf("OFFLINE, %d exported session(s), read-only", len(open))
	}
	ln, err := svc.Listen(*listen, "-listen or TSESSION_DASHBOARD_ADDR")
	if err != nil {
		log.Fatalf("dashboard: %v", err)
	}
	// The address that was bound, not the one asked for: with port 0 they differ.
	base := "http://" + ln.Addr().String()
	log.Printf("dashboard on %s - %s", base, mode)
	if *demo || len(open) > 0 {
		// These sessions answer in the shapes of the real services, so the
		// CLI reads them with the commands it uses on live ones.
		log.Printf("tracectl reads these sessions too (list, summary, events, trace, entities, history, show, report, digest, diff):")
		log.Printf("  tracectl --cp %s/api/cp --collector %s/api/col list", base, base)
	}
	if err := svc.ServeOn(ctx, srv, ln, 3*time.Second, nil); err != nil {
		log.Fatal(err)
	}
}
