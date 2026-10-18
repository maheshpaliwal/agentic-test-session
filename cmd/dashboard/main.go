// Command dashboard serves the tsession web UI.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/dashboard"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	listen := flag.String("listen", env("TSESSION_DASHBOARD_ADDR", "127.0.0.1:7072"), "listen address (keep on loopback)")
	cpURL := flag.String("control-plane", env("TSESSION_CONTROLPLANE_URL", "http://127.0.0.1:7070"), "control plane base URL")
	colURL := flag.String("collector", env("TSESSION_COLLECTOR_URL", "http://127.0.0.1:7071"), "collector base URL")
	demo := flag.Bool("demo", os.Getenv("TSESSION_DASHBOARD_DEMO") == "true", "serve built-in synthetic data; no other services needed")
	hosts := flag.String("allowed-hosts", env("TSESSION_DASHBOARD_ALLOWED_HOSTS", ""), "extra comma-separated Host values to accept (default: loopback only)")
	flag.Parse()

	var extra []string
	for _, h := range strings.Split(*hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			extra = append(extra, h)
		}
	}
	h, err := dashboard.New(dashboard.Config{ControlPlaneURL: *cpURL, CollectorURL: *colURL, Demo: *demo, AllowedHosts: extra})
	if err != nil {
		log.Fatalf("dashboard: %v", err)
	}
	if host, _, err := net.SplitHostPort(*listen); err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		log.Fatalf("dashboard: refusing to listen on %q; bind a specific address such as 127.0.0.1:7072", *listen)
	}
	srv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		sc, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(sc)
	}()
	mode := "proxying " + *cpURL + " and " + *colURL
	if *demo {
		mode = "DEMO mode (synthetic data)"
	}
	log.Printf("dashboard on http://%s - %s", *listen, mode)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
