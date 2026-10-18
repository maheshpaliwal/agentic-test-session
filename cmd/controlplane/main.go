// Command controlplane runs the tsession control plane.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

func main() {
	addr := flag.String("addr", svc.Env("TSESSION_CP_ADDR", "127.0.0.1:7070"), "listen address")
	collector := flag.String("collector-url", svc.Env("TSESSION_COLLECTOR_URL", "http://127.0.0.1:7071"), "collector base URL")
	storeFile := flag.String("store-file", svc.Env("TSESSION_CP_STORE", ""), "optional JSON file for session persistence")
	ttl := flag.Int("default-ttl", svc.EnvPositiveInt("TSESSION_CP_DEFAULT_TTL", 3600), "default session TTL (seconds)")
	hosts := flag.String("allowed-hosts", svc.Env("TSESSION_CP_ALLOWED_HOSTS", ""), "extra comma-separated Host values to accept (e.g. controlplane:7070); default loopback only")
	origins := flag.String("allowed-origins", svc.Env("TSESSION_CP_ALLOWED_ORIGINS", ""), "comma-separated browser Origins allowed to call the API (default none)")
	retention := flag.Duration("retention", svc.EnvPositiveDur("TSESSION_CP_RETENTION", 24*time.Hour), "keep finished sessions in memory this long")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if svc.IsWildcardAddr(*addr) {
		log.Warn("control plane is listening on all interfaces; it has no authentication", "addr", *addr)
	}

	var store controlplane.Store = controlplane.NewMemStore()
	if *storeFile != "" {
		fs, err := controlplane.NewFileStore(*storeFile)
		if err != nil {
			log.Error("open store", "err", err)
			os.Exit(1)
		}
		store = fs
	}
	m := controlplane.NewManager(controlplane.Config{
		Store:      store,
		Collector:  &controlplane.HTTPCollector{BaseURL: *collector, Client: &http.Client{Timeout: 30 * time.Second}},
		Logger:     log,
		DefaultTTL: *ttl,
		Retention:  *retention,

		AllowedHosts:   svc.SplitCSV(*hosts),
		AllowedOrigins: svc.SplitCSV(*origins),
	})
	defer m.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	go m.Run(ctx)

	srv := &http.Server{Addr: *addr, Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Info("control plane listening", "addr", *addr, "collector", *collector)
	if err := svc.Serve(ctx, srv, 5*time.Second, m.Close); err != nil { // Close ends SSE streams so Shutdown can finish
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
