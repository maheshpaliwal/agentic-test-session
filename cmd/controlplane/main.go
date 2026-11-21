// Command controlplane runs the tsession control plane.
package main

import (
	"cmp"
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
	flag.StringVar(addr, "listen", *addr, "alias of --addr, the name the collector and dashboard use")
	collector := flag.String("collector-url", svc.Env("TSESSION_COLLECTOR_URL", "http://127.0.0.1:7071"), "collector base URL")
	storeFile := flag.String("store-file", svc.Env("TSESSION_CP_STORE", ""), "optional file for session persistence (also writes FILE.journal and FILE.archive)")
	ttl := flag.Int("default-ttl", svc.EnvPositiveInt("TSESSION_CP_DEFAULT_TTL", 3600), "default session TTL (seconds)")
	hosts := flag.String("allowed-hosts", svc.Env("TSESSION_CP_ALLOWED_HOSTS", ""), "extra comma-separated Host values to accept (e.g. controlplane:7070); default loopback only")
	origins := flag.String("allowed-origins", svc.Env("TSESSION_CP_ALLOWED_ORIGINS", ""), "comma-separated browser Origins allowed to call the API (default none)")
	retention := flag.Duration("retention", svc.EnvPositiveDur("TSESSION_CP_RETENTION", 24*time.Hour), "keep finished sessions in memory this long; without --store-file they are then forgotten")
	grace := flag.Duration("delivery-grace", svc.EnvDur("TSESSION_CP_DELIVERY_GRACE", 10*time.Second), "how long a drain waits for a service that reported undelivered events and delivers none, before the session is sealed incomplete; 0 turns the check off")
	showVersion := svc.VersionFlag("tsession-controlplane")
	flag.Parse()
	showVersion()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if svc.IsWildcardAddr(*addr) {
		log.Warn("control plane is listening on all interfaces; it has no authentication", "addr", *addr)
	}

	var store controlplane.Store = controlplane.NewMemStore()
	if *storeFile != "" {
		fs, err := controlplane.NewFileStore(*storeFile, controlplane.WithStoreLogger(log))
		if err != nil {
			log.Error("open store", "err", err)
			os.Exit(1)
		}
		store = fs
		// Runs after m.Close below: fold the journal into the snapshot, so a
		// clean shutdown leaves the whole hot set in the one JSON file, and
		// release the archive file.
		defer func() {
			if err := fs.Compact(); err != nil {
				log.Warn("compact store", "err", err)
			}
			if err := fs.Close(); err != nil {
				log.Warn("close store", "err", err)
			}
		}()
	}
	m := controlplane.NewManager(controlplane.Config{
		Store:      store,
		Collector:  &controlplane.HTTPCollector{BaseURL: *collector, Client: &http.Client{Timeout: 30 * time.Second}},
		Logger:     log,
		DefaultTTL: *ttl,
		Retention:  *retention,
		// For Config zero means the default, so "off" is passed as negative.
		DeliveryGrace: cmp.Or(*grace, -1),

		AllowedHosts:   svc.SplitCSV(*hosts),
		AllowedOrigins: svc.SplitCSV(*origins),
	})
	defer m.Close()

	ctx, stop := svc.SignalContext()
	defer stop()
	go m.Run(ctx)

	// No WriteTimeout: /v1/active/stream is a long-lived SSE response.
	srv := &http.Server{Addr: *addr, Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	ln, err := svc.Listen(*addr, "--addr or TSESSION_CP_ADDR")
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	log.Info("control plane listening", "addr", *addr, "collector", *collector)
	if err := svc.ServeOn(ctx, srv, ln, 5*time.Second, m.Close); err != nil { // Close ends SSE streams so Shutdown can finish
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
