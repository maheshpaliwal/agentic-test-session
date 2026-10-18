// Command collector runs the tsession collector service.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

func main() {
	listen := flag.String("listen", svc.Env("TSESSION_COLLECTOR_LISTEN", "127.0.0.1:7071"), "listen address (keep on loopback)")
	kind := flag.String("store", svc.Env("TSESSION_COLLECTOR_STORE", "mem"), "store backend: mem|pg")
	dsn := flag.String("pg-dsn", os.Getenv("TSESSION_COLLECTOR_PG_DSN"), "Postgres DSN (store=pg)")
	schema := flag.String("pg-schema", svc.Env("TSESSION_COLLECTOR_PG_SCHEMA", store.DefaultPGSchema), "Postgres schema for trace tables (store=pg)")
	window := flag.Duration("window", svc.EnvDur("TSESSION_COLLECTOR_WINDOW", 500*time.Millisecond), "reorder window (0 = immediate)")
	bundle := flag.String("bundle-dir", svc.Env("TSESSION_COLLECTOR_BUNDLE_DIR", "./bundles"), "directory for sealed bundles (empty disables)")
	maxLine := flag.Int("max-line-bytes", svc.EnvInt("TSESSION_COLLECTOR_MAX_LINE_BYTES", 4<<20), "max bytes of one NDJSON event line; longer lines are rejected individually")
	idle := flag.Duration("idle-evict", svc.EnvDur("TSESSION_COLLECTOR_IDLE_EVICT", 5*time.Minute), "evict a sealed session's in-memory actor after this idle time (negative disables)")
	hosts := flag.String("allowed-hosts", os.Getenv("TSESSION_COLLECTOR_ALLOWED_HOSTS"), "comma-separated extra Host values accepted besides loopback (e.g. docker service names)")
	origins := flag.String("allowed-origins", os.Getenv("TSESSION_COLLECTOR_ALLOWED_ORIGINS"), "comma-separated browser origins allowed to call the API")
	internal := flag.String("internal-allow", os.Getenv("TSESSION_COLLECTOR_INTERNAL_ALLOW"), "comma-separated CIDRs/IPs besides loopback allowed to call /v1/internal/*")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := svc.SignalContext()
	defer stop()

	var st store.Store
	switch *kind {
	case "mem":
		st = store.NewMem()
	case "pg":
		if *dsn == "" {
			log.Error("store=pg requires --pg-dsn or TSESSION_COLLECTOR_PG_DSN")
			os.Exit(2)
		}
		pg, err := store.OpenPGConfig(ctx, store.PGConfig{DSN: *dsn, Schema: *schema})
		if err != nil {
			log.Error("open postgres", "err", err)
			os.Exit(1)
		}
		st = pg
	default:
		log.Error("unknown store", "store", *kind)
		os.Exit(2)
	}
	defer st.Close()

	cfg := collector.DefaultConfig()
	cfg.Window, cfg.BundleDir, cfg.Logger = *window, *bundle, log
	cfg.MaxLineBytes, cfg.IdleEvict = *maxLine, *idle
	cfg.AllowedHosts, cfg.AllowedOrigins, cfg.InternalAllow = svc.SplitCSV(*hosts), svc.SplitCSV(*origins), svc.SplitCSV(*internal)
	for _, c := range cfg.InternalAllow {
		if _, err := collector.ParseCIDR(c); err != nil {
			log.Error("invalid --internal-allow entry", "entry", c, "err", err)
			os.Exit(2)
		}
	}
	if !svc.IsLoopbackHost(*listen) {
		log.Warn("collector listens on a non-loopback address; its endpoints are unauthenticated", "listen", *listen)
	}
	col := collector.New(st, cfg)
	defer col.Close()

	srv := &http.Server{Addr: *listen, Handler: col.Handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Info("collector listening", "addr", *listen, "store", *kind, "window", *window)
	if err := svc.Serve(ctx, srv, 5*time.Second, nil); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
