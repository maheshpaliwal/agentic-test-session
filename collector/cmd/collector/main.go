// Command collector runs the tsession collector service.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/collector/store"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	listen := flag.String("listen", env("TSESSION_COLLECTOR_LISTEN", "127.0.0.1:7071"), "listen address (keep on loopback)")
	kind := flag.String("store", env("TSESSION_COLLECTOR_STORE", "mem"), "store backend: mem|pg")
	dsn := flag.String("pg-dsn", os.Getenv("TSESSION_COLLECTOR_PG_DSN"), "Postgres DSN (store=pg)")
	schema := flag.String("pg-schema", env("TSESSION_COLLECTOR_PG_SCHEMA", store.DefaultPGSchema), "Postgres schema for trace tables (store=pg)")
	window := flag.Duration("window", envDur("TSESSION_COLLECTOR_WINDOW", 500*time.Millisecond), "reorder window (0 = immediate)")
	bundle := flag.String("bundle-dir", env("TSESSION_COLLECTOR_BUNDLE_DIR", "./bundles"), "directory for sealed bundles (empty disables)")
	maxLine := flag.Int("max-line-bytes", envInt("TSESSION_COLLECTOR_MAX_LINE_BYTES", 4<<20), "max bytes of one NDJSON event line; longer lines are rejected individually")
	idle := flag.Duration("idle-evict", envDur("TSESSION_COLLECTOR_IDLE_EVICT", 5*time.Minute), "evict a sealed session's in-memory actor after this idle time (negative disables)")
	hosts := flag.String("allowed-hosts", os.Getenv("TSESSION_COLLECTOR_ALLOWED_HOSTS"), "comma-separated extra Host values accepted besides loopback (e.g. docker service names)")
	origins := flag.String("allowed-origins", os.Getenv("TSESSION_COLLECTOR_ALLOWED_ORIGINS"), "comma-separated browser origins allowed to call the API")
	internal := flag.String("internal-allow", os.Getenv("TSESSION_COLLECTOR_INTERNAL_ALLOW"), "comma-separated CIDRs/IPs besides loopback allowed to call /v1/internal/*")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
	cfg.AllowedHosts, cfg.AllowedOrigins, cfg.InternalAllow = splitCSV(*hosts), splitCSV(*origins), splitCSV(*internal)
	for _, c := range cfg.InternalAllow {
		if _, err := collector.ParseCIDR(c); err != nil {
			log.Error("invalid --internal-allow entry", "entry", c, "err", err)
			os.Exit(2)
		}
	}
	if h, _, err := net.SplitHostPort(*listen); err == nil && h != "127.0.0.1" && h != "localhost" && h != "::1" {
		log.Warn("collector listens on a non-loopback address; its endpoints are unauthenticated", "listen", *listen)
	}
	col := collector.New(st, cfg)
	defer col.Close()

	srv := &http.Server{Addr: *listen, Handler: col.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	log.Info("collector listening", "addr", *listen, "store", *kind, "window", *window)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
