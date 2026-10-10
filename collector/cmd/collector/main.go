// Command collector runs the tsession collector service.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tsession/collector"
	"tsession/collector/store"
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

func main() {
	listen := flag.String("listen", env("TSESSION_COLLECTOR_LISTEN", "127.0.0.1:7071"), "listen address (keep on loopback)")
	kind := flag.String("store", env("TSESSION_COLLECTOR_STORE", "mem"), "store backend: mem|pg")
	dsn := flag.String("pg-dsn", os.Getenv("TSESSION_COLLECTOR_PG_DSN"), "Postgres DSN (store=pg)")
	window := flag.Duration("window", envDur("TSESSION_COLLECTOR_WINDOW", 200*time.Millisecond), "reorder window (0 = immediate)")
	bundle := flag.String("bundle-dir", env("TSESSION_COLLECTOR_BUNDLE_DIR", "./bundles"), "directory for sealed bundles (empty disables)")
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
		pg, err := store.OpenPG(ctx, *dsn)
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
