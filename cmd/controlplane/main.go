// Command controlplane runs the tsession control plane.
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
	"syscall"
	"time"

	"tsession/controlplane"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	addr := flag.String("addr", env("TSESSION_CP_ADDR", "127.0.0.1:7070"), "listen address")
	collector := flag.String("collector-url", env("TSESSION_COLLECTOR_URL", "http://127.0.0.1:7071"), "collector base URL")
	storeFile := flag.String("store-file", env("TSESSION_CP_STORE", ""), "optional JSON file for session persistence")
	ttl := flag.Int("default-ttl", envInt("TSESSION_CP_DEFAULT_TTL", 3600), "default session TTL (seconds)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if host, _, err := net.SplitHostPort(*addr); err != nil || host == "" || host == "0.0.0.0" || host == "::" {
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
	})
	defer m.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go m.Run(ctx)

	srv := &http.Server{Addr: *addr, Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		m.Close() // ends SSE streams so Shutdown can finish
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("control plane listening", "addr", *addr, "collector", *collector)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func envInt(k string, d int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n > 0 {
		return n
	}
	return d
}
