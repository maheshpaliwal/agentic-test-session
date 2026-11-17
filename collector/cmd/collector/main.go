// Command collector runs the tsession collector service.
package main

import (
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector"
	"github.com/maheshpaliwal/agentic-test-session/internal/svc"
)

func main() {
	sf := registerStoreFlags()
	listen := flag.String("listen", svc.Env("TSESSION_COLLECTOR_LISTEN", "127.0.0.1:7071"), "listen address (keep on loopback)")
	window := flag.Duration("window", svc.EnvDur("TSESSION_COLLECTOR_WINDOW", 500*time.Millisecond), "reorder window (0 = immediate)")
	bundle := flag.String("bundle-dir", svc.Env("TSESSION_COLLECTOR_BUNDLE_DIR", "./bundles"), "directory for sealed bundles (empty disables)")
	maxMsg := flag.Int("max-message-bytes", svc.EnvInt("TSESSION_COLLECTOR_MAX_MESSAGE_BYTES", 4<<20), "max encoded bytes of one protobuf Event message; longer ones are rejected individually")
	maxLine := flag.Int("max-line-bytes", 0, "deprecated alias of --max-message-bytes")
	idle := flag.Duration("idle-evict", svc.EnvDur("TSESSION_COLLECTOR_IDLE_EVICT", 5*time.Minute), "evict a sealed session's in-memory actor after this idle time (negative disables)")
	retention := flag.Duration("retention", svc.EnvDur("TSESSION_COLLECTOR_RETENTION", 0), "delete a session this long after it was sealed, e.g. 168h (0 keeps sessions forever)")
	hosts := flag.String("allowed-hosts", os.Getenv("TSESSION_COLLECTOR_ALLOWED_HOSTS"), "comma-separated extra Host values accepted besides loopback (e.g. docker service names)")
	origins := flag.String("allowed-origins", os.Getenv("TSESSION_COLLECTOR_ALLOWED_ORIGINS"), "comma-separated browser origins allowed to call the API")
	internal := flag.String("internal-allow", os.Getenv("TSESSION_COLLECTOR_INTERNAL_ALLOW"), "comma-separated CIDRs/IPs besides loopback allowed to call /v1/internal/*")
	showVersion := svc.VersionFlag("tsession-collector")
	flag.Parse()
	showVersion()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// Deprecated names still apply (the flag wins over the new env var, the old
	// env var only when the new one is unset) so old configs are not ignored.
	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
	switch _, oldEnv := os.LookupEnv("TSESSION_COLLECTOR_MAX_LINE_BYTES"); {
	case set["max-line-bytes"]:
		log.Warn("--max-line-bytes is deprecated; use --max-message-bytes")
		if !set["max-message-bytes"] {
			*maxMsg = *maxLine
		}
	case oldEnv && !set["max-message-bytes"] && os.Getenv("TSESSION_COLLECTOR_MAX_MESSAGE_BYTES") == "":
		log.Warn("TSESSION_COLLECTOR_MAX_LINE_BYTES is deprecated; use TSESSION_COLLECTOR_MAX_MESSAGE_BYTES")
		*maxMsg = svc.EnvInt("TSESSION_COLLECTOR_MAX_LINE_BYTES", *maxMsg)
	}
	ctx, stop := svc.SignalContext()
	defer stop()

	st, err := sf.open(ctx)
	if err != nil {
		log.Error("open store", "store", *sf.kind, "err", err)
		var ue usageError
		if errors.As(err, &ue) {
			os.Exit(2)
		}
		os.Exit(1)
	}
	defer st.Close()

	cfg := collector.DefaultConfig()
	cfg.Window, cfg.BundleDir, cfg.Logger = *window, *bundle, log
	cfg.MaxMessageBytes, cfg.IdleEvict = *maxMsg, *idle
	if *retention < 0 {
		log.Error("--retention must not be negative", "retention", *retention)
		os.Exit(2)
	}
	cfg.Retention = *retention
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
	log.Info("collector listening", "addr", *listen, "store", *sf.kind, "window", *window, "retention", *retention)
	if err := svc.Serve(ctx, srv, 5*time.Second, nil); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
