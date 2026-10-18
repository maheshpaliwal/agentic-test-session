// Command exampleapp is the integration-test service. Adoption is one blank
// import; everything is env driven (TSESSION_ENABLED=true to trace).
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	_ "tsession/sdk/autoinit"

	"tsession/integration/app"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	listen := env("EXAMPLEAPP_LISTEN", "127.0.0.1:0")
	if h, _, err := net.SplitHostPort(listen); err != nil || h == "" || h == "0.0.0.0" || h == "::" {
		log.Fatalf("refusing to listen on %q", listen)
	}
	a, err := app.New(app.Config{
		PGDSN:         env("EXAMPLEAPP_PG_DSN", "postgres://127.0.0.1:55432/postgres?sslmode=disable"),
		RedisAddr:     env("EXAMPLEAPP_REDIS_ADDR", "127.0.0.1:6379"),
		DownstreamURL: os.Getenv("EXAMPLEAPP_DOWNSTREAM_URL"),
		Namespace:     os.Getenv("EXAMPLEAPP_NAMESPACE"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer a.Close()
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("exampleapp listening on http://%s", ln.Addr())
	srv := &http.Server{Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
