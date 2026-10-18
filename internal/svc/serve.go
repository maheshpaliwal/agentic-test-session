package svc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SignalContext is cancelled on SIGINT or SIGTERM (docker stop sends the latter).
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// IsWildcardAddr reports whether addr (host:port) binds every interface:
// an empty host, 0.0.0.0 or ::. A malformed addr counts as wildcard.
func IsWildcardAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err != nil || host == "" || host == "0.0.0.0" || host == "::"
}

// IsLoopbackHost reports whether the host part of addr is a loopback name.
// A malformed addr is not loopback.
func IsLoopbackHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err == nil && (host == "127.0.0.1" || host == "localhost" || host == "::1")
}

// Serve runs srv until it fails or ctx is cancelled, then shuts it down within
// timeout. beforeShutdown (optional) runs first, e.g. to end long-lived streams
// that would otherwise block Shutdown. A clean shutdown returns nil.
func Serve(ctx context.Context, srv *http.Server, timeout time.Duration, beforeShutdown func()) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		if beforeShutdown != nil {
			beforeShutdown()
		}
		sc, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-done // let Shutdown finish draining before main returns
		return nil
	}
	return err
}
