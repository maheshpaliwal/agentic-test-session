package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A transport error names the URL. What may be a credential in it (user
// name, password, query) must not reach the message, which tracectl and
// tests print into CI logs; the cause must still be reachable.
func TestTransportErrorHidesCredentialsInTheURL(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	for _, base := range []string{"http://ciuser:hunter2@" + addr, "http://" + addr + "/?token=tok123"} {
		_, err := Start(context.Background(), Options{BaseURL: base})
		if err == nil {
			panic(fmt.Sprintf("%s: expected an error", base))
		}
		for _, secret := range []string{"ciuser", "hunter2", "tok123"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("the error shows %q: %v", secret, err)
			}
		}
		var ne net.Error
		if !strings.Contains(err.Error(), addr) || !errors.As(err, &ne) {
			t.Errorf("the error must still name the host and unwrap to the cause: %v", err)
		}
	}
}

// A reply that is not 2xx is an error whose status can be read without
// matching its text, also through a wrapping error.
func TestStatusErrorCarriesTheCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"session not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	_, err := Attach(context.Background(), Options{BaseURL: srv.URL}, "ts_missing")
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound || se.Message != "session not found" {
		panic(fmt.Sprintf("error %v (%T), want a StatusError with code 404", err, err))
	}
	if StatusCode(fmt.Errorf("wrapped: %w", err)) != http.StatusNotFound || StatusCode(errors.New("x")) != 0 || StatusCode(nil) != 0 {
		panic("StatusCode does not read the code through a wrapping error, or invents one")
	}
	if !strings.Contains(err.Error(), ": 404 Not Found: session not found") {
		panic(fmt.Sprintf("the text of the error changed: %v", err))
	}
}
