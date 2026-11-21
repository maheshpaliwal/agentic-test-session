package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

// A transport error names the URL. What may be a credential in it (user
// name, password, query) must not reach the message, which tracectl and
// tests print into CI logs; the cause must still be reachable.
func TestTransportErrorHidesCredentialsInTheURL(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	for _, base := range []string{"http://ciuser:hunter2@" + addr, "http://" + addr + "/?token=tok123"} {
		_, err := Start(context.Background(), Options{BaseURL: base})
		if err == nil {
			t.Fatalf("%s: expected an error", base)
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
