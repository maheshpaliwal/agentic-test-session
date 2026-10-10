package autoinit

import (
	"net/http"
	"testing"

	"tsession/core"
)

func TestNoopWhenDisabled(t *testing.T) {
	// Importing the package (init already ran without TSESSION_ENABLED=true)
	// must leave everything untouched.
	if core.Enabled() {
		t.Fatal("core enabled")
	}
	if _, ok := http.DefaultTransport.(*http.Transport); !ok {
		t.Fatalf("DefaultTransport replaced: %T", http.DefaultTransport)
	}
}
