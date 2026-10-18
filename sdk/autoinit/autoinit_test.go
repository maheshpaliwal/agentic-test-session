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

func TestBindPriorityEnvParse(t *testing.T) {
	got := core.ParseBindPriority("header:X-User-Id,jwt:sub,bad")
	if len(got) != 2 || got[0].String() != "header:x-user-id" || got[1].String() != "jwt_claim:sub" {
		t.Fatalf("%v", got)
	}
}
