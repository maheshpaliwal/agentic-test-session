package ident

import (
	"strings"
	"testing"
)

func TestClean(t *testing.T) {
	for in, want := range map[string]string{
		"orders-api":     "orders-api",
		"orders-api ":    "orders-api",
		"  orders api\t": "orders api",
		"pay\x1bments":   "payments",
		"hé-1":           "h-1",
		"":               "",
		" \t ":           "",
	} {
		if got := Clean(in); got != want {
			t.Errorf("Clean(%q) = %q, want %q", in, got, want)
		}
		if got := Clean(Clean(in)); got != want {
			t.Errorf("Clean is not idempotent for %q: %q", in, got)
		}
	}
	// The cut does not leave a trailing space behind, or a name would differ
	// from itself after a second cleaning.
	long := strings.Repeat("a", MaxLen-1) + " b"
	if got := Clean(long); got != strings.Repeat("a", MaxLen-1) || Clean(got) != got {
		t.Errorf("cut at the limit: %q", got)
	}
	if got := CleanN("abcdef", 3); got != "abc" {
		t.Errorf("CleanN: %q", got)
	}
}
