package core

import "testing"

func TestUncertainFlag(t *testing.T) {
	for flag, want := range map[string]bool{
		"pk_unknown": true, "pk_not_unique": true, "pk_lookup_failed": true, "after_unavailable": true, "upsert_unknown": true,
		"after_from_args": true, "image_skipped": true, "image_timeout": true,
		"image_skipped:deadline": true, "image_skipped:": true,
		// about the event, not about how far a row image can be trusted
		"redacted": false, "tx_unresolved": false, "orphaned": false, "unattributed": false, "returning_appended": false,
		// says the statement's own RETURNING was partial; the recorded image never is that partial row
		"returning_partial": false,
		"":                  false, ":pk_unknown": false, "PK_UNKNOWN": false, "pk_unknown ": false,
	} {
		if got := UncertainFlag(flag); got != want {
			t.Errorf("UncertainFlag(%q) = %v, want %v", flag, got, want)
		}
	}
	if HasUncertainFlag(nil) || HasUncertainFlag([]string{"redacted", "truncated"}) || !HasUncertainFlag([]string{"redacted", "image_timeout"}) {
		t.Error("HasUncertainFlag")
	}
}
