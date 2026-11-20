package core

import "testing"

func TestNoteWrapWhileDisabledKeepsTheFirst(t *testing.T) {
	wrappedWhileOff.Store(nil)
	defer wrappedWhileOff.Store(nil)
	if got := WrappedWhileDisabled(); got != "" {
		t.Fatalf("nothing noted yet, got %q", got)
	}
	NoteWrapWhileDisabled("thttp.Handler")
	NoteWrapWhileDisabled("tsql.Register")
	if got := WrappedWhileDisabled(); got != "thttp.Handler" {
		t.Fatalf("got %q", got)
	}
}
