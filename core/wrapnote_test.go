package core

import (
	"slices"
	"strconv"
	"testing"
)

func TestNoteWrapWhileDisabledKeepsTheFirst(t *testing.T) {
	wrappedWhileOff = nil
	defer func() { wrappedWhileOff = nil }()
	if got := WrappedWhileDisabled(); got != "" {
		t.Fatalf("nothing noted yet, got %q", got)
	}
	NoteWrapWhileDisabled("thttp.Handler")
	NoteWrapWhileDisabled("tsql.Register")
	NoteWrapWhileDisabled("thttp.Handler")
	if got := WrappedWhileDisabled(); got != "thttp.Handler" {
		t.Fatalf("got %q", got)
	}
	if got := WrappersWhileDisabled(); !slices.Equal(got, []string{"thttp.Handler", "tsql.Register"}) {
		t.Fatalf("got %q", got)
	}
	for i := range 100 {
		NoteWrapWhileDisabled("w" + strconv.Itoa(i))
	}
	if n := len(WrappersWhileDisabled()); n != maxWrapNotes {
		t.Fatalf("%d names kept, want %d", n, maxWrapNotes)
	}
}
