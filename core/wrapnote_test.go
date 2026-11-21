package core

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
)

func TestNoteWrapWhileDisabledKeepsTheFirst(t *testing.T) {
	wrappedWhileOff = nil
	defer func() { wrappedWhileOff = nil }()
	if got := WrappedWhileDisabled(); got != "" {
		panic(fmt.Sprintf("nothing noted yet, got %q", got))
	}
	NoteWrapWhileDisabled("thttp.Handler")
	NoteWrapWhileDisabled("tsql.Register")
	NoteWrapWhileDisabled("thttp.Handler")
	if got := WrappedWhileDisabled(); got != "thttp.Handler" {
		panic(fmt.Sprintf("got %q", got))
	}
	if got := WrappersWhileDisabled(); !slices.Equal(got, []string{"thttp.Handler", "tsql.Register"}) {
		panic(fmt.Sprintf("got %q", got))
	}
	for i := range 100 {
		NoteWrapWhileDisabled("w" + strconv.Itoa(i))
	}
	if n := len(WrappersWhileDisabled()); n != maxWrapNotes {
		panic(fmt.Sprintf("%d names kept, want %d", n, maxWrapNotes))
	}
}
