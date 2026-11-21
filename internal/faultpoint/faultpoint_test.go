package faultpoint

import (
	"fmt"
	"testing"
)

// hit reports whether the point name panics.
func hit(name string) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	Hit(name)
	return false
}

// Without a hook a point does nothing; PanicAt makes the named point, and
// no other, panic as often as it says; restore takes the hook back.
func TestPanicAt(t *testing.T) {
	if hit("a") {
		panic("a point panicked without a hook")
	}
	restore, hits := PanicAt("a", 2)
	if hit("b") || !hit("a") || !hit("a") || hit("a") || hits() != 2 {
		panic(fmt.Sprintf("two panics at a were asked for, %d counted", hits()))
	}
	restore()

	restore, hits = PanicAt("a", -1)
	for range 5 {
		if !hit("a") {
			panic("a point that panics every time did not")
		}
	}
	restore()
	if hit("a") || hits() != 5 {
		panic(fmt.Sprintf("after restore: %d panics counted", hits()))
	}
}
