package core

import "testing"

func TestRecoverSwallowsPanic(t *testing.T) {
	func() {
		defer Recover()
		panic("boom")
	}()
	func() { defer Recover() }() // no panic: no-op
}
