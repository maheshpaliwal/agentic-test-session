package core

import "sync/atomic"

// wrappedWhileOff holds the name of the first wrapper that was called while
// the SDK was disabled and therefore returned its argument untouched.
var wrappedWhileOff atomic.Pointer[string]

// NoteWrapWhileDisabled is called by a wrapper (thttp.Handler, tsql.Register,
// ...) that passes its argument through because the SDK is not enabled. If
// the SDK is started later, that handler or driver stays unwrapped and records
// nothing, which is otherwise silent: the runtime reports it at start.
func NoteWrapWhileDisabled(wrapper string) {
	wrappedWhileOff.CompareAndSwap(nil, &wrapper)
}

// WrappedWhileDisabled returns the name of the first wrapper that was called
// before the SDK was enabled, or "".
func WrappedWhileDisabled() string {
	if p := wrappedWhileOff.Load(); p != nil {
		return *p
	}
	return ""
}
