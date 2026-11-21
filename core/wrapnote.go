package core

import (
	"slices"
	"sync"
)

// maxWrapNotes bounds the wrapper names kept: a handful tells which packages
// were set up too early.
const maxWrapNotes = 16

var (
	wrapNoteMu      sync.Mutex
	wrappedWhileOff []string // distinct names, in the order of the first call
)

// NoteWrapWhileDisabled is called by a wrapper constructor that passes its
// argument through because the SDK is not enabled (thttp.Handler,
// tredis.Wrap, the tgrpc interceptors, ...). If the SDK is started later,
// that handler, client or driver stays unwrapped and records nothing, which
// is otherwise silent: the runtime reports it at start. A wrapper that looks
// the SDK up on every call (tredis.Hook, tqueue.Publish) records once the SDK
// runs and must not call this.
func NoteWrapWhileDisabled(wrapper string) {
	wrapNoteMu.Lock()
	defer wrapNoteMu.Unlock()
	if len(wrappedWhileOff) < maxWrapNotes && !slices.Contains(wrappedWhileOff, wrapper) {
		wrappedWhileOff = append(wrappedWhileOff, wrapper)
	}
}

// WrappedWhileDisabled returns the name of the first wrapper that was called
// before the SDK was enabled, or "".
func WrappedWhileDisabled() string {
	wrapNoteMu.Lock()
	defer wrapNoteMu.Unlock()
	if len(wrappedWhileOff) == 0 {
		return ""
	}
	return wrappedWhileOff[0]
}

// WrappersWhileDisabled returns the names of the wrappers that were called
// before the SDK was enabled, each once, in the order of their first call.
func WrappersWhileDisabled() []string {
	wrapNoteMu.Lock()
	defer wrapNoteMu.Unlock()
	return slices.Clone(wrappedWhileOff)
}
