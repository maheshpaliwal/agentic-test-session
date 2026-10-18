package core

// Recover swallows a panic. Use it as `defer core.Recover()` around recording
// code so instrumentation can never break the host call; it is a no-op when
// nothing panicked. (recover only works when called directly by the deferred
// function, which is why this is a function to defer, not a helper to call.)
func Recover() { _ = recover() }
