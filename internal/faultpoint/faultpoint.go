// Package faultpoint lets a test make a named point of the code panic, to
// prove what the code around it promises about a panic: that a lock is
// released, that a loop is started again, that bookkeeping is put right.
// Those promises cannot be tested from outside, since the code they guard
// does not panic by itself.
//
// Outside a test no hook is set and Hit is one atomic load. It is not placed
// on a path that runs once per recorded event.
package faultpoint

import "sync/atomic"

var hook atomic.Pointer[func(name string)]

// Hit marks the point name. It does nothing unless a test has set a hook.
func Hit(name string) {
	if f := hook.Load(); f != nil {
		(*f)(name)
	}
}

// Set makes f run at every point that is hit, on the goroutine that hits it,
// and returns what takes it back. Only tests call it, and not in parallel.
func Set(f func(name string)) (restore func()) {
	hook.Store(&f)
	return func() { hook.Store(nil) }
}

// PanicAt makes the point name panic the next times times it is hit (below
// zero: every time). hits reports how often it has panicked so far.
func PanicAt(name string, times int) (restore func(), hits func() int) {
	var n, left atomic.Int64
	left.Store(int64(times))
	restore = Set(func(at string) {
		if at != name || times >= 0 && left.Add(-1) < 0 {
			return
		}
		n.Add(1)
		panic("faultpoint " + name)
	})
	return restore, func() int { return int(n.Load()) }
}
