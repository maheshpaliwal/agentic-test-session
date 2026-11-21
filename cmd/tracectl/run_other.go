//go:build !unix

package main

import (
	"errors"
	"os"
	"os/exec"
)

var runSignals = []os.Signal{os.Interrupt}

func ownGroup(*exec.Cmd) {}

// forward sends sig to p. Where a process cannot be sent that signal
// (Windows), it is killed instead.
func forward(p *os.Process, sig os.Signal) {
	if err := p.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		_ = p.Kill()
	}
}

func killCommand(p *os.Process) { _ = p.Kill() }

func surviveBrokenPipe() (restore func()) { return func() {} }
