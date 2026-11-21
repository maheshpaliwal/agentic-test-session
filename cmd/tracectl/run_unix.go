//go:build unix

package main

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// runSignals are the signals run handles itself instead of dying of them
// with the session still active: it passes them on to the command and then
// stops the session.
var runSignals = []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT}

// ownGroup starts the command in a process group of its own. A Ctrl-C in a
// terminal goes to the whole foreground group, tracectl and the command, and
// tracectl forwards what it receives: in one group the command would get
// every signal twice, and many programs take a second SIGINT as "quit at
// once". With a group of its own it gets the forwarded one only, and so do
// the processes it started. The price: the command is not in the terminal's
// foreground group, so it cannot read from the terminal (a pipe or file on
// standard input is unaffected).
func ownGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

// forward sends sig to the command's process group.
func forward(p *os.Process, sig os.Signal) {
	if s, ok := sig.(syscall.Signal); ok && syscall.Kill(-p.Pid, s) == nil {
		return
	}
	_ = p.Signal(sig)
}

// killCommand ends the command's process group at once.
func killCommand(p *os.Process) {
	if syscall.Kill(-p.Pid, syscall.SIGKILL) != nil {
		_ = p.Kill()
	}
}

// surviveBrokenPipe keeps tracectl alive when what it prints has nowhere to
// go (standard error piped into a program that has exited): by default a
// write to a broken standard error kills a Go program, and the session would
// stay active. The signal is caught, not ignored, so the command still
// starts with the default.
func surviveBrokenPipe() (restore func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGPIPE)
	return func() { signal.Stop(ch) }
}
