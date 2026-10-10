package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// LabelKey marks containers that should be traced; its value is the session name.
const LabelKey = "tsession.id"

// DockerEvent is the subset of `docker events --format '{{json .}}'` we use.
type DockerEvent struct {
	Action      string
	ContainerID string
	Name        string // session name (label value)
}

// ParseDockerEvent parses one JSON line. ok is false for events that are not
// container start/die with the tsession label (those are silently ignored);
// err is only set for malformed JSON.
func ParseDockerEvent(line string) (ev DockerEvent, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return ev, false, nil
	}
	var raw struct {
		Type   string `json:"Type"`
		Action string `json:"Action"`
		Status string `json:"status"`
		ID     string `json:"id"`
		Actor  struct {
			ID         string            `json:"ID"`
			Attributes map[string]string `json:"Attributes"`
		} `json:"Actor"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return ev, false, err
	}
	if raw.Type != "" && raw.Type != "container" {
		return ev, false, nil
	}
	action := raw.Action
	if action == "" {
		action = raw.Status
	}
	if action != "start" && action != "die" {
		return ev, false, nil
	}
	name := raw.Actor.Attributes[LabelKey]
	if name == "" {
		return ev, false, nil
	}
	id := raw.Actor.ID
	if id == "" {
		id = raw.ID
	}
	return DockerEvent{Action: action, ContainerID: id, Name: name}, true, nil
}

// Watcher maps container lifecycle events onto sessions.
type Watcher struct {
	Start func(ctx context.Context, name string) (sessionID string, err error)
	Stop  func(ctx context.Context, sessionID string) error
	Logf  func(format string, args ...any)

	mu       sync.Mutex
	sessions map[string]string // container id -> session id
}

func (w *Watcher) logf(f string, a ...any) {
	if w.Logf != nil {
		w.Logf(f, a...)
	}
}

// Handle applies a single event.
func (w *Watcher) Handle(ctx context.Context, ev DockerEvent) {
	w.mu.Lock()
	if w.sessions == nil {
		w.sessions = map[string]string{}
	}
	sid, tracked := w.sessions[ev.ContainerID]
	w.mu.Unlock()

	switch ev.Action {
	case "start":
		if tracked {
			return
		}
		id, err := w.Start(ctx, ev.Name)
		if err != nil {
			w.logf("start session %q for container %.12s: %v", ev.Name, ev.ContainerID, err)
			return
		}
		w.mu.Lock()
		w.sessions[ev.ContainerID] = id
		w.mu.Unlock()
		w.logf("container %.12s started: session %s (%s)", ev.ContainerID, id, ev.Name)
	case "die":
		if !tracked {
			return
		}
		w.mu.Lock()
		delete(w.sessions, ev.ContainerID)
		w.mu.Unlock()
		if err := w.Stop(ctx, sid); err != nil {
			w.logf("stop session %s: %v", sid, err)
			return
		}
		w.logf("container %.12s died: stopped session %s", ev.ContainerID, sid)
	}
}

// Run consumes newline-delimited events until r is exhausted or ctx ends.
func (w *Watcher) Run(ctx context.Context, r io.Reader) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ev, ok, err := ParseDockerEvent(sc.Text())
		if err != nil {
			w.logf("ignoring malformed event: %v", err)
			continue
		}
		if ok {
			w.Handle(ctx, ev)
		}
	}
	return sc.Err()
}

// dockerEventsArgs are the arguments to `docker events`.
var dockerEventsArgs = []string{
	"events", "--format", "{{json .}}",
	"--filter", "type=container",
	"--filter", "event=start", "--filter", "event=die",
	"--filter", "label=" + LabelKey,
}

// RunDocker runs `docker events`, reconnecting with backoff until ctx ends.
func (w *Watcher) RunDocker(ctx context.Context, dockerBin string) error {
	backoff := time.Second
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, dockerBin, dockerEventsArgs...)
		out, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start %s: %w", dockerBin, err)
		}
		started := time.Now()
		_ = w.Run(ctx, out)
		_ = cmd.Wait()
		if ctx.Err() != nil {
			break
		}
		if time.Since(started) > 30*time.Second {
			backoff = time.Second
		}
		log.Printf("docker events exited; retrying in %s", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
	return nil
}
