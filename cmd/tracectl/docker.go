package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// LabelKey marks containers that should be traced; its value is the session name.
const LabelKey = "tsession.id"

// BindLabelKey optionally carries bindings for the session, e.g.
// "header:x-user-id=u_123,jwt:sub=u_1" (comma-separated source:name=value).
// Containers share a session only if both labels are identical.
const BindLabelKey = "tsession.bind"

// NameLabelKey optionally sets the session display name; it supports the
// server-side templates ({bind}, {bind.<name>}, {initiated_by}, {time}, {date}).
// Without it the tsession.id value is used as the name.
const NameLabelKey = "tsession.name"

// DockerEvent is the subset of `docker events --format '{{json .}}'` we use.
type DockerEvent struct {
	Action      string
	ContainerID string
	Name        string // session name (label value)
	Bind        string // raw tsession.bind label value (may be empty)
	SessName    string // raw tsession.name label value (may be empty)
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
	return DockerEvent{Action: action, ContainerID: id, Name: name, Bind: raw.Actor.Attributes[BindLabelKey], SessName: raw.Actor.Attributes[NameLabelKey]}, true, nil
}

// Runner executes a command and returns its stdout (injected in tests).
type Runner func(ctx context.Context, bin string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, bin string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, bin, args...).Output()
}

// Watcher maps container lifecycle events onto sessions. Containers that carry
// the same tsession.id label share ONE session: it is started when the first
// such container comes up and stopped when the last one is gone.
type Watcher struct {
	Start func(ctx context.Context, name string, bindings []client.Binding) (sessionID string, err error)
	Stop  func(ctx context.Context, sessionID string) error
	Logf  func(format string, args ...any)
	// Run executes docker commands for listing; defaults to os/exec.
	Runner Runner

	mu         sync.Mutex         // serialises Handle/Reconcile
	containers map[string]string  // container id -> session key (name + bindings)
	sessions   map[string]*shared // session key -> shared session
}

type shared struct {
	id   string
	refs int
}

func (w *Watcher) logf(f string, a ...any) {
	if w.Logf != nil {
		w.Logf(f, a...)
	}
}

// Handle applies a single event.
func (w *Watcher) Handle(ctx context.Context, ev DockerEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handleLocked(ctx, ev)
}

func (w *Watcher) handleLocked(ctx context.Context, ev DockerEvent) {
	if w.containers == nil {
		w.containers = map[string]string{}
		w.sessions = map[string]*shared{}
	}
	_, tracked := w.containers[ev.ContainerID]

	switch ev.Action {
	case "start":
		if tracked {
			return
		}
		binds, err := parseBindLabel(ev.Bind)
		if err != nil {
			w.logf("container %.12s: ignoring: bad %s label %q: %v", ev.ContainerID, BindLabelKey, ev.Bind, err)
			return
		}
		sname := ev.Name
		if strings.TrimSpace(ev.SessName) != "" {
			sname = ev.SessName
		}
		key := sessionKey(ev.Name+"\x02"+ev.SessName, binds)
		if sh := w.sessions[key]; sh != nil {
			sh.refs++
			w.containers[ev.ContainerID] = key
			w.logf("container %.12s joined session %s (%s, %d containers)", ev.ContainerID, sh.id, ev.Name, sh.refs)
			return
		}
		id, err := w.Start(ctx, sname, binds)
		if err != nil {
			w.logf("start session %q for container %.12s: %v", ev.Name, ev.ContainerID, err)
			return
		}
		w.sessions[key] = &shared{id: id, refs: 1}
		w.containers[ev.ContainerID] = key
		w.logf("container %.12s started: session %s (%s)", ev.ContainerID, id, ev.Name)
	case "die":
		if !tracked {
			return
		}
		key := w.containers[ev.ContainerID]
		delete(w.containers, ev.ContainerID)
		sh := w.sessions[key]
		if sh == nil {
			return
		}
		if sh.refs--; sh.refs > 0 {
			w.logf("container %.12s died; session %s still has %d container(s)", ev.ContainerID, sh.id, sh.refs)
			return
		}
		delete(w.sessions, key)
		if err := w.Stop(ctx, sh.id); err != nil {
			w.logf("stop session %s: %v", sh.id, err)
			return
		}
		w.logf("container %.12s died: stopped session %s", ev.ContainerID, sh.id)
	}
}

// sessionKey identifies a shared session: the name plus its canonical
// (sorted, header-lowercased) bindings.
func sessionKey(name string, binds []client.Binding) string {
	parts := make([]string, len(binds))
	for i, b := range binds {
		if b.Source == "header" {
			b.Name = strings.ToLower(b.Name)
		}
		parts[i] = b.Source + "\x00" + b.Name + "\x00" + b.Value
	}
	sort.Strings(parts)
	return name + "\x01" + strings.Join(parts, "\x01")
}

// Reconcile aligns tracked state with the set of currently running labelled
// containers: unseen ones are started/joined, tracked ones that are gone are
// treated as died (stopping their session when the last one leaves).
func (w *Watcher) Reconcile(ctx context.Context, running []DockerEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	alive := make(map[string]struct{}, len(running))
	for _, c := range running {
		alive[c.ContainerID] = struct{}{}
	}
	var gone []DockerEvent
	for cid := range w.containers {
		if _, ok := alive[cid]; !ok {
			gone = append(gone, DockerEvent{Action: "die", ContainerID: cid})
		}
	}
	for _, ev := range gone {
		w.handleLocked(ctx, ev)
	}
	for _, c := range running {
		c.Action = "start"
		w.handleLocked(ctx, c)
	}
}

// ParseDockerPS parses `docker ps --format '{{.ID}}\t{{.Label "tsession.id"}}'` output.
func ParseDockerPS(out []byte) []DockerEvent {
	var res []DockerEvent
	for _, l := range strings.Split(string(out), "\n") {
		l = strings.TrimRight(l, "\r")
		id, rest, ok := strings.Cut(l, "\t")
		name, rest, _ := strings.Cut(rest, "\t")
		bind, sname, _ := strings.Cut(rest, "\t")
		id, name, bind, sname = strings.TrimSpace(id), strings.TrimSpace(name), strings.TrimSpace(bind), strings.TrimSpace(sname)
		if !ok || id == "" || name == "" {
			continue
		}
		res = append(res, DockerEvent{Action: "start", ContainerID: id, Name: name, Bind: bind, SessName: sname})
	}
	return res
}

// dockerPSArgs lists running labelled containers.
var dockerPSArgs = []string{"ps", "--no-trunc", "--filter", "label=" + LabelKey,
	"--format", "{{.ID}}\t{{.Label \"" + LabelKey + "\"}}\t{{.Label \"" + BindLabelKey + "\"}}\t{{.Label \"" + NameLabelKey + "\"}}"}

// ListRunning returns running labelled containers via docker ps.
func (w *Watcher) ListRunning(ctx context.Context, dockerBin string) ([]DockerEvent, error) {
	run := w.Runner
	if run == nil {
		run = execRunner
	}
	out, err := run(ctx, dockerBin, dockerPSArgs...)
	if err != nil {
		return nil, err
	}
	return ParseDockerPS(out), nil
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
// After every (re)connect it lists running labelled containers and reconciles,
// so containers started before the watcher or during a gap are attached and
// sessions whose containers vanished while disconnected are stopped. The
// event stream is opened before listing so nothing falls between the two.
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
		if running, err := w.ListRunning(ctx, dockerBin); err != nil {
			if ctx.Err() == nil {
				log.Printf("docker ps: %v (skipping reconcile)", err)
			}
		} else {
			w.Reconcile(ctx, running)
		}
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
