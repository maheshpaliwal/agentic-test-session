package controlplane

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const stoppedPrefix = "service orders-api (instance orders-1-abc) stopped recording after an internal error ("

func stoppedMgr(col Collector, logs *logBuffer, mut func(*Config)) *Manager {
	cfg := Config{Collector: col, PollInterval: 2 * time.Millisecond, TimeUnit: 10 * time.Millisecond, Logger: logs.logger()}
	if mut != nil {
		mut(&cfg)
	}
	return NewManager(cfg)
}

func mustStart(m *Manager, name string) Session {
	s, err := m.Start(StartRequest{Name: name, QuiesceSeconds: 1})
	if err != nil {
		panic(err)
	}
	return s
}

func mustSeal(t *testing.T, m *Manager, id string) Session {
	if _, err := m.Stop(id); err != nil {
		panic(err)
	}
	return waitState(t, m, id, StateSealed)
}

func sayStopped(t *testing.T, m *Manager, body string) {
	if w := do(t, m.Handler(), "POST", "/v1/delivery", "127.0.0.1:7070", body, map[string]string{"Content-Type": "application/json"}); w.Code != http.StatusOK {
		panic(fmt.Sprintf("the batch: %d %s", w.Code, w.Body.String()))
	}
}

// An SDK whose subscriber panicked stops recording and says so. Nothing else
// would show it: the sessions it took part in sealed complete, without the
// events the service would have recorded. A session that has a delivery
// report of the instance is incomplete, with a reason that names the service
// and the instance. A session that ran then and that nothing ties to the
// instance says so as a warning. A session started later, or ended before,
// says nothing: the instance took no part in it.
func TestAnInstanceThatStoppedRecordingMakesTheSessionsItTookPartInIncomplete(t *testing.T) {
	var logs logBuffer
	col := newFlaky()
	col.failing.Store(false)
	m := stoppedMgr(col, &logs, nil)
	defer m.Close()
	done := mustSeal(t, m, mustStart(m, "ended before").ID)
	running, draining, bystander := mustStart(m, "running"), mustStart(m, "draining"), mustStart(m, "bystander")
	col.busy.Store(true) // the drain of "draining" waits
	if _, err := m.Stop(draining.ID); err != nil {
		panic(err)
	}

	// As the SDK sends it, over HTTP: the note with the reports of the
	// sessions it recorded events for.
	body := fmt.Sprintf(`{"service":"orders-api","instance":"orders-1-abc","reports":[
		{"session":%q,"recorded":2,"delivered":2},{"session":%q,"recorded":1,"delivered":1}],
		"stopped":"its subscriber panicked repeatedly and stays stopped; it records nothing more \u001b[31m http://u:hunter2@h/ `+strings.Repeat("x ", 400)+`"}`,
		running.ID, draining.ID)
	sayStopped(t, m, body)
	later := mustStart(m, "started later")
	col.busy.Store(false)

	for _, s := range []Session{mustSeal(t, m, running.ID), waitState(t, m, draining.ID, StateSealed)} {
		if !s.Incomplete || len(s.IncompleteReasons) != 1 {
			panic(fmt.Sprintf("%s: %+v", s.Name, s))
		}
		r := s.IncompleteReasons[0]
		if !strings.HasPrefix(r, stoppedPrefix+"its subscriber panicked repeatedly and stays stopped") ||
			!strings.Contains(r, "events of this session it would have recorded since are missing") {
			panic(s.Name + ": " + r)
		}
		if strings.Contains(r, "hunter2") || strings.Contains(r, "\x1b") || len(r) > 700 {
			panic(fmt.Sprintf("%s: the instance's words are stored as they came (%d bytes): %q", s.Name, len(r), r))
		}
		for _, u := range s.DeliveryUnconfirmed {
			if strings.Contains(u, "stopped recording") {
				panic(s.Name + ": named twice: " + u)
			}
		}
	}
	if s := mustSeal(t, m, bystander.ID); s.Incomplete || len(s.IncompleteReasons) != 0 || len(s.DeliveryUnconfirmed) != 1 ||
		!strings.HasPrefix(s.DeliveryUnconfirmed[0], stoppedPrefix) ||
		!strings.Contains(s.DeliveryUnconfirmed[0], "whether it took part is unknown: it may have missed events it should have recorded") ||
		strings.Contains(s.DeliveryUnconfirmed[0], "hunter2") {
		panic(fmt.Sprintf("a session nothing ties to the instance: %+v", s))
	}
	for _, s := range []Session{mustSeal(t, m, later.ID), done} {
		if got, _ := m.Get(s.ID); got.Incomplete || len(got.IncompleteReasons) != 0 || len(got.DeliveryUnconfirmed) != 0 || len(got.StoppedInstances) != 0 {
			panic(fmt.Sprintf("%s did not run when the instance stopped: %+v", s.Name, got))
		}
	}
	if out := logs.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "a service says that it stopped recording after an internal error") ||
		!strings.Contains(out, "service=orders-api") || strings.Contains(out, "hunter2") {
		t.Errorf("log:\n%s", out)
	}
}

// Events of the instance at the collector tie it to the session as a report
// does: an instance whose reports do not arrive, and whose note does.
func TestAnInstanceThatStoppedRecordingWithEventsAtTheCollectorMakesTheSessionIncomplete(t *testing.T) {
	col := newScriptCollector().counting()
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	s := mustStart(m, "p")
	col.has("orders-api", "orders-1-abc", 3)
	sayStopped(t, m, `{"service":"orders-api","instance":"orders-1-abc","reports":[],"stopped":"its subscriber panicked"}`)
	got := mustSeal(t, m, s.ID)
	if !got.Incomplete || len(got.IncompleteReasons) != 1 || !strings.HasPrefix(got.IncompleteReasons[0], stoppedPrefix+"its subscriber panicked)") ||
		len(got.DeliveryUnconfirmed) != 0 {
		panic(fmt.Sprintf("%+v", got))
	}
}

// The review's finding: one request, by an instance that reported for no
// session, made every running session incomplete (50 of 50). A single
// subscriber panic that passes sends such a request, and so can anyone who
// reaches the control plane. Now none is incomplete, and each says once what
// it was told.
func TestOneStoppedNoteDoesNotMakeSessionsIncompleteThatNothingTiesItTo(t *testing.T) {
	col := newFlaky()
	col.failing.Store(false)
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	var ss []Session
	for i := range 50 {
		ss = append(ss, mustStart(m, fmt.Sprintf("s%d", i)))
	}
	for range 3 { // as an SDK does while its subscriber is started again
		sayStopped(t, m, `{"service":"orders-api","instance":"orders-1-abc","reports":[],"stopped":"its subscriber panicked"}`)
	}
	for _, s := range ss {
		if _, err := m.Stop(s.ID); err != nil {
			panic(err)
		}
	}
	for _, s := range ss {
		got := waitState(t, m, s.ID, StateSealed)
		if got.Incomplete || len(got.IncompleteReasons) != 0 || len(got.DeliveryUnconfirmed) != 1 || !strings.HasPrefix(got.DeliveryUnconfirmed[0], stoppedPrefix) {
			panic(fmt.Sprintf("%s: %+v", got.Name, got))
		}
	}
}

// What one caller can add to a session is bounded: 1200 made-up instances
// leave 64 notes with the session and no more lines than a list holds, and
// the session is not incomplete. A note of an instance the session has a
// report of is still kept, in the place of a made-up one, and counts.
func TestStoppedNotesOfMadeUpInstancesAreBoundedAndDoNotKeepARealOneOut(t *testing.T) {
	col := newFlaky()
	col.failing.Store(false)
	m := stoppedMgr(col, &logBuffer{}, nil)
	defer m.Close()
	victim, other := mustStart(m, "victim"), mustStart(m, "other")
	for i := range 1200 {
		sayStopped(t, m, fmt.Sprintf(`{"service":"x","instance":"ghost-%d","reports":[],"stopped":"boo %s"}`, i, strings.Repeat("y", 300)))
	}
	sayStopped(t, m, fmt.Sprintf(`{"service":"orders-api","instance":"orders-1-abc","reports":[{"session":%q,"recorded":1,"delivered":1}],"stopped":"its subscriber panicked"}`, victim.ID))

	got := mustSeal(t, m, victim.ID)
	if !got.Incomplete || len(got.IncompleteReasons) != 1 || !strings.HasPrefix(got.IncompleteReasons[0], stoppedPrefix) {
		panic(fmt.Sprintf("the instance that took part is not named: incomplete=%v %q", got.Incomplete, got.IncompleteReasons))
	}
	for _, s := range []Session{got, mustSeal(t, m, other.ID)} {
		if len(s.StoppedInstances) != maxStoppedInstances || !s.StoppedInstancesMore || len(s.DeliveryUnconfirmed) != maxDeliveryReasons+1 ||
			!strings.HasPrefix(s.DeliveryUnconfirmed[maxDeliveryReasons], "and ") {
			panic(fmt.Sprintf("%s: %d notes, more=%v, %d lines unconfirmed: %q", s.Name, len(s.StoppedInstances), s.StoppedInstancesMore,
				len(s.DeliveryUnconfirmed), s.DeliveryUnconfirmed))
		}
		if b, _ := json.Marshal(s); len(b) > 64<<10 {
			panic(fmt.Sprintf("%s: the session is %d bytes", s.Name, len(b)))
		}
	}
	if s, _ := m.Get(other.ID); s.Incomplete || len(s.IncompleteReasons) != 0 {
		panic(fmt.Sprintf("made-up instances made a session incomplete: %q", s.IncompleteReasons))
	}
}

// The review's second finding: the note lived in memory only, so a control
// plane that was restarted between the note and the end of the session (it
// exits when its panic budget is spent, and is started again) sealed the
// session complete, with no reason. It is stored with the session now.
func TestAStoppedNoteSurvivesARestartOfTheControlPlane(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	col := newFlaky()
	col.failing.Store(false)
	open := func() (*Manager, *FileStore) {
		fs, err := NewFileStore(path)
		if err != nil {
			panic(err)
		}
		return stoppedMgr(col, &logBuffer{}, func(c *Config) { c.Store = fs }), fs
	}
	m, fs := open()
	tookPart, bystander := mustStart(m, "took part"), mustStart(m, "bystander")
	if _, err := m.ReportDeliveries(DeliveryBatch{Service: "orders-api", Instance: "orders-1-abc",
		Reports: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"session":%q,"recorded":1,"delivered":1}`, tookPart.ID))},
		Stopped: "its subscriber panicked repeatedly and stays stopped"}); err != nil {
		panic(err)
	}
	m.Close()
	if err := fs.Close(); err != nil {
		panic(err)
	}

	m, fs = open()
	defer func() {
		m.Close()
		if err := fs.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if got := mustSeal(t, m, tookPart.ID); !got.Incomplete || len(got.IncompleteReasons) != 1 ||
		!strings.HasPrefix(got.IncompleteReasons[0], stoppedPrefix+"its subscriber panicked repeatedly and stays stopped)") {
		panic(fmt.Sprintf("after the restart: %+v", got))
	}
	if got := mustSeal(t, m, bystander.ID); got.Incomplete || len(got.DeliveryUnconfirmed) != 1 || !strings.HasPrefix(got.DeliveryUnconfirmed[0], stoppedPrefix) {
		panic(fmt.Sprintf("after the restart: %+v", got))
	}
}

// A session is written when a note says something new about it, not every
// time an instance repeats itself; what it said last is what is kept.
func TestARepeatedStoppedNoteIsStoredOnce(t *testing.T) {
	s := Session{ID: "ts_1"}
	n := StoppedInstance{Service: "svc", Instance: "i", Why: "restarting", At: time.Unix(1, 0)}
	list, more, changed := noteStopped(s, nil, n)
	if !changed || more || len(list) != 1 {
		panic(fmt.Sprintf("%v %v %v", list, more, changed))
	}
	s.StoppedInstances = list
	if _, _, changed := noteStopped(s, nil, n); changed {
		panic("the same note changed the session")
	}
	later := n
	later.Why, later.At, later.Connected = "records again", time.Unix(9, 0), true
	list, _, changed = noteStopped(s, nil, later)
	if !changed || len(list) != 1 || list[0].Why != "records again" || !list[0].At.Equal(time.Unix(1, 0)) || !list[0].Connected {
		panic(fmt.Sprintf("%+v %v", list, changed))
	}
	if s.StoppedInstances[0].Why != "restarting" {
		panic("the stored list was changed in place")
	}
}

// With the delivery check turned off nothing makes a session incomplete; the
// stop is still said, as a warning.
func TestAnInstanceThatStoppedRecordingIsAWarningWithTheCheckOff(t *testing.T) {
	col := newFlaky()
	col.failing.Store(false)
	m := stoppedMgr(col, &logBuffer{}, func(c *Config) { c.DeliveryGrace = -1 })
	defer m.Close()
	s := mustStart(m, "p")
	// Told twice, it is said once. Its report ties it to the session.
	for range 2 {
		if _, err := m.ReportDeliveries(DeliveryBatch{Service: "svc", Instance: "i", Stopped: "its subscriber panicked",
			Reports: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"session":%q,"recorded":1,"delivered":1,"final":true}`, s.ID))}}); err != nil {
			panic(err)
		}
	}
	got := mustSeal(t, m, s.ID)
	said := 0
	for _, u := range got.DeliveryUnconfirmed {
		if strings.Contains(u, "service svc (instance i) stopped recording after an internal error") {
			said++
		}
	}
	if got.Incomplete || len(got.IncompleteReasons) != 0 || said != 1 {
		panic(fmt.Sprintf("%+v", got))
	}
}
