package controlplane

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// logBuffer collects log output; safe for concurrent use.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (l *logBuffer) logger() *slog.Logger { return slog.New(slog.NewTextHandler(l, nil)) }

// damageArchive cuts the archive file of a live store down to size bytes and
// returns a func that puts the old content back (into the same file, so the
// store's handle and offsets are good again).
func damageArchive(t *testing.T, fs *FileStore, size int64) (repair func()) {
	t.Helper()
	old, err := os.ReadFile(fs.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(fs.archivePath(), size); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		fh, err := os.OpenFile(fs.archivePath(), os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer fh.Close()
		if _, err := fh.WriteAt(old, 0); err != nil {
			t.Fatal(err)
		}
	}
}

// An archived session whose line cannot be read is not the same as a session
// the store does not have: GetErr says which, and every such session is
// counted and logged, also when a list leaves it out.
func TestFileStoreReadErrorsAreReported(t *testing.T) {
	var logs logBuffer
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path, WithStoreLogger(logs.logger()))
	closeLater(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	uncached(fs)
	var ss []Session
	for i := range 4 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
	}
	ss[3].State, ss[3].SealPending = StateSealed, true
	mustPut(t, fs, ss...)
	for _, s := range ss {
		fs.Evict(s.ID)
	}
	wantStore(t, "before", fs, ss)
	if n := fs.ReadErrors(); n != 0 || logs.String() != "" {
		t.Fatalf("read errors before the damage: %d, log %q", n, logs.String())
	}

	// The file ends in the middle of the line of s2.
	repair := damageArchive(t, fs, fs.arch[ss[2].ID].off+10)
	for i, s := range ss {
		got, ok, err := fs.GetErr(s.ID)
		if readable := i < 2; ok != readable || (err == nil) != readable || ok && got.Name != s.Name {
			t.Fatalf("GetErr %s: %+v %v %v", s.ID, got, ok, err)
		}
		if _, ok := fs.Get(s.ID); ok != (i < 2) {
			t.Fatalf("Get %s: %v", s.ID, ok)
		}
	}
	if _, ok, err := fs.GetErr("ts_nope"); ok || err != nil {
		t.Fatalf("GetErr of an unknown id: %v %v, want no session and no error", ok, err)
	}
	if n := fs.ReadErrors(); n != 4 { // s2 and s3, through GetErr and through Get
		t.Fatalf("read errors after four failed gets: %d", n)
	}
	if l := logs.String(); !strings.Contains(l, ss[2].ID) || !strings.Contains(l, ss[3].ID) || !strings.Contains(l, "level=ERROR") {
		t.Fatalf("failed gets not logged:\n%s", l)
	}

	wantSameSessions(t, "list", fs.List(), ss[:2])
	if n := fs.ReadErrors(); n != 6 {
		t.Fatalf("read errors after a list that left out two sessions: %d, want 6", n)
	}
	if l := logs.String(); !strings.Contains(l, "missing from a list") || !strings.Contains(l, "sessions=2") {
		t.Fatalf("incomplete list not logged:\n%s", l)
	}
	if got := fs.ListPending(); len(got) != 0 { // reads the line of s3 only
		t.Fatalf("list pending: %+v", got)
	}
	if got := fs.ListState(StateAborted); len(got) != 2 { // reads s0, s1, s2
		t.Fatalf("list state: %+v", got)
	}
	if n := fs.ReadErrors(); n != 8 {
		t.Fatalf("read errors after ListPending and ListState: %d, want 8", n)
	}

	// Once the file is whole again, so are the answers; nothing more is counted.
	repair()
	wantStore(t, "repaired", fs, ss)
	if n := fs.ReadErrors(); n != 8 {
		t.Fatalf("read errors after the repair: %d, want 8", n)
	}

	// The file cannot be read at all (its handle is gone).
	if err := fs.ar.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range ss {
		if _, ok, err := fs.GetErr(s.ID); ok || err == nil {
			t.Fatalf("GetErr %s from an unreadable archive: %v %v", s.ID, ok, err)
		}
	}
	if got := fs.List(); len(got) != 0 || fs.ReadErrors() != 16 {
		t.Fatalf("list from an unreadable archive: %d sessions, %d read errors, want 0 and 16", len(got), fs.ReadErrors())
	}
}

// A Manager must not take "the store could not read this session" for "no
// such session": a delete would be answered 404 with the collector's data
// still there.
func TestManagerTellsUnreadableFromUnknown(t *testing.T) {
	var logs logBuffer
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path, WithStoreLogger(logs.logger()))
	closeLater(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	uncached(fs)
	fin := archiveSession(0, "finished", StateAborted)
	mustPut(t, fs, fin)
	fs.Evict(fin.ID)
	c := newFlaky()
	c.failing.Store(false)
	m := NewManager(Config{Store: fs, Collector: c, Logger: logs.logger()})
	t.Cleanup(m.Close)
	status := func(method, target, body string) int {
		t.Helper()
		return do(t, m.Handler(), method, target, "127.0.0.1:7070", body, map[string]string{"Content-Type": "application/json"}).Code
	}

	damageArchive(t, fs, 10)
	if _, err := m.Rename(fin.ID, "new name"); !errors.Is(err, ErrStore) {
		t.Fatalf("rename of an unreadable session: %v, want ErrStore", err)
	}
	if _, err := m.Stop(fin.ID); !errors.Is(err, ErrStore) {
		t.Fatalf("stop of an unreadable session: %v, want ErrStore", err)
	}
	if _, err := m.Abort(fin.ID); !errors.Is(err, ErrStore) {
		t.Fatalf("abort of an unreadable session: %v, want ErrStore", err)
	}
	for _, r := range [][3]string{
		{"GET", "/v1/sessions/" + fin.ID, ""},
		{"PATCH", "/v1/sessions/" + fin.ID, `{"name":"x"}`},
	} {
		if code := status(r[0], r[1], r[2]); code != http.StatusInternalServerError {
			t.Fatalf("%s of an unreadable session: %d, want 500", r[0], code)
		}
	}
	if code := status("DELETE", "/v1/sessions/ts_nope", ""); code != http.StatusNotFound {
		t.Fatalf("delete of an unknown session: %d, want 404", code)
	}
	if code := status("GET", "/v1/sessions/ts_nope", ""); code != http.StatusNotFound {
		t.Fatalf("get of an unknown session: %d, want 404", code)
	}
	want := fmt.Sprintf("tsession_controlplane_store_read_errors_total %d\n", fs.ReadErrors())
	if fs.ReadErrors() == 0 || !strings.Contains(scrapeMetrics(t, m), want) {
		t.Fatalf("metrics do not have %q:\n%s", want, scrapeMetrics(t, m))
	}

	// It can still be deleted, here and in the collector: an archived session
	// is finished, and refusing would leave it undeletable.
	if code := status("DELETE", "/v1/sessions/"+fin.ID, ""); code != http.StatusOK {
		t.Fatalf("delete of an unreadable session: %d, want 200", code)
	}
	if calls := c.callLog(); len(calls) != 1 || calls[0] != "delete" {
		t.Fatalf("collector calls: %v", calls)
	}
	if _, ok, err := fs.GetErr(fin.ID); ok || err != nil {
		t.Fatalf("deleted session: %v %v", ok, err)
	}
}

// A Manager that starts on a store with archived sessions it cannot read
// says so: a pending seal among them is not resumed.
func TestManagerLogsUnreadableSessionsAtStart(t *testing.T) {
	var storeLogs, logs logBuffer
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path, WithStoreLogger(storeLogs.logger()))
	closeLater(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	pend := archiveSession(1, "seal pending", StateSealed)
	pend.SealPending, pend.EndedAt = true, time.Now().UTC()
	mustPut(t, fs, pend)
	fs.Evict(pend.ID)
	uncached(fs)

	healthy := NewManager(Config{Store: fs, Collector: newFlaky(), Logger: logs.logger()})
	healthy.Close()
	if pendingCount(healthy) != 1 || strings.Contains(logs.String(), "could not be read") {
		t.Fatalf("readable store: %d pending seals, log:\n%s", pendingCount(healthy), logs.String())
	}

	damageArchive(t, fs, 10)
	m := NewManager(Config{Store: fs, Collector: newFlaky(), Logger: logs.logger()})
	m.Close()
	if l := logs.String(); !strings.Contains(l, "level=ERROR") || !strings.Contains(l, "could not be read at startup") || !strings.Contains(l, "sessions=1") {
		t.Fatalf("manager started on an unreadable archive without an error in its log:\n%s", l)
	}
	if !strings.Contains(storeLogs.String(), pend.ID) {
		t.Fatalf("the store's log does not name the session:\n%s", storeLogs.String())
	}
}

// A pending seal whose session cannot be read when an attempt returns is
// kept and tried again, not dropped until the next restart.
func TestSealRetrySurvivesUnreadableSession(t *testing.T) {
	var logs logBuffer
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path, WithStoreLogger(logs.logger()))
	closeLater(t, fs)
	if err != nil {
		t.Fatal(err)
	}
	pend := archiveSession(1, "seal pending", StateSealed)
	pend.SealPending, pend.Incomplete, pend.SealError, pend.BundleURI = true, true, "seal failed", ""
	pend.EndedAt = time.Now().UTC()
	mustPut(t, fs, pend)
	fs.Evict(pend.ID)
	uncached(fs)

	c := newFlaky() // failing
	m := newRetryMgr(t, c, func(cfg *Config) {
		cfg.Store, cfg.Logger = fs, logs.logger()
		cfg.SealRetryMaxInterval, cfg.SealPendingBudget, cfg.Retention = 8*time.Millisecond, time.Hour, time.Hour
	})
	waitSeals(t, c, 0)
	if pendingCount(m) != 1 {
		t.Fatalf("%d pending seals after the start, want 1", pendingCount(m))
	}

	repair := damageArchive(t, fs, 10)
	c.failing.Store(false)
	// Seals succeed now and their result cannot be stored.
	waitSeals(t, c, c.seals.Load()+2)
	if pendingCount(m) != 1 {
		t.Fatalf("the pending seal was dropped while its session could not be read (%d pending)", pendingCount(m))
	}
	if !strings.Contains(logs.String(), "seal retry: session could not be read") {
		t.Fatalf("not logged:\n%s", logs.String())
	}

	repair()
	got := waitSession(t, m, pend.ID, "sealed on retry", func(s Session) bool { return !s.SealPending })
	if got.BundleURI != c.res.BundleURI || got.SealError != "" || got.State != StateSealed {
		t.Fatalf("after the repair: %+v", got)
	}
	if pendingCount(m) != 0 {
		t.Fatalf("%d pending seals left", pendingCount(m))
	}
}
