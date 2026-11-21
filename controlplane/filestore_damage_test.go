package controlplane

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A failed append is blanked because the truncate failed, and the fsync of
// the blanking fails as well. The blanks may then never reach the disk, so
// the next append must start on a line of its own: otherwise the torn bytes
// and the next session's line would load as one unparseable line and that
// session, already out of the snapshot, would be lost.
func TestBlankingThatCouldNotBeSyncedStartsANewLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	faults := injectFaults(fs)
	a, b, c := archiveSession(1, "a", StateSealed), archiveSession(2, "b", StateSealed), archiveSession(3, "c", StateSealed)
	mustPut(t, fs, a, b, c)
	fs.Evict(a.ID)
	before := fs.archSize

	faults.clear()
	faults.failTruncate, faults.failSync = true, true
	faults.failWrite = func(call int, _ []byte) (int, bool) { return 40, call == 1 }
	fs.Evict(b.ID) // 40 bytes of the line reach the file, then the write fails
	faults.clear()
	if !fs.archTorn {
		panic(fmt.Sprintf("after a blanking whose fsync failed the next append must start with a newline (archSize %d, was %d)", fs.archSize, before))
	}

	fs.Evict(c.ID)
	raw := archiveBytes(t, fs)
	at := strings.Index(string(raw), `"id":"`+c.ID+`"`)
	if at < 0 {
		panic(fmt.Sprintf("c is not in the archive:\n%s", raw))
	}
	lineStart := strings.LastIndexByte(string(raw[:at]), '\n') + 1
	if int64(lineStart) <= before+40 {
		panic(fmt.Sprintf("c's line starts at %d, inside or directly behind the failed append (%d..%d)", lineStart, before, before+40))
	}
	// What the disk holds if the blanking was never written: the torn start of b's line, a
	// newline, then c. Opening that must still find a (archived), b (hot) and c (archived).
	if err := fs.Compact(); err != nil {
		panic(err)
	}
	dir := t.TempDir()
	for _, suffix := range []string{"", ".journal", ".archive"} {
		data, err := os.ReadFile(path + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			panic(err)
		}
		if suffix == ".archive" {
			copy(data[before:before+40], strings.Repeat("{", 40)) // 40 bytes that are not blanks and not a line
		}
		if err := os.WriteFile(filepath.Join(dir, "s.json"+suffix), data, 0o600); err != nil {
			panic(err)
		}
	}
	re := openStore(t, filepath.Join(dir, "s.json"))
	for _, s := range []Session{a, b, c} {
		if got, ok := re.Get(s.ID); !ok || got.Name != s.Name {
			t.Errorf("%s is lost after reopening the disk state without the blanking", s.ID)
		}
	}
}

// A damaged line in the archive is a session the store can no longer serve.
// While the store runs it can still be deleted, here and in the collector.
// At the next start the line is not erased silently: it is saved aside, the
// start is logged, and the read-error counter shows it.
func TestDamagedArchiveLine(t *testing.T) {
	var logs logBuffer
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path, WithStoreLogger(logs.logger()))
	closeLater(t, fs)
	if err != nil {
		panic(err)
	}
	a, b, keep := archiveSession(1, "a", StateSealed), archiveSession(2, "b", StateAborted), archiveSession(4, "keep", StateSealed)
	mustPut(t, fs, a, b, keep)
	fs.Evict(a.ID, b.ID, keep.ID)
	if err := fs.Compact(); err != nil {
		panic(err)
	}
	uncached(fs)
	damage := func(id string) {
		t.Helper()
		fh, err := os.OpenFile(fs.archivePath(), os.O_WRONLY, 0)
		if err != nil {
			panic(err)
		}
		defer fh.Close()
		if _, err := fh.WriteAt([]byte("#####"), fs.arch[id].off+20); err != nil { // a few bad bytes inside the line
			panic(err)
		}
	}
	damage(a.ID)
	damage(b.ID)

	c := newFlaky()
	c.failing.Store(false)
	m := NewManager(Config{Store: fs, Collector: c, Logger: logs.logger()})
	code := func(m *Manager, method, id string) int {
		t.Helper()
		return do(t, m.Handler(), method, "/v1/sessions/"+id, "127.0.0.1:7070", "", nil).Code
	}
	if got := code(m, "GET", a.ID); got != http.StatusInternalServerError {
		panic(fmt.Sprintf("GET of a session whose line is damaged: %d, want 500", got))
	}
	if got := code(m, "DELETE", a.ID); got != http.StatusOK {
		panic(fmt.Sprintf("DELETE of a session whose line is damaged: %d, want 200", got))
	}
	if calls := c.callLog(); len(calls) != 1 || calls[0] != "delete" {
		panic(fmt.Sprintf("the collector must be told to delete it too: %v", calls))
	}
	if got := code(m, "GET", a.ID); got != http.StatusNotFound {
		panic(fmt.Sprintf("GET after the delete: %d, want 404", got))
	}
	m.Close()
	if err := fs.Close(); err != nil {
		panic(err)
	}

	// b's line is still damaged at the next start.
	var logs2 logBuffer
	re, err := NewFileStore(path, WithStoreLogger(logs2.logger()))
	closeLater(t, re)
	if err != nil {
		panic(err)
	}
	if got, ok := re.Get(keep.ID); !ok || got.Name != keep.Name {
		panic("the undamaged session is gone")
	}
	if _, ok := re.Get(b.ID); ok {
		panic("a session whose line is damaged cannot be served")
	}
	side, err := os.ReadFile(re.archivePath() + ".corrupt")
	if err != nil || !strings.Contains(string(side), "#####") || strings.Count(string(side), "\n") != 1 {
		panic(fmt.Sprintf("the damaged line must be saved aside, exactly once: %q %v", side, err))
	}
	if !strings.Contains(logs2.String(), "cannot be read") || !strings.Contains(logs2.String(), "lines=1") {
		panic(fmt.Sprintf("the start must say that a line could not be read:\n%s", logs2.String()))
	}
	if re.ReadErrors() != 1 {
		panic(fmt.Sprintf("read errors after open: %d, want 1", re.ReadErrors()))
	}
}

// A List in flight while the store is closed must not put a session back into
// the cache that Close has just emptied: nothing archived is served after Close.
func TestListAfterCloseServesNoArchivedSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	hot, cold := archiveSession(1, "hot", StateActive), archiveSession(2, "cold", StateSealed)
	mustPut(t, fs, hot, cold)
	fs.Evict(cold.ID)
	for i := 0; i < 200; i++ {
		done := make(chan struct{})
		go func() { defer close(done); _ = fs.List() }()
		_ = fs.Close()
		<-done
		for _, s := range fs.List() {
			if s.ID == cold.ID {
				panic(fmt.Sprintf("round %d: an archived session was listed after Close", i))
			}
		}
		re, err := NewFileStore(path)
		if err != nil {
			panic(err)
		}
		fs = re
	}
	closeLater(t, fs)
}
