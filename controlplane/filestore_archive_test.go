package controlplane

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// uncached turns off the store's cache of decoded archived sessions, so that
// every read of one goes to the archive file.
func uncached(fs *FileStore) *FileStore {
	fs.cache.mu.Lock()
	fs.cache.max, fs.cache.m = 0, nil
	fs.cache.mu.Unlock()
	return fs
}

// archiveSession is storeSession with the fields that make a line long and
// awkward: a binding, a name the JSON encoder escapes, an error string.
func archiveSession(i int, name string, st State) Session {
	s := storeSession(i, name+` <é&"> `+strings.Repeat("x", i%7), st)
	s.Bindings = []Binding{{Name: "x-user-id", Value: fmt.Sprintf("user-%d", i)}}
	s.BundleURI = fmt.Sprintf("file:///bundles/%d.tar.zst", i)
	if i%3 == 0 {
		s.SealError = "seal failed after retries: boom\n\"quoted\""
	}
	return s
}

func jsonLine(t *testing.T, s Session) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// wantStore checks that the store serves exactly the sessions in want through
// Get, List, ListState and ListPending, and none of the ids in gone.
func wantStore(t *testing.T, what string, fs *FileStore, want []Session, gone ...string) {
	t.Helper()
	want = slices.Clone(want)
	sortSessions(want)
	wantSameSessions(t, what+": list", fs.List(), want)
	for _, s := range want {
		if got, ok := fs.Get(s.ID); !ok || !reflect.DeepEqual(got, s) {
			t.Fatalf("%s: get %s:\n got %+v %v\nwant %+v", what, s.ID, got, ok, s)
		}
	}
	for _, id := range gone {
		if got, ok := fs.Get(id); ok {
			t.Fatalf("%s: get %s: %+v, want none", what, id, got)
		}
	}
	for _, st := range append(archStates[1:], "PAUSED", "NOPE") {
		var in []Session
		for _, s := range want {
			if s.State == st {
				in = append(in, s)
			}
		}
		wantSameSessions(t, fmt.Sprintf("%s: list state %s", what, st), fs.ListState(st), in)
		wantSameSessions(t, fmt.Sprintf("%s: manager list state %s", what, st), (&Manager{cfg: Config{Store: fs}}).List(st), in)
	}
	var pending []Session
	for _, s := range want {
		if _, hot := fs.MemStore.Get(s.ID); hot || s.SealPending || !s.State.Terminal() {
			pending = append(pending, s)
		}
	}
	wantSameSessions(t, what+": list pending", fs.ListPending(), pending)
}

// The archived sessions are read from the file, by offset: every way of
// asking for them returns what was stored, with and without the cache, before
// and after a restart.
func TestFileStoreArchiveReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	var all []Session
	for i, st := range []State{StateSealed, StateAborted, StateExpired, StateSealed, StateAborted} {
		all = append(all, archiveSession(i, "finished", st))
	}
	sealPending := archiveSession(5, "seal pending", StateSealed)
	sealPending.SealPending, sealPending.Incomplete = true, true
	draining := archiveSession(6, "archived while draining", StateDraining) // a Manager never does this; the store allows it
	future := archiveSession(7, "state of a later version", "PAUSED")
	rearchived := archiveSession(8, "archived twice", StateSealed)
	changed := archiveSession(9, "archived, then changed", StateSealed)
	hotActive, hotSealed := archiveSession(10, "hot", StateActive), archiveSession(11, "hot and finished", StateSealed)
	all = append(all, sealPending, draining, future, rearchived, changed, hotActive, hotSealed)
	mustPut(t, fs, all...)
	for _, s := range all[:10] {
		fs.Evict(s.ID) // one line each
	}
	rearchived.Name = "archived twice: second record"
	mustPut(t, fs, rearchived)
	fs.Evict(rearchived.ID)
	changed.Name, changed.SealPending = "hot again", true
	mustPut(t, fs, changed)
	all[8], all[9] = rearchived, changed

	wantStore(t, "live", fs, all)
	wantStore(t, "live, uncached", uncached(fs), all)
	re := openStore(t, path)
	wantStore(t, "reopened", re, all)
	wantStore(t, "reopened, uncached", uncached(openStore(t, path)), all)
	if _, hot := re.MemStore.Get(changed.ID); !hot {
		t.Fatal("session changed after its eviction is not hot")
	}
	for id, r := range re.arch {
		s, _ := re.getArchived(id)
		if archStates[r.state] != s.State && !(r.state == 0 && s.State == "PAUSED") || r.sealPending != s.SealPending {
			t.Fatalf("index entry of %s: %+v, session %+v", id, r, s)
		}
	}
}

// The index holds offsets into the archive. Whatever a crash or an earlier
// version left in the file (a torn tail, lines that do not parse, blank
// lines, other line endings, several lines of one id), every indexed session
// must read back whole, and the file must be tidy afterwards.
func TestFileStoreArchiveOffsets(t *testing.T) {
	var ss []Session
	var lines []string
	for i := range 4 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
		lines = append(lines, jsonLine(t, ss[i]))
	}
	older := ss[1]
	older.Name = "older record of s1"
	type tc struct {
		name    string
		content string
		want    []Session
	}
	cases := []tc{
		{"clean", lines[0] + "\n" + lines[1] + "\n" + lines[2] + "\n" + lines[3] + "\n", ss},
		{"no final newline", lines[0] + "\n" + lines[1], ss[:2]},
		{"garbage line", lines[0] + "\n\x00\x00{{{\n" + lines[1] + "\n", ss[:2]},
		{"blank lines", strings.Repeat(" ", 300) + "\n" + lines[0] + "\n\n   \n" + lines[1] + "\n" + strings.Repeat(" ", 40) + "\n", ss[:2]},
		{"white space around lines", "  " + lines[0] + " \r\n\t" + lines[1] + "\r\n", ss[:2]},
		{"older line of an id", jsonLine(t, older) + "\n" + lines[0] + "\n" + lines[1] + "\n", ss[:2]},
		{"line glued to a torn one", lines[0] + "\n" + lines[1][:50] + lines[2] + "\n" + lines[3] + "\n", []Session{ss[0], ss[3]}},
		{"only a torn line", lines[0][:30], nil},
		{"empty file", "", nil},
	}
	for cut := 0; cut < len(lines[2]); cut += 5 { // the last append torn at any point
		cases = append(cases, tc{fmt.Sprintf("torn after %d bytes", cut), lines[0] + "\n" + lines[1] + "\n" + lines[2][:cut], ss[:2]})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.json")
			if err := os.WriteFile(path+".archive", []byte(c.content), 0o600); err != nil {
				t.Fatal(err)
			}
			var gone []string
			for _, s := range ss {
				if !slices.ContainsFunc(c.want, func(w Session) bool { return w.ID == s.ID }) {
					gone = append(gone, s.ID)
				}
			}
			fs := uncached(openStore(t, path))
			wantStore(t, "opened", fs, c.want, gone...)
			// One line per session is left, and nothing else.
			if c.content != "" {
				var tidy string
				for _, s := range c.want {
					tidy += jsonLine(t, s) + "\n"
				}
				if got, _ := os.ReadFile(fs.archivePath()); string(got) != tidy {
					t.Fatalf("archive after open:\n%q\nwant\n%q", got, tidy)
				}
			}
			// New lines land behind the old ones, at the offsets the index says.
			added := archiveSession(9, "added", StateSealed)
			mustPut(t, fs, added)
			fs.Evict(added.ID)
			want := append(slices.Clone(c.want), added)
			wantStore(t, "after an eviction", fs, want, gone...)
			wantStore(t, "reopened", uncached(openStore(t, path)), want, gone...)
		})
	}
}

// If the archive file is cut short or changed behind an open store, the
// sessions whose lines are gone are not found; nothing else is returned in
// their place.
func TestFileStoreArchiveDamagedUnderStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	var ss []Session
	for i := range 6 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
	}
	mustPut(t, fs, ss...)
	for _, s := range ss {
		fs.Evict(s.ID)
	}
	uncached(fs)
	wantStore(t, "before", fs, ss)

	// The line of s1 now holds another session, of the same length.
	other := ss[1]
	other.ID = "ts_other0001"
	r := fs.arch[ss[1].ID]
	if line := jsonLine(t, other); len(line) != int(r.n) {
		t.Fatalf("test setup: line of %d bytes, want %d", len(line), r.n)
	} else if _, err := fs.ar.WriteAt([]byte(line), r.off); err != nil {
		t.Fatal(err)
	}
	// The file ends in the middle of the line of s4.
	if err := os.Truncate(fs.archivePath(), fs.arch[ss[4].ID].off+10); err != nil {
		t.Fatal(err)
	}
	wantSameSessions(t, "list", fs.List(), []Session{ss[0], ss[2], ss[3]})
	for i, s := range ss {
		got, ok := fs.Get(s.ID)
		if readable := i == 0 || i == 2 || i == 3; ok != readable || ok && !reflect.DeepEqual(got, s) {
			t.Fatalf("get %s: %+v %v", s.ID, got, ok)
		}
	}
	if l := fs.ListState(StateAborted); len(l) != 3 {
		t.Fatalf("list state: %+v", l)
	}
}

// Another store that opens the same files writes the archive anew (the tests
// in this package do that to model a restart). The first store keeps its
// offsets and must keep reading the file they belong to.
func TestFileStoreArchiveReplacedUnderStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := uncached(openStore(t, path))
	var ss []Session
	for i := range 6 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
	}
	mustPut(t, fs, ss...)
	for _, s := range ss {
		fs.Evict(s.ID)
	}
	if err := fs.Delete(ss[0].ID); err != nil { // leaves a blank line at the start
		t.Fatal(err)
	}
	before, err := os.Stat(fs.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	re := uncached(openStore(t, path))
	after, err := os.Stat(fs.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) || after.Size() >= before.Size() {
		t.Fatalf("archive not written anew on open: %d bytes before, %d after", before.Size(), after.Size())
	}
	if re.arch[ss[1].ID].off == fs.arch[ss[1].ID].off {
		t.Fatal("test setup: the offsets did not change")
	}
	wantStore(t, "store with the old offsets", fs, ss[1:], ss[0].ID)
	wantStore(t, "store with the new offsets", re, ss[1:], ss[0].ID)
}

// Deleting an archived session overwrites its line in place: no trace of the
// session is left in the file, the other lines stay where they are, and the
// file is not written anew. The same goes for the old line of a session that
// is archived again.
func TestFileStoreDeleteArchivedInPlace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := uncached(openStore(t, path))
	var ss []Session
	for i := range 5 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("name-%d-secret", i), StateAborted))
	}
	mustPut(t, fs, ss...)
	for _, s := range ss {
		fs.Evict(s.ID)
	}
	if err := fs.Compact(); err != nil { // the journal no longer names them
		t.Fatal(err)
	}
	before, err := os.Stat(fs.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	archive := func() string {
		t.Helper()
		b, err := os.ReadFile(fs.archivePath())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	if err := fs.Delete(ss[1].ID); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(fs.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() {
		t.Fatalf("delete replaced or resized the archive: %d bytes before, %d after", before.Size(), after.Size())
	}
	if a := archive(); strings.Contains(a, ss[1].ID) || strings.Contains(a, "name-1-secret") || strings.Contains(a, "user-1") {
		t.Fatalf("deleted session left in the archive:\n%s", a)
	}
	left := []Session{ss[0], ss[2], ss[3], ss[4]}
	wantStore(t, "after delete", fs, left, ss[1].ID)

	// Archived again under a new name: the old line goes.
	ss[2].Name = "renamed"
	mustPut(t, fs, ss[2])
	fs.Evict(ss[2].ID)
	if a := archive(); strings.Contains(a, "name-2-secret") || strings.Count(a, ss[2].ID) != 1 {
		t.Fatalf("old line of a session archived again left in the archive:\n%s", a)
	}
	if len(fs.stale) != 0 {
		t.Fatalf("stale lines: %+v", fs.stale)
	}
	left[1] = ss[2]
	wantStore(t, "after archiving again", fs, left, ss[1].ID)

	// A restart drops the blank lines and finds the same sessions.
	re := uncached(openStore(t, path))
	wantStore(t, "reopened", re, left, ss[1].ID)
	if n := len(fileLines(t, re.archivePath())); n != len(left) {
		t.Fatalf("archive has %d lines after a restart, want %d", n, len(left))
	}

	// Deleting them all removes the file. A new archive starts at offset 0
	// again; what the old one had at an offset must not be served for it.
	re.cache.max = archCacheMax
	wantStore(t, "cached", re, left)
	for _, s := range left {
		if err := re.Delete(s.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(re.archivePath()); !os.IsNotExist(err) {
		t.Fatalf("empty archive left behind: %v", err)
	}
	again := left[0]
	again.Name = "same id, stored again"
	mustPut(t, re, again)
	re.Evict(again.ID)
	wantStore(t, "new archive", re, []Session{again}, left[1].ID, left[2].ID, left[3].ID)
	wantStore(t, "new archive, reopened", openStore(t, path), []Session{again}, left[1].ID)
}

// The crash windows of the in-place changes to the archive.
func TestFileStoreArchiveCrashes(t *testing.T) {
	setup := func(t *testing.T) (fs *FileStore, a, b Session) {
		fs = openStore(t, filepath.Join(t.TempDir(), "s.json"))
		a, b = archiveSession(0, "a", StateAborted), archiveSession(1, "b", StateAborted)
		mustPut(t, fs, a, b)
		fs.Evict(a.ID, b.ID)
		return fs, a, b
	}
	archiveHas := func(t *testing.T, fs *FileStore, id string) bool {
		t.Helper()
		raw, err := os.ReadFile(fs.archivePath())
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return bytes.Contains(raw, []byte(id))
	}

	// A session is archived again; the crash comes after its new line is
	// written and before the old one is overwritten. The new line wins, and a
	// later delete must take both.
	t.Run("old line not overwritten", func(t *testing.T) {
		fs, a, b := setup(t)
		old, err := os.ReadFile(fs.archivePath())
		if err != nil {
			t.Fatal(err)
		}
		a.Name = "a, second record"
		mustPut(t, fs, a)
		fs.Evict(a.ID)
		newLine := jsonLine(t, a) + "\n"
		if err := os.WriteFile(fs.archivePath(), append(old, newLine...), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, compactFirst := range []bool{false, true} {
			re := uncached(openStore(t, fs.path))
			wantStore(t, "reopened", re, []Session{a, b})
			if compactFirst { // the delete is then in no journal when the store is opened next
				if err := re.Compact(); err != nil {
					t.Fatal(err)
				}
			}
		}
		re := openStore(t, fs.path)
		if err := re.Delete(a.ID); err != nil {
			t.Fatal(err)
		}
		if err := re.Compact(); err != nil {
			t.Fatal(err)
		}
		re2 := openStore(t, fs.path)
		wantStore(t, "after delete and restart", re2, []Session{b}, a.ID)
		if archiveHas(t, re2, a.ID) {
			t.Fatal("a line of the deleted session is left")
		}
	})

	// The line of a deleted session is overwritten and the journal record of
	// the delete is lost (a power failure): the session stays deleted.
	t.Run("delete record lost", func(t *testing.T) {
		fs, a, b := setup(t)
		if err := fs.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := fs.Delete(a.ID); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(fs.journalPath()); err != nil {
			t.Fatal(err)
		}
		wantStore(t, "reopened", uncached(openStore(t, fs.path)), []Session{b}, a.ID)
	})

	// A delete is journalled, its line still in the archive, and the archive
	// cannot be written anew when the store is opened (no temp file can be
	// created). The delete is finished in place.
	t.Run("delete finished in place", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can write to a read-only directory")
		}
		fs, a, b := setup(t)
		archive, err := os.ReadFile(fs.archivePath())
		if err != nil {
			t.Fatal(err)
		}
		if err := fs.Delete(a.ID); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fs.archivePath(), archive, 0o600); err != nil { // the line was never overwritten
			t.Fatal(err)
		}
		dir := filepath.Dir(fs.path)
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		re, err := NewFileStore(fs.path)
		if cerr := os.Chmod(dir, 0o700); cerr != nil {
			t.Fatal(cerr)
		}
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		wantStore(t, "opened in a read-only directory", uncached(re), []Session{b}, a.ID)
		if archiveHas(t, re, a.ID) {
			t.Fatal("the line of the deleted session is left")
		}
		re2 := uncached(openStore(t, fs.path))
		wantStore(t, "reopened", re2, []Session{b}, a.ID)
		if n := len(fileLines(t, re2.archivePath())); n != 1 {
			t.Fatalf("archive has %d lines, want 1", n)
		}
	})

	// The same, and the archive file cannot be written either: opening the
	// store must fail rather than bring the session back.
	t.Run("delete cannot be finished", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can write to read-only files")
		}
		fs, a, b := setup(t)
		archive, err := os.ReadFile(fs.archivePath())
		if err != nil {
			t.Fatal(err)
		}
		if err := fs.Delete(a.ID); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fs.archivePath(), archive, 0o600); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Dir(fs.path)
		if err := os.Chmod(fs.archivePath(), 0o400); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		_, err = NewFileStore(fs.path)
		if cerr := os.Chmod(dir, 0o700); cerr != nil {
			t.Fatal(cerr)
		}
		if err == nil {
			t.Fatal("store opened although a journalled delete could not be finished")
		}
		// A read-only archive without such a delete is served, not changed.
		if err := os.Remove(fs.journalPath()); err != nil {
			t.Fatal(err)
		}
		ro := uncached(openStore(t, fs.path))
		wantStore(t, "read-only archive", ro, []Session{a, b})
		if err := ro.Delete(a.ID); err == nil {
			t.Fatal("delete from a read-only archive reported success")
		}
		if got, ok := ro.Get(a.ID); !ok || got.Name != a.Name {
			t.Fatalf("session lost by a failed delete: %+v %v", got, ok)
		}
		// Once the file is writable the delete, journalled above, is finished.
		if err := os.Chmod(fs.archivePath(), 0o600); err != nil {
			t.Fatal(err)
		}
		wantStore(t, "writable again", uncached(openStore(t, fs.path)), []Session{b}, a.ID)
	})
}

// List reads the archive lines after it has released the locks. A session
// that is deleted in between is not listed; one that is archived again in
// between (its old line is overwritten) is listed with its new record.
func TestFileStoreListWhileLinesChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := uncached(openStore(t, path))
	var ss []Session
	for i := range 5 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
	}
	mustPut(t, fs, ss...)
	for _, s := range ss {
		fs.Evict(s.ID)
	}
	rearchived, hotAgain := ss[1], ss[3]
	rearchived.Name, hotAgain.Name = "archived again", "hot again"
	fs.testHookListed = func() {
		fs.testHookListed = nil
		mustPut(t, fs, rearchived)
		fs.Evict(rearchived.ID)
		if err := fs.Delete(ss[2].ID); err != nil {
			t.Error(err)
		}
		mustPut(t, fs, hotAgain)
		fs.Evict(hotAgain.ID)
		mustPut(t, fs, hotAgain)
	}
	wantSameSessions(t, "list", fs.List(), []Session{ss[0], rearchived, hotAgain, ss[4]})
	wantStore(t, "afterwards", fs, []Session{ss[0], rearchived, hotAgain, ss[4]}, ss[2].ID)

	// The same when every archived session goes and the file with it.
	fs.Evict(hotAgain.ID)
	fs.testHookListed = func() {
		fs.testHookListed = nil
		for _, id := range []string{ss[0].ID, rearchived.ID, hotAgain.ID, ss[4].ID} {
			if err := fs.Delete(id); err != nil {
				t.Error(err)
			}
		}
		mustPut(t, fs, ss[4])
		fs.Evict(ss[4].ID)
	}
	wantSameSessions(t, "list", fs.ListState(StateAborted), []Session{ss[4]})
	wantStore(t, "afterwards", fs, []Session{ss[4]}, ss[0].ID)
}

// The cache of decoded archived sessions never grows past its bound.
func TestFileStoreArchiveCacheBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	fs.cache.max = 8
	var ss []Session
	for i := range 50 {
		ss = append(ss, archiveSession(i, fmt.Sprintf("s%d", i), StateAborted))
	}
	mustPut(t, fs, ss...)
	for i := 0; i < len(ss); i += 10 {
		fs.Evict(ss[i].ID, ss[i+1].ID, ss[i+2].ID, ss[i+3].ID, ss[i+4].ID, ss[i+5].ID, ss[i+6].ID, ss[i+7].ID, ss[i+8].ID, ss[i+9].ID)
	}
	for range 3 {
		wantStore(t, "small cache", fs, ss)
		fs.cache.mu.Lock()
		n := len(fs.cache.m)
		fs.cache.mu.Unlock()
		if n == 0 || n > 8 {
			t.Fatalf("%d sessions cached, want 1..8", n)
		}
	}
}

// Writers, each with a model of its own sessions, and readers, with
// background compactions of the journal in between and nearly every read of
// an archived session going to the file (run with -race). A writer must
// always read back what its model says. A reader must never see a session
// that is not one a writer stored, whole, and must find in every list the
// sessions that are never deleted, although their lines keep moving. In the
// end the store, and a restart of it, serve exactly the models.
func TestFileStoreConcurrentModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	lowerCompactThreshold(fs, 4<<10)
	fs.cache.max = 4
	const workers, perWorker, steps, readers = 6, 10, 160, 3
	if testing.Short() {
		t.Skip("writes a few thousand lines with fsyncs")
	}

	idOf := func(w, i int) string { return fmt.Sprintf("ts_w%d_%03d", w, i) }
	// version builds the v-th record of a session. Everything in it follows
	// from the id and the name, which is what whole checks.
	version := func(w, i, v int, rng *rand.Rand) Session {
		s := archiveSession(w*perWorker+i, fmt.Sprintf("w%d v%d", w, v), []State{StateSealed, StateAborted, StateExpired, StateActive}[rng.IntN(4)])
		s.ID = idOf(w, i)
		s.SealPending = s.State == StateSealed && rng.IntN(4) == 0
		s.BundleURI = "bundle://" + s.ID + "/" + s.Name
		return s
	}
	whole := func(s Session) bool {
		return s.BundleURI == "bundle://"+s.ID+"/"+s.Name && len(s.Bindings) == 1 && !s.CreatedAt.IsZero()
	}

	// The first session of every worker is never deleted, only changed and
	// archived over and over.
	models := make([]map[string]Session, workers)
	for w := range workers {
		s := version(w, 0, -1, rand.New(rand.NewPCG(uint64(w), 1)))
		mustPut(t, fs, s)
		models[w] = map[string]Session{s.ID: s}
	}
	var stop atomic.Bool
	var writersWG, readersWG sync.WaitGroup
	for w := range workers {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 42))
			model := models[w]
			mine := func(l []Session) []Session {
				return slices.DeleteFunc(l, func(s Session) bool { return !strings.HasPrefix(s.ID, fmt.Sprintf("ts_w%d_", w)) })
			}
			check := func(what string, ids ...string) bool {
				for _, id := range ids {
					want, stored := model[id]
					if got, ok := fs.Get(id); ok != stored || !reflect.DeepEqual(got, want) {
						t.Errorf("%s: get %s:\n got %+v %v\nwant %+v %v", what, id, got, ok, want, stored)
						return false
					}
				}
				return true
			}
			for step := range steps {
				i := rng.IntN(perWorker)
				if step%2 == 0 {
					i = 0
				}
				id := idOf(w, i)
				op := rng.IntN(10)
				if i == 0 {
					op = step / 2 % 2 * 4 // store a new record and archive it, in turns
				}
				switch {
				case op < 4: // store a new record; the session is hot
					s := version(w, i, step, rng)
					if err := fs.Put(s); err != nil {
						t.Errorf("put: %v", err)
						return
					}
					model[id] = s
				case op < 7: // archive some, among them unknown and archived ones
					ids := []string{id, idOf(w, rng.IntN(perWorker)), idOf(w, rng.IntN(perWorker)), "ts_unknown"}
					fs.Evict(ids...)
					if !check("after evict", ids[:3]...) {
						return
					}
				case op < 8:
					if err := fs.Delete(id); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
					delete(model, id)
				case op < 9:
					var want []Session
					for _, s := range model {
						want = append(want, s)
					}
					sortSessions(want)
					if got := mine(fs.List()); !reflect.DeepEqual(got, want) && (len(got) != 0 || len(want) != 0) {
						t.Errorf("list of worker %d:\n got %+v\nwant %+v", w, got, want)
						return
					}
				default:
					var want []Session
					for _, s := range model {
						if s.State == StateSealed {
							want = append(want, s)
						}
					}
					sortSessions(want)
					if got := mine(fs.ListState(StateSealed)); !reflect.DeepEqual(got, want) && (len(got) != 0 || len(want) != 0) {
						t.Errorf("sealed sessions of worker %d:\n got %+v\nwant %+v", w, got, want)
						return
					}
				}
				if !check("after step", id) {
					return
				}
			}
		}()
	}
	for r := range readers {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 7))
			for !stop.Load() {
				var l []Session
				switch rng.IntN(3) {
				case 0:
					l = fs.List()
					for w := range workers {
						if !slices.ContainsFunc(l, func(s Session) bool { return s.ID == idOf(w, 0) }) {
							t.Errorf("%s, which is never deleted, is missing from a list", idOf(w, 0))
							return
						}
					}
				case 1:
					l = fs.ListState(StateAborted)
					for _, s := range l {
						if s.State != StateAborted {
							t.Errorf("list state ABORTED returned %+v", s)
							return
						}
					}
				default:
					l = fs.ListPending()
				}
				for i, s := range l {
					if !whole(s) {
						t.Errorf("listed session is not one that was stored: %+v", s)
						return
					}
					if i > 0 && (l[i-1].ID == s.ID || l[i-1].CreatedAt.After(s.CreatedAt)) {
						t.Errorf("list out of order or with a duplicate: %s, %s", l[i-1].ID, s.ID)
						return
					}
				}
				for range 20 {
					id := idOf(rng.IntN(workers), rng.IntN(perWorker))
					if s, ok := fs.Get(id); ok && (s.ID != id || !whole(s)) {
						t.Errorf("get %s: not a session that was stored: %+v", id, s)
						return
					}
				}
			}
		}()
	}
	writersWG.Wait()
	stop.Store(true)
	readersWG.Wait()
	waitCompaction(fs)
	if t.Failed() {
		return
	}

	var want []Session
	var gone []string
	for w, model := range models {
		for i := range perWorker {
			if s, ok := model[idOf(w, i)]; ok {
				want = append(want, s)
			} else {
				gone = append(gone, idOf(w, i))
			}
		}
	}
	if len(want) == 0 || len(gone) == 0 || len(fs.arch) == 0 {
		t.Fatalf("test setup: %d sessions stored, %d deleted, %d archived", len(want), len(gone), len(fs.arch))
	}
	wantStore(t, "live", fs, want, gone...)
	re := wantSameAfterReopen(t, fs)
	wantStore(t, "reopened", uncached(re), want, gone...)
	// The restart left one line per archived session.
	if n := len(fileLines(t, re.archivePath())); n != len(re.arch) {
		t.Fatalf("archive has %d lines for %d archived sessions", n, len(re.arch))
	}
	raw, err := os.ReadFile(re.archivePath())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range gone {
		if bytes.Contains(raw, []byte(id)) {
			t.Fatalf("deleted session %s is in the archive", id)
		}
	}
}
