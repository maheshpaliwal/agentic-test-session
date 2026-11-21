package controlplane

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func openStore(t *testing.T, path string) *FileStore {
	t.Helper()
	fs, err := NewFileStore(path)
	closeLater(t, fs)
	if err != nil {
		panic(fmt.Sprintf("open store: %v", err))
	}
	return fs
}

// closeLater closes the store when the test ends, so that tests do not leave
// archive files open. fs may be nil (a store that failed to open).
func closeLater(t testing.TB, fs *FileStore) {
	if fs != nil {
		t.Cleanup(func() { _ = fs.Close() })
	}
}

// storeSession builds the i-th test session: ids and creation times ascend
// with i.
func storeSession(i int, name string, st State) Session {
	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC).Add(time.Duration(i) * time.Minute)
	s := Session{ID: fmt.Sprintf("ts_store%04d", i), Name: name, Mode: ModeHeader, State: st, InitiatedBy: "test",
		CreatedAt: at, StartedAt: at, TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60}
	if st.Terminal() {
		s.EndedAt = at.Add(time.Second)
	}
	return s
}

func mustPut(t *testing.T, fs *FileStore, ss ...Session) {
	t.Helper()
	for _, s := range ss {
		if err := fs.Put(s); err != nil {
			panic(fmt.Sprintf("put %s: %v", s.ID, err))
		}
	}
}

// lowerCompactThreshold makes the store compact in the background after n
// journal bytes.
func lowerCompactThreshold(fs *FileStore, n int64) {
	fs.wmu.Lock()
	fs.compactMin, fs.compactAt = n, fs.journalSize+n
	fs.wmu.Unlock()
}

// waitCompaction blocks until no background compaction is running.
func waitCompaction(fs *FileStore) {
	for {
		fs.wmu.Lock()
		running := fs.compacting
		fs.wmu.Unlock()
		if running == nil {
			return
		}
		<-running
	}
}

// wantSameAfterReopen opens the store directory again, as after a crash or a
// restart, and compares it with what the live store serves.
func wantSameAfterReopen(t *testing.T, fs *FileStore) *FileStore {
	t.Helper()
	re := openStore(t, fs.path)
	wantSameSessions(t, "sessions after reopen", re.List(), fs.List())
	wantSameSessions(t, "hot set after reopen", re.MemStore.List(), fs.MemStore.List())
	return re
}

func wantSameSessions(t *testing.T, what string, got, want []Session) {
	t.Helper()
	for i := 0; i < len(got) && i < len(want); i++ {
		if !reflect.DeepEqual(got[i], want[i]) {
			panic(fmt.Sprintf("%s: [%d]\n got %+v\nwant %+v", what, i, got[i], want[i]))
		}
	}
	if len(got) != len(want) {
		panic(fmt.Sprintf("%s: %d sessions, want %d", what, len(got), len(want)))
	}
}

func fileLines(t *testing.T, path string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		panic(err)
	}
	return bytes.Split(bytes.TrimSuffix(b, []byte{'\n'}), []byte{'\n'})
}

func appendRaw(t *testing.T, path, data string) {
	t.Helper()
	if err := appendFile(path, []byte(data), false); err != nil {
		panic(err)
	}
}

// A store directory written before the journal existed (testdata/filestore_v1
// was produced by that code: a snapshot and an archive, with a session
// archived twice) must load, and keep working.
func TestFileStoreLoadsOldFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	for _, suffix := range []string{"", ".archive"} {
		b, err := os.ReadFile(filepath.Join("testdata", "filestore_v1", "sessions.json"+suffix))
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(path+suffix, b, 0o600); err != nil {
			panic(err)
		}
	}
	fs := openStore(t, path)

	want := []struct {
		id, name string
		state    State
		hot      bool
	}{
		{"ts_fixturea", "active one", StateActive, true},
		{"ts_fixtureb", "draining one", StateDraining, true},
		{"ts_fixturec", "sealed hot", StateSealed, true},
		{"ts_fixtured", "archived", StateAborted, false},
		{"ts_fixturee", "after rename", StateSealed, false}, // two archive lines: the last wins
		{"ts_fixturef", "seal pending", StateSealed, false},
	}
	list := fs.List()
	if len(list) != len(want) {
		panic(fmt.Sprintf("list: %d sessions, want %d: %+v", len(list), len(want), list))
	}
	for i, w := range want {
		if s := list[i]; s.ID != w.id || s.Name != w.name || s.State != w.state {
			panic(fmt.Sprintf("list[%d] = %+v, want %+v", i, s, w))
		}
		if s, ok := fs.Get(w.id); !ok || s.Name != w.name {
			panic(fmt.Sprintf("get %s: %+v %v", w.id, s, ok))
		}
		if _, hot := fs.MemStore.Get(w.id); hot != w.hot {
			panic(fmt.Sprintf("%s: hot=%v, want %v", w.id, hot, w.hot))
		}
	}
	if s, _ := fs.Get("ts_fixturea"); len(s.Bindings) != 1 || s.Bindings[0].Value != "u1" || s.CreatedAt.IsZero() {
		panic(fmt.Sprintf("fields lost: %+v", s))
	}
	if s, _ := fs.Get("ts_fixturef"); !s.SealPending || s.SealError == "" || !s.Incomplete {
		panic(fmt.Sprintf("seal marker lost: %+v", s))
	}

	// The loaded store takes every kind of change and keeps it.
	b, _ := fs.Get("ts_fixtureb")
	b.State, b.EndedAt = StateSealed, b.CreatedAt.Add(time.Minute)
	mustPut(t, fs, b)
	fs.Evict("ts_fixturec")
	if err := fs.Delete("ts_fixtured"); err != nil {
		panic(err)
	}
	re := wantSameAfterReopen(t, fs)
	if s, ok := re.Get("ts_fixtureb"); !ok || s.State != StateSealed {
		panic(fmt.Sprintf("change lost: %+v %v", s, ok))
	}
	if _, ok := re.Get("ts_fixtured"); ok || len(re.List()) != 5 {
		panic(fmt.Sprintf("delete lost: %+v", re.List()))
	}
	if _, hot := re.MemStore.Get("ts_fixturec"); hot {
		panic("evicted session is hot again")
	}
}

// Changes are journalled, not written to the snapshot; opening the store
// replays the journal and folds it into a snapshot in the old format.
func TestFileStoreJournalReplayAndCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a, b, c, d := storeSession(0, "a", StateActive), storeSession(1, "b", StateAborted),
		storeSession(2, "c", StateSealed), storeSession(3, "d", StateAborted)
	mustPut(t, fs, a, b, c, d)
	a.State = StateDraining
	mustPut(t, fs, a)
	fs.Evict(b.ID, c.ID)
	c.Name = "c renamed"
	mustPut(t, fs, c) // archived and hot now
	if err := fs.Delete(d.ID); err != nil {
		panic(err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		panic(fmt.Sprintf("snapshot written by a change: %v", err))
	}
	if n := len(fileLines(t, fs.journalPath())); n != 8 {
		panic(fmt.Sprintf("journal has %d records, want 8 (one per change, one for the batch evict)", n))
	}

	re := wantSameAfterReopen(t, fs)
	if l := re.List(); len(l) != 3 || l[0].State != StateDraining || l[2].Name != "c renamed" {
		panic(fmt.Sprintf("replayed: %+v", l))
	}
	if _, err := os.Stat(re.journalPath()); !os.IsNotExist(err) {
		panic(fmt.Sprintf("journal not folded away on open: %v", err))
	}
	// The snapshot alone is what earlier versions wrote: an array of sessions.
	raw, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var snap []Session
	if err := json.Unmarshal(raw, &snap); err != nil || !reflect.DeepEqual(snap, re.MemStore.List()) {
		panic(fmt.Sprintf("snapshot: %v\n%s", err, raw))
	}

	// Compact does the same on demand, and the store stays usable.
	mustPut(t, re, storeSession(4, "e", StateActive))
	if err := re.Compact(); err != nil {
		panic(err)
	}
	if _, err := os.Stat(re.journalPath()); !os.IsNotExist(err) {
		panic(fmt.Sprintf("journal left by Compact: %v", err))
	}
	mustPut(t, re, storeSession(5, "f", StateActive))
	if l := wantSameAfterReopen(t, re).List(); len(l) != 5 {
		panic(fmt.Sprintf("after compact: %+v", l))
	}
}

// A crash can cut the last journal line short. The store must open with
// everything before it, and keep working.
func TestFileStoreTornJournal(t *testing.T) {
	rec := func(s Session) string {
		b, _ := json.Marshal(journalRecord{Op: "put", Session: &s})
		return string(b)
	}
	c := storeSession(2, "c", StateActive)
	for _, tc := range []struct {
		name  string
		tail  string
		wantC bool
	}{
		{"truncated record", rec(c)[:40], false},
		{"truncated to one byte", "{", false},
		{"garbage", "\x00\x00\x00\x00", false},
		{"complete record without newline", rec(c), true},
		{"torn line then a record", rec(c)[:40] + "\n" + rec(c) + "\n", true},
		{"record glued to a torn line", rec(c)[:40] + rec(c) + "\n", false},
		{"unknown op", `{"op":"future","ids":["x"]}` + "\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.json")
			fs := openStore(t, path)
			a, b := storeSession(0, "a", StateActive), storeSession(1, "b", StateSealed)
			mustPut(t, fs, a, b)
			appendRaw(t, fs.journalPath(), tc.tail)

			re := openStore(t, path)
			wantN := 2
			if tc.wantC {
				wantN = 3
			}
			if l := re.List(); len(l) != wantN || l[0].Name != "a" || l[1].Name != "b" {
				panic(fmt.Sprintf("after torn journal: %+v", l))
			}
			if _, ok := re.Get(c.ID); ok != tc.wantC {
				panic(fmt.Sprintf("session of the torn tail present=%v, want %v", ok, tc.wantC))
			}
			mustPut(t, re, storeSession(3, "d", StateActive))
			if l := wantSameAfterReopen(t, re).List(); len(l) != wantN+1 {
				panic(fmt.Sprintf("after another change: %+v", l))
			}
		})
	}

	t.Run("only a torn line", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.json")
		appendRaw(t, path+".journal", rec(c)[:25])
		fs := openStore(t, path)
		if l := fs.List(); len(l) != 0 {
			panic(fmt.Sprintf("list: %+v", l))
		}
		mustPut(t, fs, c)
		wantSameAfterReopen(t, fs)
	})

	// If the journal cannot be folded away on open, the torn line stays, and
	// the next record must not be glued to it.
	t.Run("journal kept on open", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can write to a read-only directory")
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "s.json")
		fs := openStore(t, path)
		mustPut(t, fs, storeSession(0, "a", StateActive))
		appendRaw(t, fs.journalPath(), rec(c)[:40])
		if err := os.Chmod(dir, 0o500); err != nil { // no snapshot can be written
			panic(err)
		}
		re := openStore(t, path)
		mustPut(t, re, storeSession(1, "b", StateActive)) // the journal itself is writable
		if err := os.Chmod(dir, 0o700); err != nil {
			panic(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			panic(fmt.Sprintf("snapshot written to a read-only directory: %v", err))
		}
		if l := wantSameAfterReopen(t, re).List(); len(l) != 2 {
			panic(fmt.Sprintf("list: %+v", l))
		}
	})
}

// A crash while a snapshot is being written leaves a temp file. It must not
// get in the way, and the store's own temp files are cleaned up.
func TestFileStoreLeftoverTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	fs := openStore(t, path)
	mustPut(t, fs, storeSession(0, "a", StateActive), storeSession(1, "b", StateSealed))
	if err := fs.Compact(); err != nil {
		panic(err)
	}
	mustPut(t, fs, storeSession(2, "c", StateActive))

	own := filepath.Join(dir, ".s.json.123456.tmp")       // half a snapshot of this store
	legacy := filepath.Join(dir, ".sessions-987654.tmp")  // what earlier versions left
	foreign := filepath.Join(dir, ".other.json.1234.tmp") // another store in the same directory
	for _, p := range []string{own, legacy, foreign} {
		if err := os.WriteFile(p, []byte(`[{"id":"ts_half","na`), 0o600); err != nil {
			panic(err)
		}
	}
	re := wantSameAfterReopen(t, fs)
	if l := re.List(); len(l) != 3 {
		panic(fmt.Sprintf("list: %+v", l))
	}
	if _, err := os.Stat(own); !os.IsNotExist(err) {
		panic(fmt.Sprintf("own temp file not removed: %v", err))
	}
	for _, p := range []string{legacy, foreign} {
		if _, err := os.Stat(p); err != nil {
			panic(fmt.Sprintf("%s touched: %v", filepath.Base(p), err))
		}
	}
}

// A crash between the snapshot rename and the journal cut leaves a snapshot
// with a journal whose records it already contains. Replaying them again must
// give the same store.
func TestFileStoreCrashBetweenSnapshotAndJournalCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	ss := make([]Session, 6)
	for i := range ss {
		ss[i] = storeSession(i, fmt.Sprintf("s%d", i), StateAborted)
	}
	mustPut(t, fs, ss...)
	fs.Evict(ss[0].ID, ss[1].ID, ss[2].ID)
	ss[1].Name = "renamed from the archive"
	mustPut(t, fs, ss[1])
	fs.Evict(ss[1].ID)
	ss[2].Name = "hot again"
	mustPut(t, fs, ss[2])
	for _, id := range []string{ss[0].ID, ss[3].ID} { // one archived, one hot
		if err := fs.Delete(id); err != nil {
			panic(err)
		}
	}
	ss[4].Name = "last write wins"
	mustPut(t, fs, ss[4])
	// Deleted and stored again: the delete must not reach the newer record.
	fs.Evict(ss[5].ID)
	if err := fs.Delete(ss[5].ID); err != nil {
		panic(err)
	}
	ss[5].Name = "back after delete"
	mustPut(t, fs, ss[5])
	fs.Evict(ss[5].ID)

	journal, err := os.ReadFile(fs.journalPath())
	if err != nil {
		panic(err)
	}
	if err := fs.Compact(); err != nil {
		panic(err)
	}
	if err := os.WriteFile(fs.journalPath(), journal, 0o600); err != nil { // the cut never happened
		panic(err)
	}
	re := wantSameAfterReopen(t, fs)
	if l := re.List(); len(l) != 4 {
		panic(fmt.Sprintf("list: %+v", l))
	}
	if s, _ := re.Get(ss[1].ID); s.Name != "renamed from the archive" {
		panic(fmt.Sprintf("archived rename lost: %+v", s))
	}
	if _, hot := re.MemStore.Get(ss[2].ID); !hot {
		panic("session put after its eviction is not hot")
	}
	if s, ok := re.Get(ss[5].ID); !ok || s.Name != "back after delete" {
		panic(fmt.Sprintf("session stored again after a delete: %+v %v", s, ok))
	}
}

// Past the size threshold the journal is folded into a snapshot in the
// background, while changes keep coming, without losing any.
func TestFileStoreBackgroundCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	lowerCompactThreshold(fs, 2<<10)
	var written int64
	for i := range 400 {
		s := storeSession(i%25, fmt.Sprintf("round %d", i), StateActive)
		mustPut(t, fs, s)
		b, _ := json.Marshal(journalRecord{Op: "put", Session: &s})
		written += int64(len(b)) + 1
	}
	waitCompaction(fs)

	snap, err := os.Stat(path)
	if err != nil {
		panic(fmt.Sprintf("no snapshot after %d journal bytes: %v", written, err))
	}
	fs.wmu.Lock()
	journalSize, snapSize := fs.journalSize, fs.snapSize
	fs.wmu.Unlock()
	if snapSize != snap.Size() {
		panic(fmt.Sprintf("snapshot size %d, tracked %d", snap.Size(), snapSize))
	}
	onDisk := int64(0)
	if st, err := os.Stat(fs.journalPath()); err == nil {
		onDisk = st.Size()
	}
	if journalSize != onDisk || journalSize >= written {
		panic(fmt.Sprintf("journal: %d bytes tracked, %d on disk, %d written in total", journalSize, onDisk, written))
	}
	if l := wantSameAfterReopen(t, fs).List(); len(l) != 25 || l[24].Name != "round 399" {
		panic(fmt.Sprintf("list: %+v", l))
	}
}

// One Evict call is one archive append and one journal record, whatever the
// number of sessions.
func TestFileStoreEvictBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	ss := make([]Session, 5)
	for i := range ss {
		ss[i] = storeSession(i, fmt.Sprintf("s%d", i), StateAborted)
	}
	mustPut(t, fs, ss...)
	before := len(fileLines(t, fs.journalPath()))

	fs.Evict(ss[0].ID, ss[2].ID, "ts_unknown", ss[4].ID)
	if n := len(fileLines(t, fs.archivePath())); n != 3 {
		panic(fmt.Sprintf("archive has %d lines, want 3", n))
	}
	if n := len(fileLines(t, fs.journalPath())) - before; n != 1 {
		panic(fmt.Sprintf("batch evict wrote %d journal records, want 1", n))
	}
	for i, s := range ss {
		_, hot := fs.MemStore.Get(s.ID)
		if got, ok := fs.Get(s.ID); !ok || got.Name != s.Name || hot != (i%2 == 1) {
			panic(fmt.Sprintf("%s: %+v readable=%v hot=%v", s.ID, got, ok, hot))
		}
	}
	if l := fs.List(); len(l) != 5 {
		panic(fmt.Sprintf("list: %+v", l))
	}

	// Nothing to do: nothing is written.
	fs.Evict()
	fs.Evict(ss[0].ID, "ts_unknown")
	if n := len(fileLines(t, fs.archivePath())); n != 3 {
		panic(fmt.Sprintf("archive has %d lines after no-op evicts, want 3", n))
	}
	wantSameAfterReopen(t, fs)
}

// If the archive cannot be written the sessions stay in the hot set.
func TestFileStoreEvictKeepsSessionsWhenArchiveFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a := storeSession(0, "a", StateAborted)
	mustPut(t, fs, a)
	if err := os.Mkdir(fs.archivePath(), 0o700); err != nil {
		panic(err)
	}
	fs.Evict(a.ID)
	if _, hot := fs.MemStore.Get(a.ID); !hot {
		panic("session dropped although the archive could not be written")
	}
	if err := os.Remove(fs.archivePath()); err != nil {
		panic(err)
	}
	fs.Evict(a.ID)
	if _, hot := fs.MemStore.Get(a.ID); hot {
		panic("session still hot")
	}
	wantSameAfterReopen(t, fs)
}

// The archive is written before the journal: if the evict record is lost the
// session is hot and archived, is listed once and can be evicted again.
func TestFileStoreEvictRecordLost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a, b := storeSession(0, "a", StateAborted), storeSession(1, "b", StateActive)
	mustPut(t, fs, a, b)
	if err := fs.Compact(); err != nil {
		panic(err)
	}
	fs.Evict(a.ID)
	if err := os.Remove(fs.journalPath()); err != nil { // the record never made it
		panic(err)
	}
	re := openStore(t, path)
	if _, hot := re.MemStore.Get(a.ID); !hot {
		panic("session lost")
	}
	if l := re.List(); len(l) != 2 {
		panic(fmt.Sprintf("list: %+v", l))
	}
	re.Evict(a.ID)
	if l := wantSameAfterReopen(t, re).List(); len(l) != 2 {
		panic(fmt.Sprintf("list after second evict: %+v", l))
	}
}

// Neither the archive line nor the evict record is fsynced. If a power
// failure keeps the record but not the line, the session must stay hot.
func TestFileStoreArchiveLineLost(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a, b := storeSession(0, "a", StateAborted), storeSession(1, "b", StateAborted)
	mustPut(t, fs, a, b)
	fs.Evict(a.ID)
	fs.Evict(b.ID)
	lines := fileLines(t, fs.archivePath())
	if err := os.WriteFile(fs.archivePath(), append(lines[0], '\n'), 0o600); err != nil { // b's line never made it
		panic(err)
	}
	re := openStore(t, path)
	if _, hot := re.MemStore.Get(a.ID); hot {
		panic("archived session is hot")
	}
	if s, hot := re.MemStore.Get(b.ID); !hot || s.Name != "b" {
		panic(fmt.Sprintf("session whose archive line was lost: %+v hot=%v", s, hot))
	}
	re.Evict(b.ID)
	if l := wantSameAfterReopen(t, re).List(); len(l) != 2 {
		panic(fmt.Sprintf("list: %+v", l))
	}
}

// A torn last archive line is skipped, and the next archived session does
// not end up on the same line.
func TestFileStoreTornArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a, b := storeSession(0, "a", StateAborted), storeSession(1, "b", StateAborted)
	mustPut(t, fs, a, b)
	fs.Evict(a.ID)
	appendRaw(t, fs.archivePath(), `{"id":"ts_torn","name":"to`)

	re := openStore(t, path)
	if l := re.List(); len(l) != 2 {
		panic(fmt.Sprintf("list: %+v", l))
	}
	re.Evict(b.ID)
	re2 := wantSameAfterReopen(t, re)
	for _, s := range []Session{a, b} {
		if got, ok := re2.Get(s.ID); !ok || got.Name != s.Name {
			panic(fmt.Sprintf("%s: %+v %v", s.ID, got, ok))
		}
		if _, hot := re2.MemStore.Get(s.ID); hot {
			panic(fmt.Sprintf("%s is hot", s.ID))
		}
	}
}

// A delete that was journalled but did not get to rewrite the archive is
// finished when the store is opened.
func TestFileStoreDeleteFinishedOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	a, b := storeSession(0, "a", StateAborted), storeSession(1, "b", StateAborted)
	mustPut(t, fs, a, b)
	fs.Evict(a.ID, b.ID)
	archive, err := os.ReadFile(fs.archivePath())
	if err != nil {
		panic(err)
	}
	if err := fs.Delete(a.ID); err != nil {
		panic(err)
	}
	if err := os.WriteFile(fs.archivePath(), archive, 0o600); err != nil { // the rewrite never happened
		panic(err)
	}
	re := openStore(t, path)
	if _, ok := re.Get(a.ID); ok {
		panic("deleted session is back")
	}
	if n := len(fileLines(t, re.archivePath())); n != 1 {
		panic(fmt.Sprintf("archive has %d lines, want 1", n))
	}
	if l := wantSameAfterReopen(t, re).List(); len(l) != 1 || l[0].ID != b.ID {
		panic(fmt.Sprintf("list: %+v", l))
	}
}

// Writers and readers from many goroutines, with background compactions in
// between (run with -race). What the store serves at the end is what a
// restart loads.
func TestFileStoreConcurrentUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	lowerCompactThreshold(fs, 4<<10)
	const workers, perWorker, rounds = 8, 20, 6
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range rounds {
				var ids []string
				for i := range perWorker {
					s := storeSession(w*perWorker+i, fmt.Sprintf("w%d r%d", w, r), StateAborted)
					if err := fs.Put(s); err != nil {
						t.Errorf("put: %v", err)
						return
					}
					ids = append(ids, s.ID)
					if got, ok := fs.Get(s.ID); !ok || got.Name != s.Name {
						t.Errorf("get after put: %+v %v", got, ok)
						return
					}
				}
				fs.Evict(ids[:perWorker/2]...)
				for _, id := range ids {
					if _, ok := fs.Get(id); !ok {
						t.Errorf("%s unreadable after evict", id)
						return
					}
				}
				if err := fs.Delete(ids[r]); err != nil {
					t.Errorf("delete: %v", err)
					return
				}
				if _, ok := fs.Get(ids[r]); ok {
					t.Errorf("%s readable after delete", ids[r])
					return
				}
				if l := fs.List(); len(l) == 0 {
					t.Error("empty list")
					return
				}
			}
		}()
	}
	wg.Wait()
	waitCompaction(fs)
	if t.Failed() {
		return
	}
	// The last round re-put every session and then deleted one per worker.
	if l := wantSameAfterReopen(t, fs).List(); len(l) != workers*(perWorker-1) {
		panic(fmt.Sprintf("%d sessions, want %d", len(l), workers*(perWorker-1)))
	}
}

// batchStore records the Evict calls a Manager makes.
type batchStore struct {
	*MemStore
	mu    sync.Mutex
	calls [][]string
}

func (b *batchStore) Evict(ids ...string) {
	b.mu.Lock()
	b.calls = append(b.calls, ids)
	b.mu.Unlock()
	b.MemStore.Evict(ids...)
}

// A retention sweep hands all expired sessions to the store in one call.
func TestSweepEvictsInOneBatch(t *testing.T) {
	st := &batchStore{MemStore: NewMemStore()}
	m := newMgr(t, newFakeCollector(t), func(c *Config) { c.Store = st; c.Retention = time.Hour })
	var ids []string
	for range 5 {
		s, err := m.Start(StartRequest{})
		if err != nil {
			panic(err)
		}
		m.Abort(s.ID)
		ids = append(ids, s.ID)
	}
	live, _ := m.Start(StartRequest{})
	m.Sweep(time.Now()) // nothing is old enough
	m.Sweep(time.Now().Add(2 * time.Hour))
	m.Sweep(time.Now().Add(3 * time.Hour)) // nothing left to evict
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.calls) != 1 || !reflect.DeepEqual(st.calls[0], ids) {
		panic(fmt.Sprintf("evict calls: %v, want one with %v", st.calls, ids))
	}
	if l := st.List(); len(l) != 1 || l[0].ID != live.ID {
		panic(fmt.Sprintf("store after sweep: %+v", l))
	}
}
