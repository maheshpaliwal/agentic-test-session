package controlplane

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/internal/mapx"
)

// journalCompactBytes is the smallest journal size at which the journal is
// folded into a fresh snapshot (the threshold is the snapshot size when that
// is larger, so the amortised write per change stays constant).
const journalCompactBytes = 4 << 20

// FileStore is a Store kept in three files:
//
//   - path: a snapshot of the hot set, a JSON array of sessions;
//   - path.journal: the changes since that snapshot, one JSON object per line
//     (see journalRecord), appended on every Put, Evict and Delete;
//   - path.archive: the sessions evicted from the hot set, one JSON session
//     per line, append-only; the last line of an id wins.
//
// A change costs one journal append, whatever the size of the hot set. The
// journal is replayed on top of the snapshot when the store is opened and
// folded into a new snapshot then, on Compact, and in the background once it
// passes a size threshold.
//
// Durability: a change has reached the operating system when Put, Evict or
// Delete returns, but is not fsynced, so it survives a crash of the process,
// and a power failure may take back the last few changes. It cannot do more
// than that: snapshots are fsynced before they replace the old one, and the
// archive is fsynced before a snapshot without the archived sessions is
// published, so an eviction that did not reach the disk is undone (the
// session is hot again) instead of losing the session. A torn last journal or
// archive line is skipped when the store is opened.
//
// The archived sessions are indexed in memory (latest record per id), so Get
// and List never read the archive.
//
// A FileStore is safe for concurrent use. One process may use a path at a
// time.
type FileStore struct {
	*MemStore // the hot set
	path      string

	// wmu serialises the writers and all file changes. It is held across the
	// in-memory change and its journal append, so the journal order is the
	// order in which the hot set changed.
	wmu         sync.Mutex
	journalSize int64         // bytes in the journal file
	snapSize    int64         // bytes in the snapshot file
	compactAt   int64         // journal size that starts the next compaction
	compactMin  int64         // journalCompactBytes; tests lower it
	compacting  chan struct{} // non-nil while a background compaction runs; closed when it ends
	// archiveUnsynced is set by an archive append that was not fsynced. The
	// next snapshot waits for that fsync: it no longer holds those sessions.
	archiveUnsynced bool

	// amu guards arch. Writers take it inside wmu, readers on its own.
	amu  sync.RWMutex
	arch map[string]Session // latest archived record per id
}

// journalRecord is one line of the journal.
type journalRecord struct {
	// Op is "put" (Session joins or changes in the hot set), "evict" (IDs
	// left the hot set for the archive) or "delete" (IDs are gone for good).
	Op      string   `json:"op"`
	Session *Session `json:"session,omitempty"`
	IDs     []string `json:"ids,omitempty"`
}

// NewFileStore opens the store at path, creating it on the first change. It
// reads the snapshot (the only file written by earlier versions besides the
// archive), replays the journal and folds it into a new snapshot. Temp files
// left by a crash are removed.
func NewFileStore(path string) (*FileStore, error) {
	f := &FileStore{MemStore: NewMemStore(), path: path, arch: map[string]Session{}, compactMin: journalCompactBytes}
	f.removeTempFiles()

	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var list []Session
	if len(b) > 0 { // an empty file is what a power failure could leave before snapshots were fsynced
		if err := json.Unmarshal(b, &list); err != nil {
			return nil, err
		}
	}
	for _, s := range list {
		f.m[s.ID] = s
	}
	f.snapSize = int64(len(b))

	if err := f.loadArchive(); err != nil {
		return nil, err
	}
	replayed, archiveChanged, err := f.replayJournal()
	if err != nil {
		return nil, err
	}
	if archiveChanged {
		// A delete was journalled but the archive not rewritten yet. The
		// journal goes away below, so this must not fail silently.
		if err := f.writeArchiveLocked(); err != nil {
			return nil, err
		}
	}
	f.compactAt = f.journalSize + max(f.compactMin, f.snapSize)
	if replayed {
		// The journal may hold evictions whose archive lines the last run
		// never fsynced. Best effort: on failure the journal stays and is
		// replayed again.
		f.archiveUnsynced = true
		_ = f.compactLocked()
	}
	return f, nil
}

func (f *FileStore) archivePath() string { return f.path + ".archive" }
func (f *FileStore) journalPath() string { return f.path + ".journal" }

// tempPattern names the temp files of this store (os.CreateTemp pattern).
func (f *FileStore) tempPattern() string { return "." + filepath.Base(f.path) + ".*.tmp" }

// removeTempFiles deletes temp files a crash left next to the store.
func (f *FileStore) removeTempFiles() {
	dir := filepath.Dir(f.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	prefix, suffix, _ := strings.Cut(f.tempPattern(), "*")
	for _, e := range entries {
		if n := e.Name(); strings.HasPrefix(n, prefix) && strings.HasSuffix(n, suffix) && e.Type().IsRegular() {
			_ = os.Remove(filepath.Join(dir, n))
		}
	}
}

// readLines calls fn for every complete or final line of the file at path
// (without the newline; empty lines are skipped). It reports whether the
// file exists and whether its last byte is a newline. A missing file is not
// an error.
func readLines(path string, fn func(line []byte)) (exists, endsWithNewline bool, err error) {
	fh, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	defer fh.Close()
	r := bufio.NewReaderSize(fh, 64*1024)
	endsWithNewline = true
	for {
		line, rerr := r.ReadBytes('\n')
		if len(line) > 0 {
			endsWithNewline = line[len(line)-1] == '\n'
			if line = bytes.TrimSpace(line); len(line) > 0 {
				fn(line)
			}
		}
		if rerr == io.EOF {
			return true, endsWithNewline, nil
		}
		if rerr != nil {
			return true, endsWithNewline, rerr
		}
	}
}

// loadArchive builds the in-memory index of the archive. Lines that do not
// parse (a torn append) are skipped.
func (f *FileStore) loadArchive() error {
	_, nl, err := readLines(f.archivePath(), func(line []byte) {
		var s Session
		if json.Unmarshal(line, &s) == nil && s.ID != "" {
			f.arch[s.ID] = s
		}
	})
	if err != nil {
		return err
	}
	if !nl {
		// End the torn line, so the next append starts a line of its own.
		return appendFile(f.archivePath(), []byte{'\n'}, false)
	}
	return nil
}

// replayJournal applies the journal to the hot set and the archive index.
// Lines that do not parse (a torn append) are skipped. Replaying records that
// a snapshot already contains is harmless: the last record of an id decides
// its state. It reports whether a journal file exists and whether a delete
// had to be finished in the archive index (the archive file must then be
// rewritten).
func (f *FileStore) replayJournal() (exists, archiveChanged bool, err error) {
	// Ids whose last record is a delete. Only those are dropped from the
	// archive: after "delete, put, evict" the archive holds a newer record.
	deleted := map[string]bool{}
	exists, nl, err := readLines(f.journalPath(), func(line []byte) {
		var rec journalRecord
		if json.Unmarshal(line, &rec) != nil {
			return
		}
		switch rec.Op {
		case "put":
			if rec.Session != nil && rec.Session.ID != "" {
				f.m[rec.Session.ID] = *rec.Session
				delete(deleted, rec.Session.ID)
			}
		case "evict":
			for _, id := range rec.IDs {
				// The archive line is written before the record, but neither
				// is fsynced: if a power failure kept only the record, the
				// session stays hot.
				if _, ok := f.arch[id]; ok {
					delete(f.m, id)
				}
			}
		case "delete":
			for _, id := range rec.IDs {
				delete(f.m, id)
				deleted[id] = true
			}
		}
	})
	for id := range deleted {
		if _, ok := f.arch[id]; ok {
			delete(f.arch, id)
			archiveChanged = true
		}
	}
	if err != nil {
		return false, false, err
	}
	if !exists {
		return false, false, nil
	}
	if !nl {
		// End the torn line, so that a later append starts a line of its own
		// (this matters only if the journal cannot be folded away now).
		if err := appendFile(f.journalPath(), []byte{'\n'}, false); err != nil {
			return false, false, err
		}
	}
	st, err := os.Stat(f.journalPath())
	if err != nil {
		return false, false, err
	}
	f.journalSize = st.Size()
	return true, archiveChanged, nil
}

// Put implements Store. The session is in the hot set at once and in the
// journal when Put returns (see FileStore for what that guarantees).
func (f *FileStore) Put(s Session) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	_ = f.MemStore.Put(s)
	return f.journalLocked(journalRecord{Op: "put", Session: &s})
}

// Evict implements Evictor: it moves the sessions out of the hot set into
// the archive with one archive write for the whole batch. They stay readable
// through Get and List. Ids that are not in the hot set are ignored. If the
// archive cannot be written the sessions stay in the hot set.
func (f *FileStore) Evict(ids ...string) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	var (
		moved []Session
		lines bytes.Buffer
		enc   = json.NewEncoder(&lines) // one line per session
	)
	for _, id := range ids {
		if s, ok := f.MemStore.Get(id); ok && enc.Encode(s) == nil {
			moved = append(moved, s)
		}
	}
	if len(moved) == 0 {
		return
	}
	// A session that is archived already (it was changed after its eviction)
	// is fsynced now: replay could not tell its new line from the old one.
	// For all others the fsync waits for the next snapshot.
	rearchived := false
	f.amu.RLock()
	for _, s := range moved {
		if _, ok := f.arch[s.ID]; ok {
			rearchived = true
			break
		}
	}
	f.amu.RUnlock()
	if err := appendFile(f.archivePath(), lines.Bytes(), rearchived); err != nil {
		return // keep them in memory rather than lose them
	}
	f.archiveUnsynced = !rearchived
	gone := make([]string, len(moved))
	f.amu.Lock()
	for i, s := range moved {
		f.arch[s.ID], gone[i] = s, s.ID
	}
	f.amu.Unlock()
	f.MemStore.Evict(gone...) // after the index has them: a reader finds them in one or the other
	// If this fails the sessions are hot again after a restart, as well as
	// archived; the hot copy wins and they are evicted once more.
	_ = f.journalLocked(journalRecord{Op: "evict", IDs: gone})
}

// Delete implements Store: the session leaves the hot set and the archive
// for good. Deleting an archived session rewrites the archive file.
func (f *FileStore) Delete(id string) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	_, hot := f.MemStore.Get(id)
	f.amu.RLock()
	_, archived := f.arch[id]
	f.amu.RUnlock()
	if !hot && !archived {
		return nil
	}
	// Journal first: if the archive rewrite below does not happen (error or
	// crash), opening the store finishes the delete.
	if err := f.journalLocked(journalRecord{Op: "delete", IDs: []string{id}}); err != nil {
		return err
	}
	if archived {
		f.amu.Lock()
		old := f.arch[id]
		delete(f.arch, id)
		f.amu.Unlock()
		if err := f.writeArchiveLocked(); err != nil {
			f.amu.Lock()
			f.arch[id] = old // still on disk: keep it readable and the delete repeatable
			f.amu.Unlock()
			f.MemStore.Evict(id)
			return err
		}
	}
	f.MemStore.Evict(id)
	return nil
}

// Get implements Store, falling back to the archive index for evicted
// sessions.
func (f *FileStore) Get(id string) (Session, bool) {
	if s, ok := f.MemStore.Get(id); ok {
		return s, true
	}
	f.amu.RLock()
	defer f.amu.RUnlock()
	s, ok := f.arch[id]
	return s, ok
}

// List implements Store: the hot sessions plus the archived ones. A session
// that is in both (an archived session that was changed again) is listed
// once, with its hot record.
func (f *FileStore) List() []Session {
	f.MemStore.mu.RLock()
	f.amu.RLock()
	out := make([]Session, 0, len(f.m)+len(f.arch))
	for _, s := range f.m {
		out = append(out, s)
	}
	for id, s := range f.arch {
		if _, hot := f.m[id]; !hot {
			out = append(out, s)
		}
	}
	f.amu.RUnlock()
	f.MemStore.mu.RUnlock()
	sortSessions(out)
	return out
}

// Compact folds the journal into a new snapshot and removes it, after which
// the snapshot alone holds the hot set (as files of earlier versions did).
// The store stays usable. It is called when the store is opened; call it on
// shutdown to leave a single up-to-date file behind.
func (f *FileStore) Compact() error {
	for {
		f.wmu.Lock()
		running := f.compacting
		if running == nil {
			break
		}
		f.wmu.Unlock()
		<-running
	}
	defer f.wmu.Unlock()
	return f.compactLocked()
}

// compactLocked writes the hot set as the snapshot and removes the journal.
// A crash in between leaves the new snapshot with the old journal, which
// replays to the same state. Caller holds f.wmu (or is the constructor).
func (f *FileStore) compactLocked() error {
	if f.archiveUnsynced {
		if err := syncFile(f.archivePath()); err != nil {
			return err
		}
		f.archiveUnsynced = false
	}
	data, err := marshalSnapshot(f.MemStore.List())
	if err != nil {
		return err
	}
	if err := f.writeFileAtomic(f.path, data); err != nil {
		return err
	}
	f.snapSize = int64(len(data))
	if err := os.Remove(f.journalPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f.journalSize = 0
	f.compactAt = max(f.compactMin, f.snapSize)
	return nil
}

func marshalSnapshot(hot []Session) ([]byte, error) {
	if hot == nil {
		hot = []Session{}
	}
	return json.MarshalIndent(hot, "", "  ")
}

// journalLocked appends rec to the journal and starts a background
// compaction when the journal has grown past the threshold. Caller holds
// f.wmu.
func (f *FileStore) journalLocked(rec journalRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err := appendFile(f.journalPath(), b, false); err != nil {
		// Drop a partial line, so that the next record is not glued to it
		// (it would be skipped on replay).
		_ = os.Truncate(f.journalPath(), f.journalSize)
		return err
	}
	f.journalSize += int64(len(b))
	if f.journalSize >= f.compactAt && f.compacting == nil {
		// Copy the hot set now, in step with the journal; marshalling and
		// writing it happens without the lock.
		f.MemStore.mu.RLock()
		hot := mapx.Values(f.m)
		f.MemStore.mu.RUnlock()
		done := make(chan struct{})
		f.compacting = done
		go f.compactInBackground(hot, f.journalSize, f.archiveUnsynced, done)
		f.archiveUnsynced = false
	}
	return nil
}

// compactInBackground writes hot, the hot set as of journal offset upto, as
// the new snapshot, and then cuts the journal down to the records after
// upto. Only the two renames at the end run under f.wmu. Every intermediate
// state on disk loads to the current hot set: old snapshot with the whole
// journal, new snapshot with the whole journal (replaying records a snapshot
// already has is harmless), new snapshot with the rest of the journal. With
// syncArchive the archive is fsynced first: it holds sessions that hot no
// longer does.
func (f *FileStore) compactInBackground(hot []Session, upto int64, syncArchive bool, done chan struct{}) {
	sortSessions(hot)
	var (
		tmp  string
		data []byte
		err  error
	)
	if syncArchive {
		err = syncFile(f.archivePath())
	}
	if err == nil {
		data, err = marshalSnapshot(hot)
	}
	if err == nil {
		tmp, err = f.writeTemp(data)
	}

	f.wmu.Lock()
	defer f.wmu.Unlock()
	defer close(done)
	f.compacting = nil
	// Whatever happens next, wait for the journal to grow again before the
	// next attempt.
	f.compactAt = f.journalSize + max(f.compactMin, f.snapSize)
	if err != nil {
		f.archiveUnsynced = f.archiveUnsynced || syncArchive
		return
	}
	if err := os.Rename(tmp, f.path); err != nil {
		_ = os.Remove(tmp)
		return
	}
	syncDir(filepath.Dir(f.path))
	f.snapSize = int64(len(data))

	rest, err := readFrom(f.journalPath(), upto)
	if err != nil {
		return // the whole journal stays: correct, only longer
	}
	if len(rest) == 0 {
		err = os.Remove(f.journalPath())
	} else {
		err = f.writeFileAtomic(f.journalPath(), rest)
	}
	if err != nil {
		return
	}
	f.journalSize = int64(len(rest))
	f.compactAt = f.journalSize + max(f.compactMin, f.snapSize)
}

// writeArchiveLocked replaces the archive file with the indexed records (one
// line per session, which also drops the superseded lines of sessions that
// were archived more than once). An empty archive is removed. Caller holds
// f.wmu (or is the constructor).
func (f *FileStore) writeArchiveLocked() error {
	f.amu.RLock()
	all := mapx.Values(f.arch)
	f.amu.RUnlock()
	if len(all) == 0 {
		if err := os.Remove(f.archivePath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	sortSessions(all)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, s := range all {
		if err := enc.Encode(s); err != nil {
			return err
		}
	}
	return f.writeFileAtomic(f.archivePath(), buf.Bytes())
}

// writeTemp writes data to a new temp file next to the store, fsyncs it and
// returns its name.
func (f *FileStore) writeTemp(data []byte) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(f.path), f.tempPattern())
	if err != nil {
		return "", err
	}
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// writeFileAtomic replaces path with data through a synced temp file and a
// rename, so that a crash or a power failure leaves either the old or the
// new content, never a partial file.
func (f *FileStore) writeFileAtomic(path string, data []byte) error {
	tmp, err := f.writeTemp(data)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir makes a rename in dir durable. Best effort: not every platform can
// sync a directory.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// syncFile fsyncs the file at path. A missing file has nothing to sync.
func syncFile(path string) error {
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	err = fh.Sync()
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	return err
}

// appendFile appends data to path with one write, creating the file if
// needed, and optionally fsyncs it.
func appendFile(path string, data []byte, sync bool) error {
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = fh.Write(data)
	if err == nil && sync {
		err = fh.Sync()
	}
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	return err
}

// readFrom returns the content of the file at path from offset on.
func readFrom(path string, offset int64) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	if _, err := fh.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(fh)
}
