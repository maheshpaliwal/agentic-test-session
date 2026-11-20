package controlplane

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

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
//     per line, appended; the last line of an id wins. The line of a deleted
//     session, and the old line of a session that was archived again, is
//     overwritten with spaces. Such blank lines are skipped by every reader
//     and dropped when the store is opened.
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
// archive line is skipped when the store is opened. Overwriting an archive
// line is fsynced before Delete or Evict returns.
//
// Memory: the hot set is held in full. Of an archived session the store
// holds its id and a fixed-size index entry (archRef: where its line is, its
// state and its seal marker), about 100 bytes, and reads the line from the
// file when the session is asked for. Up to archCacheMax decoded archived
// sessions are cached, so Get and List do not touch the file while the
// archive is that small. List returns every session, so its result grows
// with the archive whatever the store holds; beyond the cache it reads the
// archive once per call, without holding a lock. ListState and ListPending
// read only the lines of the sessions they return.
//
// Read errors: Get and the List methods have no error result. An archived
// session whose line cannot be read (an I/O error, or a file that was cut or
// changed behind the store's back) is missing from what they return. Every
// such session is counted (ReadErrors) and logged, and GetErr tells it from a
// session the store does not have.
//
// A FileStore is safe for concurrent use. One process may use a path at a
// time.
type FileStore struct {
	*MemStore // the hot set
	path      string
	log       *slog.Logger
	readErrs  atomic.Int64 // archived sessions that could not be read

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
	// archSize is the offset at which the next archive line is written. It is
	// never below the size of the archive file (a line written below the end
	// would leave the lines after it in place, to win over it when the archive
	// is loaded), and equal to it except after a failed append whose bytes
	// could not be found (see undoAppend).
	archSize int64
	// archTorn is set when the bytes before archSize may not end with a
	// newline (a failed append that could neither be removed nor ended). The
	// next append starts with a newline of its own.
	archTorn bool
	// stale holds, by id, the archive lines that a later line of the id
	// replaced and that are still in the file. It is empty unless overwriting
	// such a line failed. Delete overwrites them with the latest line, so that
	// no older record of a deleted session is left to come back.
	stale map[string][]archRef

	// amu guards arch and ar. Writers take it inside wmu, readers on its own.
	// A line is overwritten only after its entry has left arch, so the line an
	// entry points at is intact for as long as a reader holds amu.
	amu  sync.RWMutex
	arch map[string]archRef // where the latest archived record of an id is
	ar   *os.File           // the archive file, for reading and writing at offsets; nil while there is none
	// archGen counts the archive files this store has removed (see
	// removeArchive): an offset names a line only together with it.
	archGen uint64

	cache archCache

	// ops are the calls that change the archive file through its handle.
	// Tests replace them to make them fail.
	ops archOps

	// testHookListed, if set, runs in list between taking the entries from
	// the index and reading their lines.
	testHookListed func()
}

// archOps are the calls FileStore makes to change the archive file: the ones
// whose failure it has to recover from. They are the methods of os.File
// unless a test injects faults.
type archOps struct {
	writeAt  func(fh *os.File, b []byte, off int64) (int, error)
	truncate func(fh *os.File, size int64) error
	sync     func(fh *os.File) error
	size     func(fh *os.File) (int64, error)
}

func defaultArchOps() archOps {
	return archOps{
		writeAt:  (*os.File).WriteAt,
		truncate: (*os.File).Truncate,
		sync:     (*os.File).Sync,
		size: func(fh *os.File) (int64, error) {
			st, err := fh.Stat()
			if err != nil {
				return 0, err
			}
			return st.Size(), nil
		},
	}
}

// archRef is the index entry of an archived session.
type archRef struct {
	off         int64  // offset of the session's line in the archive file
	n           uint32 // length of the line, without the newline
	state       uint8  // the session's State, as an index into archStates
	sealPending bool   // the session's SealPending
}

// archStates lists the states an archRef can name. Index 0 stands for any
// other state: the line has to be read to know it.
var archStates = [...]State{"", StateCreated, StateActive, StateDraining, StateSealed, StateAborted, StateExpired}

func archStateCode(st State) uint8 {
	for i := 1; i < len(archStates); i++ {
		if archStates[i] == st {
			return uint8(i)
		}
	}
	return 0
}

func newArchRef(s Session, off int64, n int) archRef {
	return archRef{off: off, n: uint32(n), state: archStateCode(s.State), sealPending: s.SealPending}
}

// archCacheMax is the number of decoded archived sessions kept in memory
// (some 12 MB of sessions with a name, a bundle and a binding).
const archCacheMax = 16384

// archCache holds up to max decoded archived sessions by id, each with the
// place of the line it was decoded from (FileStore.archGen and the offset).
// An entry counts only while the index points at that place. No line is ever
// written to a place that held another one, so entries need no invalidation:
// one that is out of date is not used, and is replaced or dropped in time.
type archCache struct {
	mu  sync.Mutex
	max int
	m   map[string]cachedSession
}

type cachedSession struct {
	gen uint64
	off int64
	s   Session
}

func (c *archCache) get(id string, gen uint64, off int64) (Session, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok || e.gen != gen || e.off != off {
		return Session{}, false
	}
	return e.s, true
}

// put caches s. When the cache is full it makes room by dropping an arbitrary
// entry if evict is set, and caches nothing otherwise.
func (c *archCache) put(id string, gen uint64, off int64, s Session, evict bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max <= 0 {
		return
	}
	if _, ok := c.m[id]; !ok && len(c.m) >= c.max {
		if !evict {
			return
		}
		for k := range c.m {
			delete(c.m, k)
			break
		}
	}
	if c.m == nil {
		c.m = map[string]cachedSession{}
	}
	c.m[id] = cachedSession{gen, off, s}
}

// journalRecord is one line of the journal.
type journalRecord struct {
	// Op is "put" (Session joins or changes in the hot set), "evict" (IDs
	// left the hot set for the archive) or "delete" (IDs are gone for good).
	Op      string   `json:"op"`
	Session *Session `json:"session,omitempty"`
	IDs     []string `json:"ids,omitempty"`
}

// FileStoreOption configures a FileStore when it is opened.
type FileStoreOption func(*FileStore)

// WithStoreLogger sets the logger for what the store cannot report to its
// caller: archived sessions it fails to read. The default is slog.Default().
func WithStoreLogger(l *slog.Logger) FileStoreOption {
	return func(f *FileStore) {
		if l != nil {
			f.log = l
		}
	}
}

// NewFileStore opens the store at path, creating it on the first change. It
// reads the snapshot (the only file written by earlier versions besides the
// archive), indexes the archive, replays the journal and folds it into a new
// snapshot. Temp files left by a crash are removed, and so are the archive
// lines that no longer count (see tidyArchive).
func NewFileStore(path string, opts ...FileStoreOption) (*FileStore, error) {
	f := &FileStore{MemStore: NewMemStore(), path: path, arch: map[string]archRef{}, stale: map[string][]archRef{}, compactMin: journalCompactBytes}
	f.log = slog.Default()
	for _, o := range opts {
		o(f)
	}
	f.cache.max = archCacheMax
	f.ops = defaultArchOps()
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

	replayed, err := f.loadArchiveAndJournal()
	if err != nil {
		if f.ar != nil {
			_ = f.ar.Close()
		}
		return nil, err
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

// loadArchiveAndJournal indexes the archive, replays the journal and tidies
// the archive. It reports whether a journal file exists.
func (f *FileStore) loadArchiveAndJournal() (replayed bool, err error) {
	if err := f.loadArchive(); err != nil {
		return false, err
	}
	replayed, unfinished, err := f.replayJournal()
	if err != nil {
		return false, err
	}
	// If a delete was journalled but its line is still in the archive, this
	// must not fail silently: the caller folds the journal away.
	return replayed, f.tidyArchive(unfinished)
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

// scanLines calls fn for every complete or final line of r that is not
// blank, with the white space around it removed and with the offset in r of
// its first byte. It returns the number of bytes in r and whether the last
// one is a newline (true for an empty r).
func scanLines(r io.Reader, fn func(off int64, line []byte)) (size int64, endsWithNewline bool, err error) {
	br := bufio.NewReaderSize(r, 256*1024)
	endsWithNewline = true
	for {
		raw, rerr := br.ReadBytes('\n')
		if len(raw) > 0 {
			endsWithNewline = raw[len(raw)-1] == '\n'
			if line := bytes.TrimSpace(raw); len(line) > 0 {
				lead := len(raw) - len(bytes.TrimLeftFunc(raw, unicode.IsSpace))
				fn(size+int64(lead), line)
			}
			size += int64(len(raw))
		}
		if rerr == io.EOF {
			return size, endsWithNewline, nil
		}
		if rerr != nil {
			return size, endsWithNewline, rerr
		}
	}
}

// readLines is scanLines over the file at path, without the offsets. It
// reports whether the file exists; a missing file is not an error.
func readLines(path string, fn func(line []byte)) (exists, endsWithNewline bool, err error) {
	fh, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	defer fh.Close()
	_, endsWithNewline, err = scanLines(fh, func(_ int64, line []byte) { fn(line) })
	return true, endsWithNewline, err
}

// loadArchive opens the archive file and builds the in-memory index of it.
// Lines that do not parse (a torn append) and blank lines are skipped. An
// older line of an id that has a later one is noted in f.stale.
func (f *FileStore) loadArchive() error {
	fh, err := os.OpenFile(f.archivePath(), os.O_RDWR, 0)
	if errors.Is(err, os.ErrPermission) {
		fh, err = os.Open(f.archivePath()) // read what is there; evictions and deletes of archived sessions will fail
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	f.ar = fh
	size, nl, err := scanLines(fh, func(off int64, line []byte) {
		var s Session
		if json.Unmarshal(line, &s) != nil || s.ID == "" {
			return
		}
		if old, ok := f.arch[s.ID]; ok {
			f.stale[s.ID] = append(f.stale[s.ID], old)
		}
		f.arch[s.ID] = newArchRef(s, off, len(line))
	})
	if err != nil {
		return err
	}
	f.archSize = size
	if !nl {
		// End the torn line, so the next append starts a line of its own.
		if _, err := fh.WriteAt([]byte{'\n'}, size); err != nil {
			return err
		}
		f.archSize++
	}
	return nil
}

// tidyArchive makes the archive file hold the line of every indexed session
// and nothing else, when the store is opened. That drops the lines given in
// unfinished (of sessions whose delete the journal replay had to finish), the
// stale lines, and blank and torn lines. It writes the archive anew; if that
// fails, the unfinished and stale lines are overwritten in place instead. An
// error is returned only if a line of a deleted session is left.
func (f *FileStore) tidyArchive(unfinished []archRef) error {
	if f.ar == nil {
		return nil
	}
	live := int64(0)
	for _, r := range f.arch {
		live += int64(r.n) + 1
	}
	if len(unfinished) == 0 && len(f.stale) == 0 && live == f.archSize {
		return nil
	}
	err := f.rewriteArchive()
	if err == nil {
		return nil
	}
	if berr := f.blankLines(unfinished...); berr != nil {
		return errors.Join(err, berr)
	}
	for id, refs := range f.stale {
		if f.blankLines(refs...) == nil { // if not, Delete tries again
			delete(f.stale, id)
		}
	}
	return nil
}

// rewriteArchive replaces the archive file with one that holds exactly the
// line of every indexed session, in the order they have in the old file, and
// points the index at it. An empty archive is removed. The new file is synced
// before it takes the place of the old one, so a crash leaves one or the
// other, and both load to the same index. Only the constructor calls it:
// nothing else may be using the index or the file.
func (f *FileStore) rewriteArchive() error {
	type entry struct {
		id  string
		ref archRef
	}
	live := make([]entry, 0, len(f.arch))
	for id, r := range f.arch {
		live = append(live, entry{id, r})
	}
	slices.SortFunc(live, func(a, b entry) int { return cmp.Compare(a.ref.off, b.ref.off) })

	old := f.ar
	if len(live) == 0 {
		if err := os.Remove(f.archivePath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		_ = old.Close()
		f.ar, f.archSize = nil, 0
		clear(f.stale)
		return nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), f.tempPattern())
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(tmp, 1<<20)
	lr := lineReader{f: old}
	size := int64(0)
	for i := range live {
		var line []byte
		if line, err = lr.read(live[i].ref); err != nil {
			break
		}
		if _, err = w.Write(line); err != nil {
			break
		}
		if err = w.WriteByte('\n'); err != nil {
			break
		}
		live[i].ref.off = size
		size += int64(len(line)) + 1
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if err == nil {
		err = os.Rename(tmp.Name(), f.archivePath())
	}
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	syncDir(filepath.Dir(f.path))
	for _, e := range live {
		f.arch[e.id] = e.ref
	}
	_ = old.Close()
	f.ar, f.archSize = tmp, size // the handle follows the file through the rename
	clear(f.stale)
	return nil
}

// lineReader reads archive lines through f. Lines asked for in ascending
// offset order share reads of lineReaderChunk bytes. A returned line is valid
// until the next call.
type lineReader struct {
	f   *os.File
	buf []byte
	off int64 // file offset of buf[0]
}

const lineReaderChunk = 256 << 10

func (r *lineReader) read(ref archRef) ([]byte, error) {
	if r.f == nil {
		return nil, os.ErrClosed
	}
	end := ref.off + int64(ref.n)
	if ref.off < r.off || end > r.off+int64(len(r.buf)) {
		want := max(int(ref.n), lineReaderChunk)
		if cap(r.buf) < want {
			r.buf = make([]byte, want)
		}
		n, err := r.f.ReadAt(r.buf[:want], ref.off)
		r.buf, r.off = r.buf[:n], ref.off
		if n < int(ref.n) {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF // the file is shorter than the index says
			}
			return nil, err
		}
	}
	return r.buf[ref.off-r.off : end-r.off], nil
}

// decodeArchived parses the archive line of the session id. A line that does
// not hold that session (it was overwritten, or the file was changed behind
// the store's back) is an error.
func decodeArchived(id string, line []byte) (Session, error) {
	var s Session
	if err := json.Unmarshal(line, &s); err != nil {
		return Session{}, err
	}
	if s.ID != id {
		return Session{}, errors.New("archive line holds another session")
	}
	return s, nil
}

// blankLines overwrites archive lines with spaces and fsyncs the archive.
// The lines are gone for every reader of the file (blank lines are skipped)
// and the lines after them stay where they are. Caller holds f.wmu (or is
// the constructor) and has taken the lines out of the index.
func (f *FileStore) blankLines(refs ...archRef) error {
	if len(refs) == 0 {
		return nil
	}
	if f.ar == nil {
		return os.ErrClosed
	}
	var spaces []byte
	for _, r := range refs {
		if len(spaces) < int(r.n) {
			spaces = bytes.Repeat([]byte{' '}, int(r.n))
		}
		if _, err := f.ops.writeAt(f.ar, spaces[:r.n], r.off); err != nil {
			return err
		}
	}
	if err := f.ops.sync(f.ar); err != nil {
		return err
	}
	f.archiveUnsynced = false
	return nil
}

// archiveForAppend returns the archive file, creating it if there is none.
// Caller holds f.wmu.
func (f *FileStore) archiveForAppend() (*os.File, error) {
	if f.ar != nil {
		return f.ar, nil
	}
	fh, err := os.OpenFile(f.archivePath(), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return nil, err
	}
	f.amu.Lock()
	f.ar = fh
	f.amu.Unlock()
	f.archSize = st.Size()
	return fh, nil
}

// removeArchive deletes the archive file; the index is empty. If the file
// cannot be removed it holds blank lines only, and is appended to by the next
// eviction or removed when the store is opened. Caller holds f.wmu.
func (f *FileStore) removeArchive() {
	f.amu.Lock()
	ar := f.ar
	f.ar = nil
	f.archGen++ // the next archive file starts at offset 0 again
	f.amu.Unlock()
	f.archSize, f.archiveUnsynced = 0, false
	clear(f.stale)
	if ar != nil {
		_ = ar.Close()
	}
	_ = os.Remove(f.archivePath())
}

// syncArchive fsyncs the archive file, if there is one.
func (f *FileStore) syncArchive() error {
	f.amu.RLock()
	ar := f.ar
	f.amu.RUnlock()
	if ar == nil {
		return nil
	}
	if err := ar.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err // closed: the last archived session was deleted meanwhile
	}
	return nil
}

// replayJournal applies the journal to the hot set and the archive index.
// Lines that do not parse (a torn append) are skipped. Replaying records that
// a snapshot already contains is harmless: the last record of an id decides
// its state. It reports whether a journal file exists, and returns the
// archive lines of the sessions whose delete it had to finish in the archive
// index (they must leave the archive file too, see tidyArchive).
func (f *FileStore) replayJournal() (exists bool, unfinished []archRef, err error) {
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
		if r, ok := f.arch[id]; ok {
			delete(f.arch, id)
			unfinished = append(append(unfinished, r), f.stale[id]...)
			delete(f.stale, id)
		}
	}
	if err != nil {
		return false, nil, err
	}
	if !exists {
		return false, unfinished, nil
	}
	if !nl {
		// End the torn line, so that a later append starts a line of its own
		// (this matters only if the journal cannot be folded away now).
		if err := appendFile(f.journalPath(), []byte{'\n'}, false); err != nil {
			return false, nil, err
		}
	}
	st, err := os.Stat(f.journalPath())
	if err != nil {
		return false, nil, err
	}
	f.journalSize = st.Size()
	return true, unfinished, nil
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
		ends  []int // ends[i] is where the line of moved[i] ends in lines, newline included
		lines bytes.Buffer
		enc   = json.NewEncoder(&lines) // one line per session
	)
	for _, id := range ids {
		if s, ok := f.MemStore.Get(id); ok && enc.Encode(s) == nil {
			moved, ends = append(moved, s), append(ends, lines.Len())
		}
	}
	if len(moved) == 0 {
		return
	}
	ar, err := f.archiveForAppend()
	if err != nil {
		return // keep them in memory rather than lose them
	}
	// A session that is archived already (it was changed after its eviction)
	// is fsynced now: replay could not tell its new line from the old one,
	// and the old one is overwritten below. For all others the fsync waits
	// for the next snapshot.
	rearchived := false
	f.amu.RLock()
	for _, s := range moved {
		if _, ok := f.arch[s.ID]; ok {
			rearchived = true
			break
		}
	}
	f.amu.RUnlock()
	// The offset is the store's own count of the file, never a guess. After
	// an append that could not be ended (archTorn) the write starts with the
	// newline that ends it.
	before, data := f.archSize, lines.Bytes()
	if f.archTorn {
		data = append([]byte{'\n'}, data...)
	}
	lead := len(data) - lines.Len()
	n, err := f.ops.writeAt(ar, data, before)
	if err == nil && rearchived {
		err = f.ops.sync(ar)
	}
	if err != nil {
		if f.undoAppend(ar, before, n, len(data)) {
			// Bytes of this append are in the file, or may come back to it.
			// The index has to know them, also the part of a line that was
			// cut: a later delete must overwrite them and a later eviction
			// must replace them. Their sessions stay in the hot set, like
			// sessions changed after an eviction, so the index entries are
			// never read.
			f.indexLines(moved, ends, before+int64(lead), n-lead)
		}
		return // keep them in memory rather than lose them
	}
	f.archTorn = false
	f.archSize += int64(len(data))
	f.archiveUnsynced = !rearchived
	gone := make([]string, len(moved))
	for i, s := range moved {
		gone[i] = s.ID
	}
	replaced := f.indexLines(moved, ends, before+int64(lead), lines.Len())
	f.MemStore.Evict(gone...) // after the index has them: a reader finds them in one or the other
	// If this fails the sessions are hot again after a restart, as well as
	// archived; the hot copy wins and they are evicted once more.
	_ = f.journalLocked(journalRecord{Op: "evict", IDs: gone})
	// The new lines are on disk: the ones they replace can go. A line that
	// cannot be overwritten stays in f.stale; it loses against the newer line
	// when the archive is loaded, and Delete overwrites it.
	for _, id := range replaced {
		if f.blankLines(f.stale[id]...) == nil {
			delete(f.stale, id)
		}
	}
}

// indexLines points the index at the lines of moved, which were written to
// the archive file from offset base on (ends[i] is where the line of moved[i]
// ends, newline included, relative to base). Only the first written bytes
// count: a line that was cut there is indexed as far as it goes, the lines
// after it are not. The line an id had before moves to f.stale; the ids that
// had one are returned. Caller holds f.wmu.
func (f *FileStore) indexLines(moved []Session, ends []int, base int64, written int) (replaced []string) {
	f.amu.Lock()
	defer f.amu.Unlock()
	start := 0
	for i, s := range moved {
		if start >= written {
			break
		}
		if old, ok := f.arch[s.ID]; ok {
			f.stale[s.ID] = append(f.stale[s.ID], old)
			replaced = append(replaced, s.ID)
		}
		ref := newArchRef(s, base+int64(start), min(ends[i]-1, written)-start)
		f.arch[s.ID] = ref
		if written >= ends[i]-1 { // whole
			f.cache.put(s.ID, f.archGen, ref.off, s, true)
		}
		start = ends[i]
	}
	return replaced
}

// undoAppend cleans up after a failed append of want bytes at offset before,
// of which n were written (all of them when only the fsync failed). It keeps
// two things true: no line is ever written below the end of the file, and no
// part of a line that the store does not know is taken for a session when the
// archive is loaded. In order:
//
//  1. truncate the file back to before;
//  2. if that fails, overwrite the n bytes with spaces and fsync: they are a
//     blank line now, and the next line goes after them;
//  3. if that fails too, leave the bytes, move archSize to the real end of
//     the file and end the last line there with a newline. If the size of the
//     file cannot be read, archSize moves past all want bytes; if the newline
//     cannot be written, the next append starts with one (archTorn).
//
// It reports whether lines of the append are still in the file or may come
// back to it (step 3, or a blanking that was not fsynced, which a power
// failure can undo). The caller then indexes the complete ones.
func (f *FileStore) undoAppend(ar *os.File, before int64, n, want int) (left bool) {
	if f.ops.truncate(ar, before) == nil {
		return false
	}
	if n > 0 {
		if _, err := f.ops.writeAt(ar, bytes.Repeat([]byte{' '}, n), before); err == nil {
			f.archSize = before + int64(n)
			return f.ops.sync(ar) != nil
		}
	}
	end := before + int64(want)
	if size, err := f.ops.size(ar); err == nil {
		end = max(size, before)
	}
	if end == before {
		return false // nothing of the append reached the file
	}
	f.archSize = end
	if _, err := f.ops.writeAt(ar, []byte{'\n'}, end); err == nil {
		f.archSize++
	} else {
		f.archTorn = true
	}
	return true
}

// Delete implements Store: the session leaves the hot set and the archive
// for good. The archive line of an archived session is overwritten in place,
// which costs one small write and one fsync whatever the size of the archive.
func (f *FileStore) Delete(id string) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	_, hot := f.MemStore.Get(id)
	f.amu.RLock()
	ref, archived := f.arch[id]
	f.amu.RUnlock()
	if !hot && !archived {
		return nil
	}
	// Journal first: if the archive line is not overwritten below (error or
	// crash), opening the store finishes the delete.
	if err := f.appendJournalLocked(journalRecord{Op: "delete", IDs: []string{id}}); err != nil {
		return err
	}
	if archived {
		f.amu.Lock()
		delete(f.arch, id) // first: readers must not be sent to a line that is being overwritten
		empty := len(f.arch) == 0
		f.amu.Unlock()
		if err := f.blankLines(append([]archRef{ref}, f.stale[id]...)...); err != nil {
			// Still on disk, or possibly so: keep it in the index and the
			// delete repeatable. A hot copy stays too; dropping it would
			// bring this older archived record back as the session's current
			// state.
			f.amu.Lock()
			f.arch[id] = ref
			f.amu.Unlock()
			return err
		}
		delete(f.stale, id)
		if empty {
			f.removeArchive()
		}
	}
	f.MemStore.Evict(id)
	// Only now: a compaction started by the record above, before the session
	// left the hot set, would write it into the snapshot and cut the journal
	// after the delete record, bringing it back at the next unclean start.
	f.maybeCompactLocked()
	return nil
}

// Get implements Store, falling back to the archive for evicted sessions.
// An archived session whose line cannot be read (an I/O error, or a file
// changed behind the store's back) is reported as not found here, counted
// and logged; GetErr reports the error.
func (f *FileStore) Get(id string) (Session, bool) {
	s, ok, _ := f.GetErr(id)
	return s, ok
}

// GetErr implements CheckedStore. The error is non-nil when the store has an
// archived session with this id and could not read its line.
func (f *FileStore) GetErr(id string) (Session, bool, error) {
	if s, ok := f.MemStore.Get(id); ok {
		return s, true, nil
	}
	s, ok, err := f.getArchived(id)
	if err != nil {
		f.readErrs.Add(1)
		f.log.Error("file store: archived session could not be read", "id", id, "archive", f.archivePath(), "err", err)
	}
	return s, ok, err
}

// ReadErrors implements CheckedStore: the number of archived sessions that
// Get, GetErr and the List methods could not read, counted once per call
// that asked for them.
func (f *FileStore) ReadErrors() int64 { return f.readErrs.Load() }

// getArchived returns the archived record of id: from the cache, or from the
// one line of the archive file that the index points at. Without an index
// entry there is no session and no error.
func (f *FileStore) getArchived(id string) (Session, bool, error) {
	f.amu.RLock()
	defer f.amu.RUnlock()
	ref, ok := f.arch[id]
	if !ok {
		return Session{}, false, nil
	}
	if f.ar == nil {
		return Session{}, false, os.ErrClosed
	}
	if s, ok := f.cache.get(id, f.archGen, ref.off); ok {
		return s, true, nil
	}
	line := make([]byte, ref.n)
	if _, err := f.ar.ReadAt(line, ref.off); err != nil {
		return Session{}, false, err
	}
	s, err := decodeArchived(id, line)
	if err != nil {
		return Session{}, false, err
	}
	f.cache.put(id, f.archGen, ref.off, s, true)
	return s, true, nil
}

// List implements Store: the hot sessions plus the archived ones. A session
// that is in both (an archived session that was changed again) is listed
// once, with its hot record. The archived sessions that are not cached are
// read from the archive file, after the locks are released.
func (f *FileStore) List() []Session {
	return f.list(nil, func(Session, bool) bool { return true })
}

// ListState implements SelectiveLister. Only the archive lines of sessions
// in that state are read.
func (f *FileStore) ListState(st State) []Session {
	code := archStateCode(st)
	return f.list(
		func(r archRef) bool { return r.state == code || r.state == 0 },
		func(s Session, _ bool) bool { return s.State == st })
}

// ListPending implements SelectiveLister. Only the archive lines of sessions
// that are not finished or have a seal pending are read; a Manager never
// evicts the former, so that is normally a handful.
func (f *FileStore) ListPending() []Session {
	return f.list(
		func(r archRef) bool { return r.sealPending || !archStates[r.state].Terminal() },
		func(s Session, hot bool) bool { return hot || s.SealPending || !s.State.Terminal() })
}

// list returns the sessions that keep accepts, ordered like List. keep is
// told whether the session is in the hot set. keepRef, if not nil, is asked
// first about an archived session and must accept every index entry whose
// session keep would accept; it saves reading the lines of the others.
func (f *FileStore) list(keepRef func(archRef) bool, keep func(s Session, hot bool) bool) []Session {
	type entry struct {
		id  string
		ref archRef
	}
	f.MemStore.mu.RLock()
	f.amu.RLock()
	n := len(f.m)
	if keepRef == nil {
		n += len(f.arch)
	}
	out := make([]Session, 0, n)
	for _, s := range f.m {
		if keep(s, true) {
			out = append(out, s)
		}
	}
	var archived []entry
	if keepRef == nil {
		archived = make([]entry, 0, len(f.arch))
	}
	for id, r := range f.arch {
		if _, hot := f.m[id]; !hot && (keepRef == nil || keepRef(r)) {
			archived = append(archived, entry{id, r})
		}
	}
	ar, gen := f.ar, f.archGen
	f.amu.RUnlock()
	f.MemStore.mu.RUnlock()

	if f.testHookListed != nil {
		f.testHookListed()
	}
	// The lines are read without a lock, in file order. One that was
	// overwritten meanwhile does not parse (and when the last archived
	// session was deleted, the file is closed): then the session was deleted,
	// or it was archived again and is looked up once more.
	slices.SortFunc(archived, func(a, b entry) int { return cmp.Compare(a.ref.off, b.ref.off) })
	lr := lineReader{f: ar}
	var (
		failed   int
		firstID  string
		firstErr error
	)
	for _, e := range archived {
		s, ok := f.cache.get(e.id, gen, e.ref.off)
		hot := false
		if !ok {
			line, err := lr.read(e.ref)
			if err == nil {
				s, err = decodeArchived(e.id, line)
			}
			if err == nil {
				f.cache.put(e.id, gen, e.ref.off, s, false)
			} else if s, hot = f.MemStore.Get(e.id); !hot {
				// Look it up again, under the lock: no error and no session
				// means it was deleted meanwhile. An error means the store
				// has it and cannot read it.
				if s, ok, err = f.getArchived(e.id); !ok {
					if err != nil {
						if failed++; firstErr == nil {
							firstID, firstErr = e.id, err
						}
					}
					continue
				}
			}
		}
		if keep(s, hot) {
			out = append(out, s)
		}
	}
	if failed > 0 {
		// One line per call, not per session: a damaged archive can hold many.
		f.readErrs.Add(int64(failed))
		f.log.Error("file store: archived sessions could not be read and are missing from a list",
			"sessions", failed, "first_id", firstID, "archive", f.archivePath(), "err", firstErr)
	}
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
		if err := f.syncArchive(); err != nil {
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
	if err := f.appendJournalLocked(rec); err != nil {
		return err
	}
	f.maybeCompactLocked()
	return nil
}

// appendJournalLocked writes one record without considering compaction; for
// callers that change the hot set only after the record is on disk.
func (f *FileStore) appendJournalLocked(rec journalRecord) error {
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
	return nil
}

// maybeCompactLocked starts a background compaction when the journal has
// grown past the threshold. The hot set is copied here, so the caller must
// already have applied to it everything the journal says: a snapshot taken
// before a journalled change was applied would undo that change when the
// journal is cut. Caller holds f.wmu.
func (f *FileStore) maybeCompactLocked() {
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
		err = f.syncArchive()
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
