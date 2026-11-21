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
// holds its id and a fixed-size index entry (archRef: where its line is, when
// it was created, its state, its seal marker and a hash of its name), about
// 100 bytes, and reads the line from the file when the session is asked for.
// Up to archCacheMax decoded archived sessions are cached, so Get and List do
// not touch the file while the archive is that small. List returns every
// session, so its result grows with the archive whatever the store holds;
// beyond the cache it reads the archive once per call, without holding a
// lock. ListState and ListPending read only the lines of the sessions they
// return, and so does ListPage with a Limit: it picks the page from the hot
// set and the index (a pass over the index, no file access) and then reads
// at most Limit lines, or, for a query by name, the lines of the sessions
// whose name has the same hash until the page is full.
//
// Read errors: Get and the List methods have no error result. An archived
// session whose line cannot be read (an I/O error, or a file that was cut or
// changed behind the store's back) is missing from what they return. Every
// such session is counted (ReadErrors) and logged, and GetErr tells it from a
// session the store does not have.
//
// A FileStore is safe for concurrent use. One process may use a path at a
// time. It keeps the archive file open; Close releases it.
type FileStore struct {
	*MemStore // the hot set
	path      string
	log       *slog.Logger
	readErrs  atomic.Int64 // archived sessions that could not be read
	lineReads atomic.Int64 // archive lines decoded from the file (not served by the cache)

	// wmu serialises the writers and all file changes. It is held across the
	// in-memory change and its journal append, so the journal order is the
	// order in which the hot set changed.
	wmu         sync.Mutex
	journalSize int64         // bytes in the journal file
	snapSize    int64         // bytes in the snapshot file
	compactAt   int64         // journal size that starts the next compaction
	compactMin  int64         // journalCompactBytes; tests lower it
	compacting  chan struct{} // non-nil while a background compaction runs; closed when it ends
	closed      bool          // set by Close: no more changes
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
	// keepArchiveAsIs is set at open when the archive holds damaged lines
	// that could not be copied aside: the file is then not rewritten.
	keepArchiveAsIs bool
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
// It holds what selects the sessions of a list or a page, so that only the
// lines of the selected ones are read. Its size is what an archived session
// costs in memory besides its id: keep it at 24 bytes.
type archRef struct {
	off         int64  // offset of the session's line in the archive file
	created     int64  // the session's place in the listing order: createdNanos(CreatedAt)
	n           uint32 // length of the line, without the newline
	nameHash    uint16 // nameHash16 of the session's Name
	state       uint8  // the session's State, as an index into archStates
	sealPending bool   // the session's SealPending
}

// nameHash16 is the hash of a session name kept in the index. Sessions with
// another hash do not have the name; of the others about one in 65,536 has
// another name, so their lines are read to know.
func nameHash16(name string) uint16 {
	h := uint32(2166136261) // FNV-1a
	for i := 0; i < len(name); i++ {
		h = (h ^ uint32(name[i])) * 16777619
	}
	return uint16(h ^ h>>16)
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
	return archRef{off: off, created: createdNanos(s.CreatedAt), n: uint32(n), nameHash: nameHash16(s.Name),
		state: archStateCode(s.State), sealPending: s.SealPending}
}

// archCacheMax is the number of decoded archived sessions kept in memory
// (some 12 MB of sessions with a name, a bundle and a binding).
const archCacheMax = 16384

// archCache holds up to max decoded archived sessions by id, each with the
// place of the line it was decoded from (FileStore.archGen and the offset).
// An entry counts only while the index points at that place. No line is ever
// written to a place that held another one, so an entry that is out of date
// is never used. It still takes a place in the cache, so the store removes
// the entry of a session whose archived record it deletes or replaces (drop,
// reset): a full cache caches nothing new for List.
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

// drop removes the entry of id, if there is one.
func (c *archCache) drop(id string) {
	c.mu.Lock()
	delete(c.m, id)
	c.mu.Unlock()
}

// reset removes every entry.
func (c *archCache) reset() {
	c.mu.Lock()
	clear(c.m)
	c.mu.Unlock()
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
	type badLine struct {
		off  int64
		data []byte
	}
	var bad []badLine
	size, nl, err := scanLines(fh, func(off int64, line []byte) {
		var s Session
		if json.Unmarshal(line, &s) != nil || s.ID == "" {
			if len(bytes.TrimSpace(line)) > 0 {
				bad = append(bad, badLine{off, bytes.Clone(line)})
			}
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
	if n := len(bad); n > 0 && !nl && bad[n-1].off+int64(len(bad[n-1].data)) == size {
		bad = bad[:n-1] // a torn last line is an interrupted append, not damage
	}
	if len(bad) > 0 {
		// A damaged line is a session this store can no longer serve. Tidying
		// the archive would erase it without a trace: keep it aside and say so.
		var kept bytes.Buffer
		for _, b := range bad {
			kept.Write(b.data)
			kept.WriteByte('\n')
		}
		side := f.archivePath() + ".corrupt"
		if werr := appendFile(side, kept.Bytes(), true); werr != nil {
			f.keepArchiveAsIs = true // do not rewrite the file the lines are in
			f.log.Error("file store: the archive has lines that cannot be read and they could not be saved aside; the archive is left as it is",
				"lines", len(bad), "archive", f.archivePath(), "err", werr)
		} else {
			f.log.Error("file store: the archive has lines that cannot be read; their sessions are gone from the control plane (the collector may still hold their events) and the lines were saved aside",
				"lines", len(bad), "archive", f.archivePath(), "saved_to", side)
		}
		f.readErrs.Add(int64(len(bad)))
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
	err := errors.New("the archive holds damaged lines that could not be saved aside")
	if !f.keepArchiveAsIs {
		if err = f.rewriteArchive(); err == nil {
			return nil
		}
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
		if line, err = lr.read(live[i].ref, lineReaderChunk); err != nil {
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
// offset order share reads: a line that is not in the buffer is read together
// with the span bytes from its start on (at least the line itself). A
// returned line is valid until the next call.
type lineReader struct {
	f   *os.File
	buf []byte
	off int64 // file offset of buf[0]
}

const (
	// lineReaderChunk is the most one read asks for, unless a line is longer.
	lineReaderChunk = 256 << 10
	// lineReaderGap is the most bytes between two wanted lines that are read
	// along rather than skipped with a read of their own.
	lineReaderGap = 16 << 10
)

// readSpan is the span to read at the first of entries (sorted by offset) so
// that the read also holds the wanted lines that follow it closely.
func readSpan(entries []archEntry) int {
	start := entries[0].ref.off
	end := start + int64(entries[0].ref.n)
	for _, e := range entries[1:] {
		eEnd := e.ref.off + int64(e.ref.n)
		if e.ref.off-end > lineReaderGap || eEnd-start > lineReaderChunk {
			break
		}
		end = max(end, eEnd)
	}
	return int(end - start)
}

func (r *lineReader) read(ref archRef, span int) ([]byte, error) {
	if r.f == nil {
		return nil, os.ErrClosed
	}
	end := ref.off + int64(ref.n)
	if ref.off < r.off || end > r.off+int64(len(r.buf)) {
		want := max(int(ref.n), span)
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
	f.cache.reset()
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
	if f.closed {
		return ErrStoreClosed
	}
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
	if f.closed {
		return // they stay in the hot set
	}
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
			f.cache.put(s.ID, f.archGen, ref.off, s, true) // in place of the record it replaces
		} else {
			f.cache.drop(s.ID)
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
			if f.ops.sync(ar) != nil {
				// The blanks may never reach the disk. Start the next append
				// on a line of its own, or the torn bytes and the next
				// session's line would load as one unparseable line.
				f.archTorn = true
				return true
			}
			return false
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
//
// Errors. Delete works in this order: journal record, archive line, hot set.
//
//   - ErrStoreClosed, or the journal record cannot be written: nothing has
//     changed and nothing will.
//   - The record is written and the archive line cannot be overwritten: the
//     error is returned and the running store still has the session, archived
//     and (if it was) hot, so the call can be repeated. But the record stays
//     in the journal, and a failed Delete can therefore still take effect:
//     if the store is opened again before that record is folded away, the
//     replay deletes the session (and opening fails if the line cannot be
//     removed then either). The record is folded away, and the session then
//     stays, by the next compaction: Compact, or the background one once the
//     journal passes its threshold. A Put of the session after the failed
//     Delete also keeps it: the replay sees the later record.
//
// So after an error the session is either still there or, after a restart,
// deleted as asked; it never comes back in an older version. A caller that
// must know repeats the Delete until it returns nil.
func (f *FileStore) Delete(id string) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	if f.closed {
		return ErrStoreClosed
	}
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
		f.cache.drop(id)   // under amu: list caches a line only while the index points at it
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
		return Session{}, false, ErrStoreClosed
	}
	if s, ok := f.cache.get(id, f.archGen, ref.off); ok {
		return s, true, nil
	}
	line := make([]byte, ref.n)
	if _, err := f.ar.ReadAt(line, ref.off); err != nil {
		return Session{}, false, err
	}
	f.lineReads.Add(1)
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
	out, _ := f.list(nil, func(Session, bool) bool { return true })
	return out
}

// ListState implements SelectiveLister. Only the archive lines of sessions
// in that state are read.
func (f *FileStore) ListState(st State) []Session {
	code := archStateCode(st)
	out, _ := f.list(
		func(r archRef) bool { return r.state == code || r.state == 0 },
		func(s Session, _ bool) bool { return s.State == st })
	return out
}

// ListPending implements SelectiveLister. Only the archive lines of sessions
// that are not finished or have a seal pending are read; a Manager never
// evicts the former, so that is normally a handful.
func (f *FileStore) ListPending() []Session {
	out, _ := f.list(
		func(r archRef) bool { return r.sealPending || !archStates[r.state].Terminal() },
		func(s Session, hot bool) bool { return hot || s.SealPending || !s.State.Terminal() })
	return out
}

// archEntry is an archived session as the index has it.
type archEntry struct {
	id  string
	ref archRef
}

// list returns the sessions that keep accepts, ordered like List, and the
// number of archived sessions that could not be read (they are missing).
// keep is told whether the session is in the hot set. keepRef, if not nil, is
// asked first about an archived session and must accept every index entry
// whose session keep would accept; it saves reading the lines of the others.
func (f *FileStore) list(keepRef func(archRef) bool, keep func(s Session, hot bool) bool) ([]Session, int) {
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
	var archived []archEntry
	if keepRef == nil {
		archived = make([]archEntry, 0, len(f.arch))
	}
	for id, r := range f.arch {
		if _, hot := f.m[id]; !hot && (keepRef == nil || keepRef(r)) {
			archived = append(archived, archEntry{id, r})
		}
	}
	ar, gen := f.ar, f.archGen
	f.amu.RUnlock()
	f.MemStore.mu.RUnlock()

	if f.testHookListed != nil {
		f.testHookListed()
	}
	failed := f.readArchived(archived, ar, gen, false, func(_ archEntry, s Session, hot bool) {
		if keep(s, hot) {
			out = append(out, s)
		}
	})
	sortSessions(out)
	return out, failed
}

// readArchived reads the sessions of entries, which were taken from the index
// when the archive file was ar and its generation gen, and calls fn for each
// one it could read, in file order (entries is sorted by offset). hot is set
// when the session was changed meanwhile and s is its hot record. It returns
// the number of sessions the store has and could not read, which it has
// counted and logged. With evict a session that is read takes the place of
// another one in a full cache.
//
// The lines are read without a lock. One that was overwritten meanwhile does
// not parse (and when the last archived session was deleted, the file is
// closed): then the session was deleted, or it was archived again and is
// looked up once more.
func (f *FileStore) readArchived(entries []archEntry, ar *os.File, gen uint64, evict bool, fn func(e archEntry, s Session, hot bool)) (failed int) {
	slices.SortFunc(entries, func(a, b archEntry) int { return cmp.Compare(a.ref.off, b.ref.off) })
	lr := lineReader{f: ar}
	var (
		firstID  string
		firstErr error
	)
	for i, e := range entries {
		s, ok := f.cache.get(e.id, gen, e.ref.off)
		hot := false
		if !ok {
			line, err := lr.read(e.ref, readSpan(entries[i:]))
			if err == nil {
				f.lineReads.Add(1)
				s, err = decodeArchived(e.id, line)
			}
			if err == nil {
				// Only while the index still points at this line: Delete and
				// Evict have removed the entry of a line they replaced, and it
				// must not come back.
				f.amu.RLock()
				if r, ok := f.arch[e.id]; ok && r.off == e.ref.off && f.archGen == gen && f.ar != nil {
					f.cache.put(e.id, gen, e.ref.off, s, evict)
				}
				f.amu.RUnlock()
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
		fn(e, s, hot)
	}
	if failed > 0 {
		// One line per call, not per session: a damaged archive can hold many.
		f.readErrs.Add(int64(failed))
		f.log.Error("file store: archived sessions could not be read and are missing from a list",
			"sessions", failed, "first_id", firstID, "archive", f.archivePath(), "err", firstErr)
	}
	return failed
}

// ListPage implements PageLister. Without a Limit it is List with the
// filters of q, reading only the lines of the sessions whose index entry
// passes them. With a Limit it selects the page from the hot set and the
// index and reads the lines of the selected sessions only:
//
//   - by state, or unfiltered: the index says exactly which sessions match,
//     so at most Limit lines are read and Total is known;
//   - by name: the index holds a 16-bit hash of the name. The sessions with
//     that hash are read from the newest on until the page is full, so the
//     lines read are those of the page plus about one in 65,536 of the
//     archived sessions before it, and Total is not known (-1).
//
// The pass over the index costs a few nanoseconds per archived session;
// nothing else grows with the archive.
func (f *FileStore) ListPage(q PageQuery) (Page, error) {
	cur, hasCur, err := q.cursor()
	if err != nil {
		return Page{}, err
	}
	code, hash := archStateCode(q.State), nameHash16(q.Name)
	// An entry names its state unless that is none of archStates (code 0).
	keepRef := func(r archRef) bool {
		return (q.State == "" || r.state == code) && (q.Name == "" || r.nameHash == hash)
	}
	if q.Limit == 0 {
		if q.State == "" && q.Name == "" {
			keepRef = nil
		}
		out, failed := f.list(keepRef, func(s Session, _ bool) bool { return q.matches(s) })
		return Page{Sessions: out, Total: len(out), Unreadable: failed}, nil
	}
	// exact: an index entry that passes keepRef is a session that matches.
	// Not so for a name (a hash) and for a state the index has no code for.
	exact := q.Name == "" && (q.State == "" || code != 0)
	var (
		top   = newest{n: q.Limit + 1}
		cands []pageCand
		hot   = map[string]Session{}
		total = 0
	)
	consider := func(c pageCand) {
		total++
		switch {
		case hasCur && !c.older(cur):
		case exact:
			top.add(c)
		default:
			cands = append(cands, c)
		}
	}
	f.MemStore.mu.RLock()
	f.amu.RLock()
	for _, s := range f.m {
		if q.matches(s) {
			consider(pageCand{pageKey: keyOf(s), hot: true})
			if !exact {
				hot[s.ID] = s
			}
		}
	}
	for id, r := range f.arch {
		if _, isHot := f.m[id]; !isHot && keepRef(r) {
			consider(pageCand{pageKey: pageKey{r.created, id}, ref: r})
		}
	}
	if exact {
		cands = top.sorted()
		for _, c := range cands {
			if c.hot {
				hot[c.id] = f.m[c.id]
			}
		}
	}
	ar, gen := f.ar, f.archGen
	f.amu.RUnlock()
	f.MemStore.mu.RUnlock()

	if f.testHookListed != nil {
		f.testHookListed()
	}
	p := Page{Total: total, Sessions: make([]Session, 0, min(len(cands), q.Limit))}
	if exact {
		// The candidates are the page. The cursor is the place of its last
		// one, also when that session turns out to be unreadable or deleted.
		if len(cands) > q.Limit {
			cands = cands[:q.Limit]
			p.Next = encodeCursor(cands[q.Limit-1].pageKey)
		}
		p.Unreadable = f.appendPage(&p.Sessions, cands, hot, ar, gen, q)
		return p, nil
	}
	// The candidates are verified from the newest on, a batch at a time, until
	// one more than the page is found: that one says there is a next page.
	p.Total = -1
	sortNewestFirst(cands)
	for len(cands) > 0 && len(p.Sessions) <= q.Limit {
		n := min(len(cands), max(q.Limit+1-len(p.Sessions), 32))
		p.Unreadable += f.appendPage(&p.Sessions, cands[:n], hot, ar, gen, q)
		cands = cands[n:]
	}
	if len(p.Sessions) > q.Limit {
		p.Sessions = p.Sessions[:q.Limit]
		p.Next = encodeCursor(keyOf(p.Sessions[q.Limit-1]))
	}
	return p, nil
}

// appendPage appends to out the sessions of cands that match q, in the order
// of cands: the hot ones from hot, the archived ones read from the archive
// (see readArchived). It returns the number that could not be read.
func (f *FileStore) appendPage(out *[]Session, cands []pageCand, hot map[string]Session, ar *os.File, gen uint64, q PageQuery) (failed int) {
	var entries []archEntry
	for _, c := range cands {
		if !c.hot {
			entries = append(entries, archEntry{c.id, c.ref})
		}
	}
	read := make(map[string]Session, len(entries))
	failed = f.readArchived(entries, ar, gen, true, func(e archEntry, s Session, _ bool) { read[e.id] = s })
	for _, c := range cands {
		s, ok := hot[c.id]
		if !c.hot {
			s, ok = read[c.id]
		}
		if ok && q.matches(s) {
			*out = append(*out, s)
		}
	}
	return failed
}

// Compact folds the journal into a new snapshot and removes it, after which
// the snapshot alone holds the hot set (as files of earlier versions did).
// The store stays usable. It is called when the store is opened; call it on
// shutdown to leave a single up-to-date file behind.
func (f *FileStore) Compact() error {
	f.lockIdle()
	defer f.wmu.Unlock()
	if f.closed {
		return ErrStoreClosed
	}
	return f.compactLocked()
}

// lockIdle takes f.wmu once no background compaction is running.
func (f *FileStore) lockIdle() {
	for {
		f.wmu.Lock()
		running := f.compacting
		if running == nil {
			return
		}
		f.wmu.Unlock()
		<-running
	}
}

// ErrStoreClosed is returned by a FileStore that was closed.
var ErrStoreClosed = errors.New("file store is closed")

// Close waits for a background compaction to end and closes the archive
// file. It does not fold the journal into the snapshot: call Compact first
// for that. Closing a closed store does nothing and returns nil.
//
// After Close the store changes nothing, in memory or on disk: Put, Delete
// and Compact return ErrStoreClosed and Evict does nothing. The sessions of
// the hot set can still be read. Archived sessions cannot: GetErr returns
// ErrStoreClosed for them, Get reports them as not found and the List
// methods leave them out, each counted as a read error.
func (f *FileStore) Close() error {
	f.lockIdle()
	defer f.wmu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.amu.Lock()
	ar := f.ar
	f.ar = nil
	f.cache.reset() // no archived session is served from now on
	f.amu.Unlock()
	if ar == nil {
		return nil
	}
	return ar.Close()
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
