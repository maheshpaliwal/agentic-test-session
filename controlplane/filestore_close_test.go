package controlplane

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// storeFiles reads the three files of a store ("" for one that is missing).
func storeFiles(t *testing.T, fs *FileStore) [3]string {
	t.Helper()
	var out [3]string
	for i, p := range []string{fs.path, fs.journalPath(), fs.archivePath()} {
		b, err := os.ReadFile(p)
		if err != nil && !os.IsNotExist(err) {
			panic(err)
		}
		out[i] = string(b)
	}
	return out
}

// Close releases the archive file. A closed store changes nothing any more
// and says so; what it has in memory can still be read.
func TestFileStoreClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path)
	if err != nil {
		panic(err)
	}
	hot, archived := archiveSession(0, "hot", StateActive), archiveSession(1, "archived", StateSealed)
	cachedToo := archiveSession(2, "archived and cached", StateAborted)
	mustPut(t, fs, hot, archived, cachedToo)
	fs.Evict(archived.ID, cachedToo.ID)
	wantStore(t, "open", fs, []Session{hot, archived, cachedToo})
	handle := fs.ar
	if handle == nil {
		panic("test setup: no archive file is open")
	}

	if err := fs.Close(); err != nil {
		panic(fmt.Sprintf("close: %v", err))
	}
	if _, err := handle.Stat(); !errors.Is(err, os.ErrClosed) {
		panic(fmt.Sprintf("the archive file is still open after Close (stat: %v)", err))
	}
	if err := fs.Close(); err != nil {
		panic(fmt.Sprintf("second close: %v", err))
	}
	files := storeFiles(t, fs)

	// Changes are refused and leave memory and disk alone.
	changed := hot
	changed.Name = "changed after close"
	if err := fs.Put(changed); !errors.Is(err, ErrStoreClosed) {
		panic(fmt.Sprintf("put after close: %v", err))
	}
	if err := fs.Put(archiveSession(3, "new", StateActive)); !errors.Is(err, ErrStoreClosed) {
		panic(fmt.Sprintf("put of a new session after close: %v", err))
	}
	if err := fs.Delete(hot.ID); !errors.Is(err, ErrStoreClosed) {
		panic(fmt.Sprintf("delete after close: %v", err))
	}
	if err := fs.Delete(archived.ID); !errors.Is(err, ErrStoreClosed) {
		panic(fmt.Sprintf("delete of an archived session after close: %v", err))
	}
	if err := fs.Compact(); !errors.Is(err, ErrStoreClosed) {
		panic(fmt.Sprintf("compact after close: %v", err))
	}
	fs.Evict(hot.ID)
	if got, ok := fs.Get(hot.ID); !ok || got.Name != hot.Name {
		panic(fmt.Sprintf("hot session after close: %+v %v", got, ok))
	}
	if _, ok := fs.MemStore.Get(hot.ID); !ok {
		panic("evict after close took the session out of the hot set")
	}
	if got := storeFiles(t, fs); got != files {
		panic(fmt.Sprintf("files changed after close:\n%q\nwant\n%q", got, files))
	}
	if _, err := os.Stat(fs.archivePath()); err != nil {
		panic(fmt.Sprintf("archive file: %v", err))
	}

	// Archived sessions are not served, and that is an error, not "unknown".
	for _, s := range []Session{archived, cachedToo} {
		if _, ok, err := fs.GetErr(s.ID); ok || !errors.Is(err, ErrStoreClosed) {
			panic(fmt.Sprintf("GetErr %s after close: %v %v", s.ID, ok, err))
		}
		if _, ok := fs.Get(s.ID); ok {
			panic(fmt.Sprintf("Get %s after close found it", s.ID))
		}
	}
	if _, ok, err := fs.GetErr("ts_nope"); ok || err != nil {
		panic(fmt.Sprintf("GetErr of an unknown id after close: %v %v", ok, err))
	}
	wantSameSessions(t, "list after close", fs.List(), []Session{hot})
	wantSameSessions(t, "list pending after close", fs.ListPending(), []Session{hot})
	if n := fs.ReadErrors(); n != 6 { // two by GetErr, two by Get, two by List
		panic(fmt.Sprintf("read errors after close: %d, want 6", n))
	}

	// Everything is on disk for the next store.
	wantStore(t, "reopened", openStore(t, path), []Session{hot, archived, cachedToo})

	// A store without an archive closes too.
	empty, err := NewFileStore(filepath.Join(t.TempDir(), "s.json"))
	if err != nil {
		panic(err)
	}
	if err := empty.Close(); err != nil {
		panic(err)
	}
	if err := empty.Close(); err != nil {
		panic(err)
	}
}

// Close waits for a background compaction: when it returns, nothing of the
// store is running or half written.
func TestFileStoreCloseWaitsForCompaction(t *testing.T) {
	for round := range 20 {
		dir := t.TempDir()
		path := filepath.Join(dir, "s.json")
		fs := openStore(t, path)
		var want []Session
		for i := range 40 {
			want = append(want, archiveSession(i, fmt.Sprintf("s%d", i), StateActive))
		}
		mustPut(t, fs, want[:39]...)
		lowerCompactThreshold(fs, 1)
		mustPut(t, fs, want[39]) // starts a compaction
		fs.wmu.Lock()
		started := fs.compacting != nil
		fs.wmu.Unlock()
		if err := fs.Close(); err != nil {
			panic(err)
		}
		fs.wmu.Lock()
		running := fs.compacting
		fs.wmu.Unlock()
		if running != nil {
			panic("a compaction is running after Close")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			panic(err)
		}
		for _, e := range entries {
			if n := e.Name(); n != "s.json" && n != "s.json.journal" && n != "s.json.archive" {
				panic(fmt.Sprintf("round %d: %s left behind by Close (compaction started: %v)", round, n, started))
			}
		}
		wantStore(t, "reopened", openStore(t, path), want)
	}
}

// Close while the store is in use (run with -race): every call returns, and
// a change either happened and is on disk, or was refused.
func TestFileStoreCloseWhileInUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs := openStore(t, path)
	lowerCompactThreshold(fs, 2<<10)
	fs.cache.max = 4
	const workers, perWorker = 4, 60
	stored := make([][]Session, workers)
	var wg sync.WaitGroup
	var puts atomic.Int32
	start := make(chan struct{})
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range perWorker {
				s := archiveSession(w*perWorker+i, fmt.Sprintf("w%d", w), StateSealed)
				err := fs.Put(s)
				if errors.Is(err, ErrStoreClosed) {
					return
				}
				if err != nil {
					t.Errorf("put: %v", err)
					return
				}
				stored[w] = append(stored[w], s)
				puts.Add(1)
				if i%2 == 0 {
					fs.Evict(s.ID)
				}
				fs.Get(s.ID)
				if i%7 == 0 {
					fs.List()
					fs.ListPending()
				}
			}
		}()
	}
	close(start)
	for puts.Load() < 40 { // in the middle of the work
		runtime.Gosched()
	}
	if err := fs.Close(); err != nil {
		panic(err)
	}
	wg.Wait()
	if err := fs.Close(); err != nil {
		panic(err)
	}
	var want []Session
	for _, ss := range stored {
		want = append(want, ss...)
	}
	wantStore(t, "reopened", uncached(openStore(t, path)), want)
}
