package controlplane

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// FileStore wraps a MemStore and rewrites a JSON file on every Put.
type FileStore struct {
	*MemStore
	path string
	wmu  sync.Mutex
}

// NewFileStore loads path if it exists.
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{MemStore: NewMemStore(), path: path}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fs, nil
	case err != nil:
		return nil, err
	}
	var list []Session
	if len(b) > 0 {
		if err := json.Unmarshal(b, &list); err != nil {
			return nil, err
		}
	}
	for _, s := range list {
		_ = fs.MemStore.Put(s)
	}
	return fs, nil
}

func (f *FileStore) Put(s Session) error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	_ = f.MemStore.Put(s)
	return f.writeLocked()
}

func (f *FileStore) archivePath() string { return f.path + ".archive" }

// readArchive streams the archive file (JSON lines), calling fn for each entry.
func (f *FileStore) readArchive(fn func(Session) bool) {
	fh, err := os.Open(f.archivePath())
	if err != nil {
		return
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 64*1024), 4<<20)
	for sc.Scan() {
		var s Session
		if json.Unmarshal(sc.Bytes(), &s) == nil && s.ID != "" && !fn(s) {
			return
		}
	}
}

// Evict moves a session out of memory into an append-only archive file; it
// stays readable through Get and List (served from disk).
func (f *FileStore) Evict(id string) {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	s, ok := f.MemStore.Get(id)
	if !ok {
		return
	}
	b, _ := json.Marshal(s)
	fh, err := os.OpenFile(f.archivePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return // keep it in memory rather than lose it
	}
	_, werr := fh.Write(append(b, '\n'))
	cerr := fh.Close()
	if werr != nil || cerr != nil {
		return
	}
	f.MemStore.Evict(id)
	_ = f.writeLocked()
}

func (f *FileStore) Get(id string) (Session, bool) {
	if s, ok := f.MemStore.Get(id); ok {
		return s, true
	}
	var out Session
	found := false
	f.readArchive(func(s Session) bool {
		if s.ID == id {
			out, found = s, true
		}
		return true
	})
	return out, found
}

func (f *FileStore) List() []Session {
	out := f.MemStore.List()
	inMem := make(map[string]struct{}, len(out))
	for _, s := range out {
		inMem[s.ID] = struct{}{}
	}
	// The archive is append-only and a renamed session may appear more than
	// once: the last line wins.
	arch := map[string]Session{}
	f.readArchive(func(s Session) bool {
		if _, dup := inMem[s.ID]; !dup {
			arch[s.ID] = s
		}
		return true
	})
	for _, s := range arch {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (f *FileStore) writeLocked() error {
	b, err := json.MarshalIndent(f.MemStore.List(), "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), ".sessions-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), f.path)
}
