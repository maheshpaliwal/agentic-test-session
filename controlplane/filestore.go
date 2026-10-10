package controlplane

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
