package controlplane

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func delSess(id string, v int, st State) Session {
	return Session{ID: id, Name: fmt.Sprintf("%s#%d", id, v), State: st, Mode: ModeHeader,
		CreatedAt: time.Unix(1700000000, 0).UTC().Add(time.Duration(len(id)) * time.Second), DroppedEvents: int64(v)}
}

func delOpen(t testing.TB, path string) *FileStore {
	t.Helper()
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return fs
}

// A Delete whose journal record starts a background compaction: the hot set
// is copied before the session leaves it.
func TestDeleteThatStartsACompactionStaysDeleted(t *testing.T) {
	// "archived only" is the case the archive index added: the delete changes
	// the index and the archive file, not the hot set.
	for _, c := range []struct {
		name                      string
		archivedToo, archivedOnly bool
	}{{"archivedToo=false", false, false}, {"archivedToo=true", true, false}, {"archived only", false, true}} {
		archivedToo, archivedOnly := c.archivedToo, c.archivedOnly
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.json")
			fs := delOpen(t, path)
			_ = fs.Put(delSess("keep", 1, StateSealed))
			_ = fs.Put(delSess("x", 1, StateSealed))
			if archivedToo {
				fs.Evict("x")
				_ = fs.Put(delSess("x", 2, StateSealed)) // hot again, archived too
			}
			if archivedOnly {
				fs.Evict("x")
			}
			if err := fs.Compact(); err != nil {
				t.Fatal(err)
			}
			fs.wmu.Lock()
			fs.compactMin = 1
			fs.compactAt = fs.journalSize + 1 // the next record starts a compaction
			fs.wmu.Unlock()
			if err := fs.Delete("x"); err != nil {
				t.Fatal(err)
			}
			waitCompaction(fs)
			if _, ok := fs.Get("x"); ok {
				t.Fatal("x still there in the running store")
			}
			re := delOpen(t, path)
			if s, ok := re.Get("x"); ok {
				snap, _ := os.ReadFile(path)
				j, _ := os.ReadFile(path + ".journal")
				t.Fatalf("deleted session is back after reopen: %+v\nsnapshot has x: %v, journal: %q", s.Name, bytes.Contains(snap, []byte(`"x"`)), j)
			}
			// Neither store may be left with a line of it in the archive.
			if raw, err := os.ReadFile(path + ".archive"); err == nil && bytes.Contains(raw, []byte(`"id":"x"`)) {
				t.Fatalf("a line of the deleted session is left in the archive:\n%s", raw)
			}
			if _, ok := re.Get("keep"); !ok {
				t.Fatal("the other session is gone")
			}
		})
	}
}
