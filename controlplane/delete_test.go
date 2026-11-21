package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeleteSession(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	ctx := context.Background()
	live, _ := m.Start(StartRequest{Name: "live"})
	done, _ := m.Start(StartRequest{Name: "done"})
	if _, err := m.Abort(done.ID); err != nil {
		panic(err)
	}

	if err := m.Delete(ctx, "ts_nope"); !errors.Is(err, ErrNotFound) {
		panic(fmt.Sprintf("unknown id: %v", err))
	}
	if err := m.Delete(ctx, live.ID); !errors.Is(err, ErrBadState) {
		panic(fmt.Sprintf("running session must not be deletable: %v", err))
	}

	// A collector failure keeps the session so the delete can be retried.
	f.deleteFails.Store(true)
	if err := m.Delete(ctx, done.ID); !errors.Is(err, ErrCollector) {
		panic(fmt.Sprintf("collector failure: %v", err))
	}
	if _, ok := m.Get(done.ID); !ok {
		panic("session forgotten although the collector still holds its events")
	}
	f.deleteFails.Store(false)

	if err := m.Delete(ctx, done.ID); err != nil {
		panic(err)
	}
	if _, ok := m.Get(done.ID); ok {
		panic("still readable after delete")
	}
	if l := m.List(""); len(l) != 1 || l[0].ID != live.ID {
		panic(fmt.Sprintf("list after delete: %+v", l))
	}
	f.mu.Lock()
	got := append([]string(nil), f.deleted...)
	f.mu.Unlock()
	if len(got) != 1 || got[0] != done.ID {
		panic(fmt.Sprintf("collector deletes: %v", got))
	}
	// The retention sweep later meets the stale queue entry; it must cope.
	m.Sweep(time.Now().Add(48 * time.Hour))
	if s, _ := m.Get(live.ID); s.ID != live.ID {
		panic("live session lost")
	}
}

func TestDeleteOverHTTP(t *testing.T) {
	f := newFakeCollector(t)
	m := newMgr(t, f, nil)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	live, _ := m.Start(StartRequest{})
	done, _ := m.Start(StartRequest{})
	m.Abort(done.ID)

	del := func(id string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/sessions/"+id, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}
	if code, _ := del("ts_nope"); code != http.StatusNotFound {
		panic(fmt.Sprintf("unknown: %d", code))
	}
	if code, body := del(live.ID); code != http.StatusConflict || !strings.Contains(body["error"].(string), "ACTIVE") {
		panic(fmt.Sprintf("running: %d %v", code, body))
	}
	f.deleteFails.Store(true)
	if code, _ := del(done.ID); code != http.StatusBadGateway {
		panic(fmt.Sprintf("collector down: %d", code))
	}
	f.deleteFails.Store(false)
	if code, body := del(done.ID); code != http.StatusOK || body["deleted"] != true || body["id"] != done.ID {
		panic(fmt.Sprintf("delete: %d %v", code, body))
	}
	if code, _ := del(done.ID); code != http.StatusNotFound {
		panic(fmt.Sprintf("second delete: %d", code))
	}
	// A browser on another origin must not be able to delete (CSRF).
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/sessions/"+live.ID, nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		panic(fmt.Sprintf("cross-origin delete: %d", resp.StatusCode))
	}
}

// FileStore must drop a deleted session from memory, the JSON file and the
// archive, including every line of a renamed archived session.
func TestFileStoreDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	fs, err := NewFileStore(path)
	closeLater(t, fs)
	if err != nil {
		panic(err)
	}
	m := newMgr(t, newFakeCollector(t), func(c *Config) { c.Store = fs; c.Retention = time.Hour })
	ctx := context.Background()
	hot, _ := m.Start(StartRequest{Name: "hot"})
	cold, _ := m.Start(StartRequest{Name: "cold"})
	other, _ := m.Start(StartRequest{Name: "other"})
	for _, id := range []string{hot.ID, cold.ID, other.ID} {
		m.Abort(id)
	}
	m.Sweep(time.Now().Add(2 * time.Hour)) // archives all three
	if _, err := m.Rename(cold.ID, "cold renamed"); err != nil {
		panic(err)
	}
	m.Sweep(time.Now().Add(4 * time.Hour)) // re-archives the renamed one: two lines now
	if _, err := m.Rename(hot.ID, "hot again"); err != nil {
		panic(err) // back in the hot set
	}

	for _, id := range []string{hot.ID, cold.ID} {
		if err := m.Delete(ctx, id); err != nil {
			panic(fmt.Sprintf("delete %s: %v", id, err))
		}
	}
	reopened, err := NewFileStore(path)
	closeLater(t, reopened)
	if err != nil {
		panic(err)
	}
	for _, st := range []*FileStore{fs, reopened} {
		for _, id := range []string{hot.ID, cold.ID} {
			if s, ok := st.Get(id); ok {
				panic(fmt.Sprintf("deleted session still readable: %+v", s))
			}
		}
		if s, ok := st.Get(other.ID); !ok || s.Name != "other" {
			panic(fmt.Sprintf("unrelated archived session lost: %+v %v", s, ok))
		}
		if l := st.List(); len(l) != 1 {
			panic(fmt.Sprintf("list: %+v", l))
		}
	}
	// Deleting the last archived session removes the archive file.
	if err := m.Delete(ctx, other.ID); err != nil {
		panic(err)
	}
	if _, err := os.Stat(fs.archivePath()); !os.IsNotExist(err) {
		panic(fmt.Sprintf("empty archive left behind: %v", err))
	}
}
