package controlplane

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// writeBenchArchive writes an archive of n sessions (benchSessions shape) for
// the store at path and returns their ids.
func writeBenchArchive(t testing.TB, path string, n int) []string {
	t.Helper()
	fh, err := os.Create(path + ".archive")
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriterSize(fh, 1<<20)
	enc := json.NewEncoder(w)
	ids := make([]string, 0, n)
	const chunk = 10000
	for done := 0; done < n; done += chunk {
		ss := benchSessions(min(done+chunk, n))[done:]
		for _, s := range ss {
			if err := enc.Encode(s); err != nil {
				panic(err)
			}
			ids = append(ids, s.ID)
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
	if err := fh.Close(); err != nil {
		panic(err)
	}
	return ids
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// TestFileStoreArchiveScale measures what an archive of n sessions costs: the
// time to open the store, the heap the open store keeps in use, and the
// latency of Get, List and Delete. It runs with 20,000 sessions by default
// and with the sizes in TSESSION_ARCHIVE_SCALE when set, for example
//
//	TSESSION_ARCHIVE_SCALE=100000,1000000 go test -run ArchiveScale -v ./controlplane/
//
// The numbers are logged. The one assertion is on the heap per archived
// session, which is what keeps the control plane's memory from growing with
// full sessions again.
func TestFileStoreArchiveScale(t *testing.T) {
	sizes := []int{20000}
	if env := os.Getenv("TSESSION_ARCHIVE_SCALE"); env != "" {
		sizes = sizes[:0]
		for _, f := range strings.Split(env, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || n <= 0 {
				panic(fmt.Sprintf("TSESSION_ARCHIVE_SCALE: bad size %q", f))
			}
			sizes = append(sizes, n)
		}
	}
	for _, n := range sizes {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.json")
			ids := writeBenchArchive(t, path, n)
			st, err := os.Stat(path + ".archive")
			if err != nil {
				panic(err)
			}

			before := heapInUse()
			t0 := time.Now()
			fs, err := NewFileStore(path)
			closeLater(t, fs)
			if err != nil {
				panic(err)
			}
			open := time.Since(t0)
			perSession := (float64(heapInUse()) - float64(before)) / float64(n)

			// What a Manager asks for when it starts, and a list by state:
			// neither reads the lines of the finished sessions.
			t0 = time.Now()
			if l := fs.ListPending(); len(l) != 0 {
				panic(fmt.Sprintf("list pending: %d sessions, want none", len(l)))
			}
			pendingTime := time.Since(t0)
			t0 = time.Now()
			if l := fs.ListState(StateActive); len(l) != 0 {
				panic(fmt.Sprintf("list state: %d sessions, want none", len(l)))
			}
			stateTime := time.Since(t0)

			// Get: distinct sessions first (nothing cached), then the same ones again.
			const gets = 2000
			rng := rand.New(rand.NewPCG(1, uint64(n)))
			pick := make([]string, gets)
			for i := range pick {
				pick[i] = ids[rng.IntN(n)]
			}
			timeGets := func() time.Duration {
				t0 := time.Now()
				for _, id := range pick {
					if s, ok := fs.Get(id); !ok || s.ID != id || s.BundleURI == "" || len(s.Bindings) != 1 {
						panic(fmt.Sprintf("get %s: %+v %v", id, s, ok))
					}
				}
				return time.Since(t0) / gets
			}
			getFirst, getAgain := timeGets(), timeGets()

			timeList := func() time.Duration {
				t0 := time.Now()
				list := fs.List()
				d := time.Since(t0)
				if len(list) != n || list[0].ID != ids[0] || list[n-1].ID != ids[n-1] {
					panic(fmt.Sprintf("list: %d sessions, want %d", len(list), n))
				}
				return d
			}
			listFirst, listAgain := timeList(), timeList()
			afterUse := (float64(heapInUse()) - float64(before)) / float64(n)

			const deletes = 5
			t0 = time.Now()
			for _, id := range pick[:deletes] {
				if err := fs.Delete(id); err != nil {
					panic(err)
				}
			}
			deleteTime := time.Since(t0) / deletes
			if _, ok := fs.Get(pick[0]); ok {
				panic(fmt.Sprintf("%s readable after delete", pick[0]))
			}

			t.Logf("archive: %d sessions, %.1f MiB (%d B/session on disk)", n, float64(st.Size())/(1<<20), st.Size()/int64(n))
			t.Logf("open: %v; heap in use after open: %.0f B/session (%.1f MiB)", open.Round(time.Millisecond), perSession, perSession*float64(n)/(1<<20))
			t.Logf("get archived: %v first read, %v repeated", getFirst, getAgain)
			t.Logf("list: %v first, %v repeated; heap in use after gets and lists: %.0f B/session", listFirst.Round(time.Millisecond), listAgain.Round(time.Millisecond), afterUse)
			t.Logf("list pending: %v; list state ACTIVE: %v", pendingTime, stateTime)
			t.Logf("delete archived: %v", deleteTime)
			if maxPerSession := 256.0; n >= 20000 && perSession > maxPerSession {
				t.Errorf("an archived session costs %.0f B of heap, want at most %.0f: the index must not hold sessions", perSession, maxPerSession)
			}
			runtime.KeepAlive(fs)
		})
	}
}
