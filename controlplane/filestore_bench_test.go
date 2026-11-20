package controlplane

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// benchSessions returns n finished sessions shaped like real ones.
func benchSessions(n int) []Session {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	out := make([]Session, n)
	for i := range out {
		at := base.Add(time.Duration(i) * time.Second)
		out[i] = Session{
			ID: fmt.Sprintf("ts_%011x%012x", at.UnixMilli(), i), Name: fmt.Sprintf("checkout flow %d", i),
			Mode: ModeHeader, State: StateSealed, InitiatedBy: "bench", CreatedAt: at, StartedAt: at,
			EndedAt: at.Add(time.Minute), TTLSeconds: 3600, QuiesceSeconds: 3, MaxDrainSeconds: 60,
			BundleURI: fmt.Sprintf("file:///var/lib/tsession/bundles/%d.tar.zst", i),
			Bindings:  []Binding{{Name: "x-user-id", Value: fmt.Sprintf("user-%d", i)}},
		}
	}
	return out
}

// benchStore opens a FileStore over a snapshot file holding sessions. The
// snapshot is written directly (a JSON array, the format every version
// reads), so that the setup does not depend on how fast Put is.
func benchStore(b *testing.B, snapshot []byte) *FileStore {
	b.Helper()
	path := filepath.Join(b.TempDir(), "s.json")
	if err := os.WriteFile(path, snapshot, 0o600); err != nil {
		b.Fatal(err)
	}
	fs, err := NewFileStore(path)
	closeLater(b, fs)
	if err != nil {
		b.Fatal(err)
	}
	return fs
}

// BenchmarkFileStorePut measures one Put (a session changing state) with a
// given number of sessions in the hot set.
func BenchmarkFileStorePut(b *testing.B) {
	for _, hot := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("hot=%d", hot), func(b *testing.B) {
			ss := benchSessions(hot)
			snapshot, _ := json.Marshal(ss)
			fs := benchStore(b, snapshot)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := ss[i%hot]
				s.Name = fmt.Sprintf("renamed %d", i)
				if err := fs.Put(s); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFileStoreEvict measures one retention sweep moving batch sessions
// out of a hot set of the given size into the archive.
func BenchmarkFileStoreEvict(b *testing.B) {
	for _, hot := range []int{1000, 10000} {
		for _, batch := range []int{1, 500} {
			b.Run(fmt.Sprintf("hot=%d/batch=%d", hot, batch), func(b *testing.B) {
				ss := benchSessions(hot)
				snapshot, _ := json.Marshal(ss)
				ids := make([]string, batch)
				for i := range ids {
					ids[i] = ss[i].ID
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					fs := benchStore(b, snapshot)
					b.StartTimer()
					fs.Evict(ids...)
				}
			})
		}
	}
}
