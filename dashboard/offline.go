package dashboard

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// maxExportLine bounds one line of an export file (one event as JSON).
const maxExportLine = 64 << 20

// NewOffline serves sessions read from files written by `tracectl export`
// (one event per line). Each file becomes one read-only, sealed session named
// after the file, so a run can be opened without the services that recorded
// it: attach the export to a failed CI job and open it locally.
func NewOffline(paths []string) (*Demo, error) {
	d := &Demo{now: time.Now, readOnly: true}
	for _, p := range paths {
		s, err := loadExport(p)
		if err != nil {
			return nil, err
		}
		if d.find(s.ID) != nil {
			return nil, fmt.Errorf("%s: session %s was already opened from another file", p, s.ID)
		}
		d.sess = append(d.sess, s)
	}
	return d, nil
}

func loadExport(path string) (*demoSess, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var evs []core.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), maxExportLine)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var e core.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("%s:%d: not an exported event: %w", path, line, err)
		}
		if e.SessionID == "" || e.Seq == 0 {
			return nil, fmt.Errorf("%s:%d: not an exported event (no session_id or seq)", path, line)
		}
		if len(evs) > 0 && e.SessionID != evs[0].SessionID {
			return nil, fmt.Errorf("%s:%d: events of two sessions (%s and %s) in one file", path, line, evs[0].SessionID, e.SessionID)
		}
		evs = append(evs, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(evs) == 0 {
		return nil, fmt.Errorf("%s: no events", path)
	}
	slicex.SortBy(evs, func(e core.Event) uint64 { return e.Seq })
	first, last := evs[0].StartNs, evs[0].EndNs
	dropped := 0
	for i := range evs {
		first, last = min(first, evs[i].StartNs), max(last, evs[i].EndNs)
		if evs[i].Kind == core.KindSDKDropped {
			var n int
			fmt.Sscan(evs[i].Attrs["count"], &n)
			dropped += n
		}
	}
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	start, end := ts(time.Unix(0, first)), ts(time.Unix(0, last))
	return &demoSess{
		Session: Session{ID: evs[0].SessionID, Name: name, Mode: "header", State: "SEALED", InitiatedBy: "file",
			CreatedAt: start, StartedAt: start, EndedAt: end, DroppedEvents: dropped, Incomplete: dropped > 0},
		events: evs, sealed: true, resolved: true}, nil
}

// readOnlyAPI refuses everything that would change a session.
func readOnlyAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeJSON(w, http.StatusMethodNotAllowed, m{"error": "read-only: these sessions were opened from files"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
