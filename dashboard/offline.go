package dashboard

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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
	seqs := map[uint64]int{} // seq -> line that holds it
	torn := false            // the previous line did not parse
	var tornErr error
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), maxExportLine)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		if torn { // only the last line may be cut short
			return nil, tornErr
		}
		var e core.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			torn, tornErr = true, fmt.Errorf("%s:%d: not an exported event: %w", path, line, err)
			continue
		}
		if e.SessionID == "" || e.Seq == 0 {
			return nil, fmt.Errorf("%s:%d: not an exported event (no session_id or seq)", path, line)
		}
		if e.Seq > math.MaxInt64 {
			return nil, fmt.Errorf("%s:%d: seq %d is out of range", path, line, e.Seq)
		}
		if first, dup := seqs[e.Seq]; dup {
			return nil, fmt.Errorf("%s:%d: seq %d was already used on line %d", path, line, e.Seq, first)
		}
		seqs[e.Seq] = line
		if len(evs) > 0 && e.SessionID != evs[0].SessionID {
			return nil, fmt.Errorf("%s:%d: events of two sessions (%s and %s) in one file", path, line, evs[0].SessionID, e.SessionID)
		}
		evs = append(evs, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(evs) == 0 {
		if torn {
			return nil, tornErr
		}
		return nil, fmt.Errorf("%s: no events", path)
	}
	slicex.SortBy(evs, func(e core.Event) uint64 { return e.Seq })
	var first, last int64
	dropped := 0
	for i := range evs {
		if s := evs[i].StartNs; s > 0 && (first == 0 || s < first) {
			first = s
		}
		last = max(last, evs[i].EndNs)
		if evs[i].Kind == core.KindSDKDropped {
			if n, err := strconv.Atoi(evs[i].Attrs["count"]); err == nil && n > 0 {
				dropped += n
			}
		}
	}
	if first == 0 {
		first = last
	}
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	// To the nanosecond: a recording is often shorter than a second, and whole
	// seconds would show its duration as zero.
	start, end := time.Unix(0, first).UTC().Format(time.RFC3339Nano), time.Unix(0, last).UTC().Format(time.RFC3339Nano)
	return &demoSess{
		Session: Session{ID: evs[0].SessionID, Name: name, Mode: "header", State: "SEALED", InitiatedBy: "file",
			CreatedAt: start, StartedAt: start, EndedAt: end, DroppedEvents: dropped,
			// A file cut short mid-line (an interrupted export) still opens; what it holds is not everything.
			Incomplete: dropped > 0 || torn},
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
