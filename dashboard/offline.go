package dashboard

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/slicex"
)

// maxExportLine bounds one line of an export file (one event as JSON).
const maxExportLine = 64 << 20

// NewOffline serves sessions read from files written by `tracectl export`
// (one event per line, the session's verdict on the first: see
// core.ExportSession; or the verdict alone, for a session without events:
// see core.ExportHeader). Each file becomes one read-only, sealed session named
// after the file, so a run can be opened without the services that recorded
// it: attach the export to a failed CI job and open it locally. The read
// endpoints answer as the real services do, so tracectl reads the file too.
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
	var meta *core.ExportSession // the session's verdict, on the line of an event (the first)
	id := ""                     // the session of the file
	seqs := map[uint64]int{}     // seq -> line that holds it
	torn := false                // the previous line did not parse
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
		// The line of a session without events: its id and its verdict.
		if sid, m, ok := core.ExportHeaderOf(sc.Bytes()); ok {
			if id != "" && sid != id {
				return nil, fmt.Errorf("%s:%d: two sessions (%s and %s) in one file", path, line, id, sid)
			}
			if meta == nil {
				meta = m
			}
			id = sid
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
		if meta == nil {
			meta = core.ExportSessionOf(sc.Bytes())
		}
		if id != "" && e.SessionID != id {
			return nil, fmt.Errorf("%s:%d: events of two sessions (%s and %s) in one file", path, line, id, e.SessionID)
		}
		id = e.SessionID
		evs = append(evs, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if id == "" {
		if torn {
			return nil, tornErr
		}
		return nil, fmt.Errorf("%s: no events, and no session: an export of a session without events holds one line with its verdict", path)
	}
	slicex.SortBy(evs, func(e core.Event) uint64 { return e.Seq })
	var first, last int64
	for i := range evs {
		if s := evs[i].StartNs; s > 0 && (first == 0 || s < first) {
			first = s
		}
		last = max(last, evs[i].EndNs)
	}
	if first == 0 {
		first = last
	}
	if len(evs) == 0 && meta != nil && !meta.StartedAt.IsZero() {
		// No event says when the session ran: the verdict's line does.
		first = meta.StartedAt.UnixNano()
		if last = first; meta.EndedAt.After(meta.StartedAt) {
			last = meta.EndedAt.UnixNano()
		}
	}
	// The collector's view of the session. The events are served as they are:
	// they were read from the collector, so they already carry transaction
	// outcomes and the orphaned flag. What the seal held besides them is
	// worked out the way the collector does it: the jobs no worker finished
	// and the events the SDKs dropped.
	open, dropped := ledger(evs)
	cv := &colView{id: id, evs: evs, sealed: true, pending: open, dropped: dropped,
		// A file cut short mid-line (an interrupted export) still opens; what it holds is not everything.
		incomplete: len(open) > 0 || dropped > 0 || torn}
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	// To the nanosecond: a recording is often shorter than a second, and whole
	// seconds would show its duration as zero.
	start, end := time.Unix(0, first).UTC(), time.Unix(0, last).UTC()
	sess := Session{ID: cv.id, Name: name, Mode: controlplane.ModeHeader, State: controlplane.StateSealed, InitiatedBy: "file",
		CreatedAt: start, StartedAt: start, EndedAt: end, DroppedEvents: dropped, Incomplete: cv.incomplete}
	// What the control plane knew and the events do not show: that events
	// never reached the collector, that the drain ran out of time, that the
	// seal failed. An export from before the verdict was carried has none,
	// and the session is then judged on its events alone.
	if meta != nil {
		sess.Incomplete = sess.Incomplete || meta.Incomplete
		sess.IncompleteReasons, sess.DeliveryUnconfirmed = meta.IncompleteReasons, meta.DeliveryUnconfirmed
		sess.DrainTimedOut, sess.MaxDrainSeconds, sess.SealError = meta.DrainTimedOut, meta.MaxDrainSeconds, meta.SealError
	}
	return &demoSess{Session: sess, events: evs, sealed: true, file: cv, path: path}, nil
}

// readOnlyAPI refuses everything that would change a session, and says that
// the session is a file.
func (d *Demo) readOnlyAPI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		msg := "read-only: this dashboard serves sessions opened from files (dashboard -open); nothing can be started or changed here"
		if rest, ok := strings.CutPrefix(r.URL.Path, "/v1/sessions/"); ok {
			id, _, _ := strings.Cut(rest, "/")
			d.mu.Lock()
			s := d.find(id)
			d.mu.Unlock()
			if s != nil {
				msg = fmt.Sprintf("read-only: session %s is a file (%s, opened with dashboard -open); it cannot be stopped, aborted, renamed or deleted", id, filepath.Base(s.path))
			}
		}
		writeJSON(w, http.StatusMethodNotAllowed, m{"error": msg})
	})
}
