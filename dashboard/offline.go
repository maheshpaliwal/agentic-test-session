package dashboard

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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
func NewOffline(paths []string) (*Demo, error) { return newOffline(paths, false) }

// NewOfflineStrict is NewOffline for a reader that acts on the verdict of a
// file (tracectl --from and its --fail-on gates). A file that does not say
// how its session ended, or how many events it holds (its first line is
// lost, it was written by an older tracectl, or the control plane could not
// be asked when it was exported), is opened with an unknown state, as a
// session the control plane has forgotten is read from the collector: it can
// be read, and no gate passes on it. NewOffline shows such a file as a
// sealed session, for a person to look at. OpenedFile.Unproven names them.
func NewOfflineStrict(paths []string) (*Demo, error) { return newOffline(paths, true) }

func newOffline(paths []string, strict bool) (*Demo, error) {
	d := &Demo{now: time.Now, readOnly: true}
	for i, p := range paths {
		if slices.Contains(paths[:i], p) { // before it is read: standard input is read once
			return nil, fmt.Errorf("%s was given twice", p)
		}
		s, err := loadExport(p)
		if err != nil {
			return nil, err
		}
		if strict && s.unproven {
			s.State = ""
		}
		if prev := d.find(s.ID); prev != nil {
			// Sessions are told apart by their id, so two exports of one session cannot be open
			// together: not to list them, and not to compare an earlier export with a later one.
			return nil, fmt.Errorf("%s and %s are exports of the same session (%s): only one of them can be read at a time, and two exports of one session cannot be compared",
				prev.path, p, s.ID)
		}
		d.sess = append(d.sess, s)
	}
	return d, nil
}

// OpenedFile is one file given to NewOffline and the session read from it.
type OpenedFile struct {
	Path      string
	SessionID string
	// Truncated is set when the last line of the file was cut short (an
	// interrupted export, a partial download). The session is served
	// without that line and marked incomplete; a reader that must not judge
	// a part of a recording refuses the file instead.
	Truncated error
	// Name is the session's name: the one the export stored, or the file's
	// name without its extension.
	Name string
	// Unproven: the file does not say how its session ended or how many
	// events it holds (see NewOfflineStrict).
	Unproven bool
}

// Files returns the files the sessions were opened from, in the order they
// were given to NewOffline; nil for the demo.
func (d *Demo) Files() []OpenedFile {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []OpenedFile
	for _, s := range d.sess {
		if s.path != "" {
			out = append(out, OpenedFile{Path: s.path, SessionID: s.ID, Name: s.Name, Truncated: s.torn, Unproven: s.unproven})
		}
	}
	return out
}

// openExport opens the export at path for reading: the file, or standard
// input for "-", and what gzip made of either (an artifact store often
// compresses), which the first two bytes tell.
func openExport(path string) (io.Reader, func(), error) {
	in, done := io.Reader(os.Stdin), func() {}
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		in, done = f, func() { _ = f.Close() }
	}
	br := bufio.NewReader(in)
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, err := gzip.NewReader(br)
		if err != nil {
			done()
			return nil, nil, fmt.Errorf("%s: gzip: %w", path, err)
		}
		return zr, done, nil
	}
	return br, done, nil
}

func loadExport(path string) (*demoSess, error) {
	f, done, err := openExport(path)
	if err != nil {
		return nil, err
	}
	defer done()
	var evs []core.Event
	var meta *core.ExportSession // the session's verdict, on the first line
	id := ""                     // the session of the file
	seqs := map[uint64]int{}     // seq -> line that holds it
	torn := false                // the previous line did not parse
	var tornErr error
	// No more than maxExportBytes are read (after decompression): the events are held in memory, several
	// times their size, and a small gzip file can hold any amount of them.
	limited := &io.LimitedReader{R: f, N: maxExportBytes + 1}
	sc := bufio.NewScanner(limited)
	sc.Buffer(make([]byte, 0, 64<<10), maxExportLine)
	firstLine := true // the next line with text is the file's first
	for line := 1; sc.Scan(); line++ {
		if limited.N <= 0 {
			return nil, fmt.Errorf("%s: more than %d MB of events: too large to open; read the session from the services, or export a shorter one", path, maxExportBytes>>20)
		}
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
		// The verdict is on the first line and nowhere else. One that cannot be read is not passed over, and
		// neither is a second one: which of two would be the session's?
		m, present, verr := core.ExportVerdict(sc.Bytes())
		if sid, _, ok := core.ExportHeaderOf(sc.Bytes()); ok && id != "" && sid != id {
			return nil, fmt.Errorf("%s:%d: two sessions (%s and %s) in one file", path, line, id, clipID(sid))
		}
		switch {
		case verr != nil:
			return nil, fmt.Errorf("%s:%d: %w", path, line, verr)
		case present && !firstLine:
			return nil, fmt.Errorf("%s:%d: a second %s: an export has one, on its first line", path, line, core.ExportSessionKey)
		case present:
			meta = m
		}
		firstLine = false
		// The line of a session without events: its id and its verdict.
		if sid, _, ok := core.ExportHeaderOf(sc.Bytes()); ok {
			if !plainID(sid) {
				return nil, fmt.Errorf("%s:%d: the session id is not one (%q)", path, line, clipID(sid))
			}
			if id != "" && sid != id {
				return nil, fmt.Errorf("%s:%d: two sessions (%s and %s) in one file", path, line, id, sid)
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
		if !plainID(e.SessionID) {
			return nil, fmt.Errorf("%s:%d: the session id is not one (%q)", path, line, clipID(e.SessionID))
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
	if limited.N <= 0 {
		return nil, fmt.Errorf("%s: more than %d MB of events: too large to open; read the session from the services, or export a shorter one", path, maxExportBytes>>20)
	}
	if id == "" {
		if torn {
			return nil, tornErr
		}
		return nil, fmt.Errorf("%s: no events, and no session: an export of a session without events holds one line with its verdict", path)
	}
	// Another number of events than the export wrote: the file was cut at the end of a line, which no line
	// shows, or lines were added to it.
	if meta != nil && meta.Events != nil && len(evs) != *meta.Events && !torn {
		torn, tornErr = true, fmt.Errorf("%s: holds %d of the %d events that were exported", path, len(evs), *meta.Events)
	}
	// Without the verdict and the count the file does not say how its session ended, nor that it is whole.
	unproven := meta == nil || meta.Events == nil
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
	name := strings.TrimSuffix(filepath.Base(path), ".gz")
	if name = strings.TrimSuffix(name, filepath.Ext(name)); path == "-" {
		name = "stdin"
	}
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
		// The session's own name, where the export carries it; the file's name stands in for an export
		// from before that.
		if n := cleanName(meta.Name); n != "" {
			sess.Name, sess.NameAuto = n, meta.NameAuto
		}
		// A session that was exported while it ran is not a finished one: its events are those that had
		// arrived, and nothing was sealed.
		if st := controlplane.State(meta.State); st != "" && !st.Terminal() {
			sess.State, sess.EndedAt, cv.sealed = st, time.Time{}, false
		} else if st != "" {
			sess.State = st
		}
		// Reasons or a failed seal are incompleteness, whatever the flag next to them says.
		sess.Incomplete = sess.Incomplete || meta.Incomplete || len(meta.IncompleteReasons) > 0 || meta.SealError != ""
		sess.IncompleteReasons, sess.DeliveryUnconfirmed = meta.IncompleteReasons, meta.DeliveryUnconfirmed
		sess.DrainTimedOut, sess.MaxDrainSeconds, sess.SealError = meta.DrainTimedOut, meta.MaxDrainSeconds, meta.SealError
	}
	ds := &demoSess{Session: sess, events: evs, sealed: cv.sealed, file: cv, path: path, unproven: unproven}
	if torn {
		ds.torn = tornErr
	}
	return ds, nil
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

// maxExportBytes is the most that is read of one export, after decompression.
// A variable for the tests.
var maxExportBytes int64 = 512 << 20

// plainID reports whether id can be a session id: printable ASCII without
// spaces, and not too long. What a file calls its session is printed to
// terminals and put into commands.
func plainID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if c := id[i]; c <= ' ' || c >= 0x7f || c == '\'' || c == '"' || c == '`' || c == '\\' || c == '$' {
			return false
		}
	}
	return true
}

func clipID(id string) string {
	if len(id) > 40 {
		return id[:40] + "..."
	}
	return id
}

// cleanName is a session name from a file without what would steer a
// terminal: control and format characters are dropped, and white space
// becomes a blank.
func cleanName(name string) string {
	name = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r):
			return ' '
		case unicode.In(r, unicode.Cc, unicode.Cf, unicode.Co) || r == utf8.RuneError:
			return -1
		}
		return r
	}, name)
	if name = strings.Join(strings.Fields(name), " "); len(name) > 200 {
		name = strings.ToValidUTF8(name[:200], "")
	}
	return strings.TrimSpace(name)
}
