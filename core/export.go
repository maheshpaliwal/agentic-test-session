package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ExportSessionKey is the member of the first line of an export that holds
// the session's verdict (see ExportSession). The line is an event like every
// other; the member is one more field of its JSON object, which a reader
// that does not know it skips, as it skips every unknown field of an event.
//
// A session without events has no event to carry it. Its export is one line
// that holds the session's id and the verdict and nothing else (see
// ExportHeader): the session the feature exists for, whose only service
// could not deliver anything, must not export as an empty file.
const ExportSessionKey = "tsession_session"

// ExportSession is what an export says about its session besides the events:
// the verdict the control plane reached when it sealed the session, which no
// event holds. An export without it was written before there was one, or of
// a session the control plane no longer knew.
type ExportSession struct {
	// Name is the session's name; NameAuto says that it was generated.
	Name     string `json:"name,omitempty"`
	NameAuto bool   `json:"name_auto,omitempty"`
	// Events is how many events the export holds; nil in an export from
	// before it was written. A reader that finds another number has a part
	// of the file, or more than the file: one that was cut at the end of a
	// line (a download that stopped) looks whole otherwise, and would be
	// judged on the events that happen to be there.
	Events *int `json:"events,omitempty"`
	// CommandStatus is the exit status of the command that `tracectl run`
	// ran in the session, when the export was written by it; nil otherwise.
	// A run can fail without a failed event (a test that asserts on a
	// response): a reader that judges the file alone would pass it.
	CommandStatus *int `json:"command_status,omitempty"`
	// State is the session's state when it was exported.
	State      string `json:"state,omitempty"`
	Incomplete bool   `json:"incomplete,omitempty"`
	// IncompleteReasons and DeliveryUnconfirmed are the control plane's: the
	// service instances whose events did not all reach the collector, and
	// those of which that could not be told.
	IncompleteReasons   []string `json:"incomplete_reasons,omitempty"`
	DeliveryUnconfirmed []string `json:"delivery_unconfirmed,omitempty"`
	// DrainTimedOut: the drain ended at MaxDrainSeconds, not on a quiet
	// collector.
	DrainTimedOut   bool   `json:"drain_timed_out,omitempty"`
	MaxDrainSeconds int    `json:"max_drain_seconds,omitempty"`
	SealError       string `json:"seal_error,omitempty"`
	// StartedAt and EndedAt are when the session ran. They are written for a
	// session without events only: the events say it otherwise.
	StartedAt time.Time `json:"started_at,omitzero"`
	EndedAt   time.Time `json:"ended_at,omitzero"`
}

// ExportHeader returns the one line an export of a session without events
// consists of, without the newline: the session's id and its verdict,
//
//	{"session_id":"ts_...","tsession_session":{"state":"SEALED","incomplete":true,...}}
//
// It is not an event: it has no seq and no kind. A reader from before it
// (dashboard -open) refuses the file with "not an exported event (no
// session_id or seq)", where it refused the empty file with "no events".
func ExportHeader(sessionID string, meta *ExportSession) ([]byte, error) {
	return json.Marshal(struct {
		SessionID string         `json:"session_id"`
		Meta      *ExportSession `json:"tsession_session"`
	}{sessionID, meta})
}

// ExportHeaderOf reads a line written by ExportHeader. ok is false for every
// other line: an event (with or without the verdict), or anything else.
func ExportHeaderOf(line []byte) (sessionID string, meta *ExportSession, ok bool) {
	if !bytes.Contains(line, []byte(`"`+ExportSessionKey+`"`)) {
		return "", nil, false
	}
	var v struct {
		SessionID string         `json:"session_id"`
		Seq       uint64         `json:"seq"`
		Kind      string         `json:"kind"`
		Meta      *ExportSession `json:"tsession_session"`
	}
	if json.Unmarshal(line, &v) != nil || v.Meta == nil || v.SessionID == "" || v.Seq != 0 || v.Kind != "" {
		return "", nil, false
	}
	return v.SessionID, v.Meta, true
}

// ExportLine returns e as one line of an export, without the newline. With
// meta the line also carries the session's verdict; that is the first line.
func ExportLine(e *Event, meta *ExportSession) ([]byte, error) {
	line, err := json.Marshal(e)
	if err != nil || meta == nil {
		return line, err
	}
	m, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	// {"...":...} becomes {"...":...,"tsession_session":{...}}.
	out := make([]byte, 0, len(line)+len(m)+len(ExportSessionKey)+4)
	out = append(out, line[:len(line)-1]...)
	if len(line) > 2 {
		out = append(out, ',')
	}
	out = append(out, '"')
	out = append(out, ExportSessionKey...)
	out = append(out, '"', ':')
	out = append(out, m...)
	return append(out, '}'), nil
}

// ExportSessionOf returns the verdict an export line carries, nil when it
// carries none.
func ExportSessionOf(line []byte) *ExportSession {
	if !bytes.Contains(line, []byte(`"`+ExportSessionKey+`"`)) {
		return nil // every line but the first
	}
	var v struct {
		Meta *ExportSession `json:"tsession_session"`
	}
	if json.Unmarshal(line, &v) != nil {
		return nil
	}
	return v.Meta
}

// ExportStates are the values of ExportSession.State: the session states of
// the control plane.
var ExportStates = []string{"CREATED", "ACTIVE", "DRAINING", "SEALED", "ABORTED", "EXPIRED"}

// ExportVerdict reads the verdict of an export line for a reader that acts
// on it. present says that the line has the key; err, that what is under it
// cannot be relied on: it does not decode (a number written as a string, a
// value of the wrong kind), counts fewer than no events, or names a state
// that is none. A verdict that is half read is worse than none: the reader
// would judge the session without the part that did not decode.
func ExportVerdict(line []byte) (meta *ExportSession, present bool, err error) {
	if !bytes.Contains(line, []byte(`"`+ExportSessionKey+`"`)) {
		return nil, false, nil
	}
	var v struct {
		Meta json.RawMessage `json:"tsession_session"`
	}
	if json.Unmarshal(line, &v) != nil {
		return nil, false, nil // not a JSON object: the reader says so in its own words
	}
	if v.Meta == nil {
		return nil, false, nil // the text is inside a recorded value, not a key of the line
	}
	// A member written twice is read as its last: a verdict that says "incomplete" could be followed by one
	// that does not, and the reader would see the second only. The same for a key in another case, which the
	// decoder takes for the same member.
	if n := countKey(line, ExportSessionKey); n != 1 {
		return nil, true, fmt.Errorf("it has %d members named %s (in any case): which one is the verdict cannot be told", n, ExportSessionKey)
	}
	if k, dup := repeatedKey(v.Meta); dup {
		return nil, true, fmt.Errorf("its %s has the member %q twice", ExportSessionKey, k)
	}
	meta = &ExportSession{}
	// Members this version does not know are passed over: a later version adds them.
	if err := json.Unmarshal(v.Meta, meta); err != nil {
		return nil, true, fmt.Errorf("its %s does not read as one: %w", ExportSessionKey, err)
	}
	if meta.Events != nil && *meta.Events < 0 {
		return nil, true, fmt.Errorf("its %s counts %d events", ExportSessionKey, *meta.Events)
	}
	if meta.CommandStatus != nil && (*meta.CommandStatus < 0 || *meta.CommandStatus > 255) {
		return nil, true, fmt.Errorf("its %s has the command status %d, which is no exit status", ExportSessionKey, *meta.CommandStatus)
	}
	if meta.State != "" && !slices.Contains(ExportStates, meta.State) {
		return nil, true, fmt.Errorf("its %s has the state %q, which is none", ExportSessionKey, meta.State)
	}
	return meta, true, nil
}

// objectKeys calls fn with every key of the JSON object b, in order, and
// reports whether b is an object that could be walked to its end.
func objectKeys(b []byte, fn func(key string)) bool {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return false
	}
	for dec.More() {
		t, err := dec.Token()
		k, ok := t.(string)
		if err != nil || !ok {
			return false
		}
		fn(k)
		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			return false
		}
	}
	return true
}

// countKey is how many keys of the object b are key, compared as the JSON
// decoder compares them to a field (without regard to case).
func countKey(b []byte, key string) int {
	n := 0
	objectKeys(b, func(k string) {
		if strings.EqualFold(k, key) {
			n++
		}
	})
	return n
}

// repeatedKey returns a key the object b has twice, compared without regard
// to case.
func repeatedKey(b []byte) (string, bool) {
	seen := map[string]struct{}{}
	rep := ""
	objectKeys(b, func(k string) {
		f := strings.ToLower(k)
		if _, dup := seen[f]; dup && rep == "" {
			rep = k
		}
		seen[f] = struct{}{}
	})
	return rep, rep != ""
}
