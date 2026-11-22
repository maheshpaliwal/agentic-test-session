package core

import (
	"bytes"
	"encoding/json"
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
	// Events is how many events the export holds. A reader that finds fewer
	// has a part of the file: one that was cut at the end of a line (a
	// download that stopped) looks whole otherwise, and would be judged on
	// the events that happen to be there.
	Events int `json:"events,omitempty"`
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
