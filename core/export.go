package core

import (
	"bytes"
	"encoding/json"
)

// ExportSessionKey is the member of the first line of an export that holds
// the session's verdict (see ExportSession). The line is an event like every
// other; the member is one more field of its JSON object, which a reader
// that does not know it skips, as it skips every unknown field of an event.
const ExportSessionKey = "tsession_session"

// ExportSession is what an export says about its session besides the events:
// the verdict the control plane reached when it sealed the session, which no
// event holds. An export without it was written before there was one, or of
// a session the control plane no longer knew.
type ExportSession struct {
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
