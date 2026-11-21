package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The verdict rides on the first event's line as one more member of its
// object: a reader that knows nothing of it (an older dashboard) decodes the
// same event, and one that does reads the verdict back.
func TestExportLineCarriesTheSession(t *testing.T) {
	e := Event{SessionID: "ts_1", Seq: 1, Kind: KindAPIIn, Service: "orders", Name: `POST /orders "tsession_session"`}
	meta := &ExportSession{State: "SEALED", Incomplete: true, DrainTimedOut: true, MaxDrainSeconds: 60,
		IncompleteReasons:   []string{`service orders-api (instance o-1): 0 of 6 recorded events reached the collector`},
		DeliveryUnconfirmed: []string{"service idle (instance n-1): unknown"}}

	plain, err := ExportLine(&e, nil)
	if err != nil || ExportSessionOf(plain) != nil {
		panic(fmt.Sprintf("a line without the session: %s %v (a member of that name inside the event is not the session)", plain, err))
	}
	if want, _ := json.Marshal(&e); string(plain) != string(want) {
		panic(fmt.Sprintf("a line without the session is the event: %s", plain))
	}

	line, err := ExportLine(&e, meta)
	if err != nil || strings.ContainsAny(string(line), "\n") {
		panic(fmt.Sprintf("%s %v", line, err))
	}
	var back Event
	if err := json.Unmarshal(line, &back); err != nil || !reflect.DeepEqual(back, e) {
		panic(fmt.Sprintf("an older reader decodes another event: %+v %v", back, err))
	}
	if got := ExportSessionOf(line); got == nil || !reflect.DeepEqual(*got, *meta) {
		panic(fmt.Sprintf("session read back: %+v", got))
	}
	// A complete session still says that it is one: the reader then knows the
	// verdict, as opposed to an export from before it was carried.
	line, _ = ExportLine(&e, &ExportSession{State: "SEALED"})
	if got := ExportSessionOf(line); got == nil || got.State != "SEALED" || got.Incomplete {
		panic(fmt.Sprintf("a complete session: %+v (%s)", got, line))
	}
	if ExportSessionOf([]byte(`{"session_id":"x","tsession_session":`)) != nil {
		panic("a torn line has no session")
	}
}

// A session without events has no event to carry the verdict: its export is
// one line with the session's id and the verdict. It is not an event, and a
// line that is one is not it.
func TestExportHeader(t *testing.T) {
	meta := &ExportSession{State: "SEALED", Incomplete: true,
		IncompleteReasons: []string{`service orders-api (instance o-1): 0 of 6 recorded events reached the collector`},
		StartedAt:         time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC), EndedAt: time.Date(2026, 11, 21, 10, 0, 12, 0, time.UTC)}
	line, err := ExportHeader("ts_1", meta)
	if err != nil || strings.ContainsAny(string(line), "\n") || !strings.HasPrefix(string(line), `{"session_id":"ts_1","tsession_session":{"state":"SEALED","incomplete":true,`) {
		panic(fmt.Sprintf("%s %v", line, err))
	}
	id, got, ok := ExportHeaderOf(line)
	if !ok || id != "ts_1" || !reflect.DeepEqual(*got, *meta) {
		panic(fmt.Sprintf("read back: %q %+v %v", id, got, ok))
	}
	// An event, with the verdict or without, is not a header; nor is a line
	// without a session, a torn one, or one that says nothing of a verdict.
	e := Event{SessionID: "ts_1", Seq: 1, Kind: KindAPIIn}
	withMeta, _ := ExportLine(&e, meta)
	plain, _ := ExportLine(&e, nil)
	for _, l := range [][]byte{withMeta, plain, []byte(`{"tsession_session":{"state":"SEALED"}}`), []byte(`{"session_id":"ts_1"}`),
		[]byte(`{"session_id":"ts_1","tsession_session":null}`), []byte(`{"session_id":"ts_1","kind":"api.in","tsession_session":{}}`), line[:len(line)-3]} {
		if _, _, ok := ExportHeaderOf(l); ok {
			panic(fmt.Sprintf("taken for the line of a session without events: %s", l))
		}
	}
	// The times are written for such a session only.
	if with, _ := ExportLine(&e, &ExportSession{State: "SEALED"}); strings.Contains(string(with), "started_at") {
		panic(fmt.Sprintf("an event's line: %s", with))
	}
}
