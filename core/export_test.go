package core

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
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
		t.Fatalf("a line without the session: %s %v (a member of that name inside the event is not the session)", plain, err)
	}
	if want, _ := json.Marshal(&e); string(plain) != string(want) {
		t.Fatalf("a line without the session is the event: %s", plain)
	}

	line, err := ExportLine(&e, meta)
	if err != nil || strings.ContainsAny(string(line), "\n") {
		t.Fatalf("%s %v", line, err)
	}
	var back Event
	if err := json.Unmarshal(line, &back); err != nil || !reflect.DeepEqual(back, e) {
		t.Fatalf("an older reader decodes another event: %+v %v", back, err)
	}
	if got := ExportSessionOf(line); got == nil || !reflect.DeepEqual(*got, *meta) {
		t.Fatalf("session read back: %+v", got)
	}
	// A complete session still says that it is one: the reader then knows the
	// verdict, as opposed to an export from before it was carried.
	line, _ = ExportLine(&e, &ExportSession{State: "SEALED"})
	if got := ExportSessionOf(line); got == nil || got.State != "SEALED" || got.Incomplete {
		t.Fatalf("a complete session: %+v (%s)", got, line)
	}
	if ExportSessionOf([]byte(`{"session_id":"x","tsession_session":`)) != nil {
		t.Fatal("a torn line has no session")
	}
}
