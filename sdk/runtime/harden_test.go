package runtime

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestExporterBisectsOn413(t *testing.T) {
	var mu sync.Mutex
	var got []core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := bufio.NewScanner(r.Body)
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		var evs []core.Event
		for sc.Scan() {
			var e core.Event
			_ = json.Unmarshal(sc.Bytes(), &e)
			evs = append(evs, e)
		}
		for _, e := range evs {
			if e.Attrs["big"] == "1" { // simulate size limit on the whole request
				http.Error(w, "too big", http.StatusRequestEntityTooLarge)
				return
			}
		}
		mu.Lock()
		got = append(got, evs...)
		mu.Unlock()
		fmt.Fprint(w, `{"accepted":1,"duplicates":0,"rejected":0}`)
	}))
	defer srv.Close()
	r := NewRecorder(100)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, BatchSize: 16, FlushInterval: time.Hour, RetryBackoff: time.Millisecond})
	for i := 1; i <= 8; i++ {
		e := core.Event{SessionID: "s", SpanID: fmt.Sprint("sp", i), LocalSeq: uint64(i), Kind: core.KindAPIIn, Name: "n"}
		if i == 5 {
			e.Attrs = map[string]string{"big": "1"}
		}
		r.Record(e)
	}
	if !x.Flush(3 * time.Second) {
		t.Fatal("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 8 {
		t.Fatalf("want 8 events delivered, got %d", len(got))
	}
	for _, e := range got {
		if e.LocalSeq == 5 {
			if len(e.Flags) != 1 || e.Flags[0] != "oversize_dropped" || e.Err == "" || e.SpanID != "sp5" || e.Attrs != nil {
				t.Fatalf("bad stub: %+v", e)
			}
		} else if len(e.Flags) != 0 {
			t.Fatalf("collateral damage: %+v", e)
		}
	}
}

func TestExporterCountsRejected(t *testing.T) {
	var mu sync.Mutex
	var drops []core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc := bufio.NewScanner(r.Body)
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		for sc.Scan() {
			var e core.Event
			_ = json.Unmarshal(sc.Bytes(), &e)
			if e.Kind == core.KindSDKDropped {
				mu.Lock()
				drops = append(drops, e)
				mu.Unlock()
			}
		}
		fmt.Fprint(w, `{"accepted":1,"duplicates":0,"rejected":3}`)
	}))
	defer srv.Close()
	r := NewRecorder(100)
	x := NewExporter(r, ExporterConfig{CollectorURL: srv.URL, FlushInterval: time.Hour})
	defer x.Close(time.Second)
	r.Record(core.Event{SessionID: "s", LocalSeq: 1})
	x.Flush(time.Second)
	x.Flush(time.Second) // second drain ships the sdk.dropped event
	mu.Lock()
	defer mu.Unlock()
	if len(drops) == 0 || drops[0].Attrs["count"] != "3" {
		t.Fatalf("rejected not reported: %+v", drops)
	}
}

func TestTransportPropagationFilters(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = r.Header.Get(core.KeySession)
		mu.Unlock()
	}))
	defer srv.Close()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()
	ctx := core.ContextWith(t.Context(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	do := func(tr *Transport, path string) {
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/"+path+"?token=SYNTH", nil)
		resp, err := (&http.Client{Transport: tr}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	do(&Transport{}, "all")                                              // default: everyone
	do(&Transport{PropagateHosts: []string{"example.com"}}, "notlisted") // 127.0.0.1 not listed
	do(&Transport{PropagateHosts: []string{"127.0.0.1"}}, "listed")
	do(&Transport{PropagateHosts: []string{"127.0.0.1"}, NoPropagate: []string{"127.0.0.1"}}, "denied") // deny wins
	mu.Lock()
	defer mu.Unlock()
	if seen["/all"] != "s1" || seen["/listed"] != "s1" || seen["/notlisted"] != "" || seen["/denied"] != "" {
		t.Fatalf("propagation wrong: %v", seen)
	}
	evs := rec.Events()
	if len(evs) != 4 {
		t.Fatalf("all requests must be recorded, got %d", len(evs))
	}
	for _, e := range evs {
		if strings.Contains(string(e.Req.Inline), "SYNTH") {
			t.Fatalf("query token leaked: %s", e.Req.Inline)
		}
	}
	if !hostMatches("a.b.example.com:8080", []string{".example.com"}) || hostMatches("notexample.com", []string{"example.com"}) {
		t.Fatal("hostMatches")
	}
}
