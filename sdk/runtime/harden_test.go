package runtime

import (
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
		evs := readFrames(r.Body)
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
		panic("flush")
	}
	x.Close(time.Second)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 8 {
		panic(fmt.Sprintf("want 8 events delivered, got %d", len(got)))
	}
	for _, e := range got {
		if e.LocalSeq == 5 {
			if len(e.Flags) != 1 || e.Flags[0] != "oversize_dropped" || e.Err == "" || e.SpanID != "sp5" || e.Attrs != nil {
				panic(fmt.Sprintf("bad stub: %+v", e))
			}
		} else if len(e.Flags) != 0 {
			panic(fmt.Sprintf("collateral damage: %+v", e))
		}
	}
}

func TestExporterCountsRejected(t *testing.T) {
	var mu sync.Mutex
	var drops []core.Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, e := range readFrames(r.Body) {
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
		panic(fmt.Sprintf("rejected not reported: %+v", drops))
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
			panic(err)
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
		panic(fmt.Sprintf("propagation wrong: %v", seen))
	}
	evs := rec.Events()
	if len(evs) != 4 {
		panic(fmt.Sprintf("all requests must be recorded, got %d", len(evs)))
	}
	for _, e := range evs {
		if strings.Contains(string(e.Req.Inline), "SYNTH") {
			panic(fmt.Sprintf("query token leaked: %s", e.Req.Inline))
		}
	}
	if !hostMatches("a.b.example.com:8080", []string{".example.com"}) || hostMatches("notexample.com", []string{"example.com"}) {
		panic("hostMatches")
	}
}

// Close must not wait out a request that is stuck on an unresponsive
// collector: the in-flight POST is abandoned when the flush budget is spent.
func TestCloseAbandonsHungCollector(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	rec := NewRecorder(16)
	x := NewExporter(rec, ExporterConfig{CollectorURL: srv.URL, HTTPTimeout: 30 * time.Second, FlushInterval: time.Hour})
	rec.Record(core.Event{SessionID: "s", Kind: core.KindAPIIn})
	start := time.Now()
	x.Close(50 * time.Millisecond)
	if d := time.Since(start); d > 5*time.Second {
		panic(fmt.Sprintf("Close took %v with a hung collector (HTTP timeout is 30s)", d))
	}
}
