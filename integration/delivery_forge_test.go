package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/proto/tsessionproto"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// What a caller without credentials can do to the delivery verdict of a
// session, and what it cannot: the statements of docs/PROTOCOL.md ("Limits")
// one by one, against a real collector and control plane. Nothing here is
// authenticated (nothing in tsession is); the tests pin what follows from
// that, so that the document stays true. They are the scenarios an
// adversarial review ran with a forging tool.
//
// The control plane runs with a "second" of 100ms: the grace is 1s.
type forgeEnv struct {
	t  *testing.T
	in *infra
}

func (f *forgeEnv) post(path string, body any, hdr map[string]string) (int, string) {
	f.t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, f.in.cpSrv.URL+path, strings.NewReader(string(b)))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := deliveryHTTP.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(out))
}

type forgedReport map[string]any

// report posts a delivery report in an instance's name.
func (f *forgeEnv) report(id string, r forgedReport) (int, string) {
	return f.post("/v1/sessions/"+id+"/delivery", r, nil)
}

// ingest sends n events to the collector in an instance's name.
func (f *forgeEnv) ingest(id, service, instance string, n int) {
	f.t.Helper()
	evs := make([]core.Event, 0, n)
	for i := range n {
		evs = append(evs, core.Event{SessionID: id, TraceID: "tr", SpanID: fmt.Sprintf("%s-%d", instance, i), Service: service, Instance: instance,
			Kind: core.KindCacheRead, EndHLC: uint64(1000 + i), LocalSeq: uint64(i + 1), StartNs: 1, EndNs: 2})
	}
	body, err := tsessionproto.Encode(evs)
	if err != nil {
		panic(err)
	}
	resp, err := deliveryHTTP.Post(f.in.colSrv.URL+"/v1/ingest", tsessionproto.ContentType, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("ingest: %s", resp.Status))
	}
}

// stream follows the active set as an SDK that announces that it reports.
func (f *forgeEnv) stream(service, instance string) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.in.cpSrv.URL+"/v1/active/stream", nil)
	req.Header.Set(core.HeaderService, service)
	req.Header.Set(core.HeaderDeliveryInstance, instance)
	// The stream is one connection for as long as the test runs; it is closed
	// with the test (cancel), and its transport keeps none.
	tr := &http.Transport{DisableKeepAlives: true}
	f.t.Cleanup(tr.CloseIdleConnections)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		panic(err)
	}
	go func() { _, _ = io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
}

// each runs a scenario n times at once: start a session, let do act on it,
// stop it and wait for the seal. It returns the sealed sessions and the
// longest drain.
func (f *forgeEnv) each(n int, do func(id string)) ([]client.Info, time.Duration) {
	f.t.Helper()
	out := make([]client.Info, n)
	var longest time.Duration
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			// Quiet for 0.5s, drained within 6s at most.
			s, err := client.Start(ctx, client.Options{BaseURL: f.in.cpSrv.URL, CollectorURL: f.in.colSrv.URL, Name: "forge", HTTPClient: deliveryHTTP,
				QuiesceSeconds: 5, MaxDrainSeconds: 60, PollInterval: 20 * time.Millisecond})
			if err != nil {
				f.t.Error(err)
				return
			}
			do(s.ID)
			began := time.Now()
			info, err := s.StopAndWait(ctx)
			if err != nil {
				f.t.Error(err)
				return
			}
			mu.Lock()
			out[i], longest = info, max(longest, time.Since(began))
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, longest
}

// honest is what a healthy instance does for a session: ten events at the
// collector and a final report that says so.
func (f *forgeEnv) honest(id string) {
	f.ingest(id, "real", "real-1", 10)
	f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 10, "final": true})
}

// shortfall is an instance that recorded ten events and delivered none, and
// said so in a final report.
func (f *forgeEnv) shortfall(id string) {
	f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 0, "buffered": 10, "final": true})
}

func TestDeliveryForgery(t *testing.T) {
	if testing.Short() {
		t.Skip("runs sessions for seconds")
	}
	n := deliveryRuns()
	all := func(t *testing.T, infos []client.Info, what string, ok func(client.Info) bool) {
		t.Helper()
		for _, info := range infos {
			if info.State != "SEALED" || !ok(info) {
				t.Errorf("%s: state=%s incomplete=%v timed_out=%v reasons=%q unconfirmed=%q", what, info.State, info.Incomplete, info.DrainTimedOut, info.IncompleteReasons, info.DeliveryUnconfirmed)
			}
		}
	}
	complete := func(info client.Info) bool {
		return !info.Incomplete && len(info.IncompleteReasons) == 0 && len(info.DeliveryUnconfirmed) == 0
	}
	incompleteFor := func(part string) func(client.Info) bool {
		return func(info client.Info) bool {
			return info.Incomplete && !info.DrainTimedOut && len(info.IncompleteReasons) == 1 && strings.Contains(info.IncompleteReasons[0], part)
		}
	}
	const healthyDrain = 3 * time.Second // quiet for 0.5s; the grace is 1s

	t.Run("baseline: an honest instance", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		infos, _ := f.each(n, f.honest)
		all(t, infos, "an honest session", complete)
	})

	// (a) A caller opens the stream, announcing that it reports, and never
	// does. It cannot fail a session and cannot hold a drain. Every session
	// names it under delivery_unconfirmed for as long as it stays connected.
	t.Run("a: a stream that announces reports and sends none", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		f.stream("ghost", "ghost-1")
		time.Sleep(300 * time.Millisecond)
		infos, longest := f.each(n, f.honest)
		all(t, infos, "with a silent stream", func(info client.Info) bool {
			return !info.Incomplete && len(info.IncompleteReasons) == 0 && len(info.DeliveryUnconfirmed) == 1 &&
				strings.Contains(info.DeliveryUnconfirmed[0], "service ghost (instance ghost-1): it follows the control plane, but no delivery report of it has arrived since it connected")
		})
		if longest > healthyDrain {
			t.Errorf("the silent stream held a drain for %v", longest)
		}
		// (b) It also posts a report in its own name: it is then an instance
		// like any other that took no part, and is not named.
		infos, _ = f.each(n, func(id string) {
			f.honest(id)
			if code, body := f.report(id, forgedReport{"service": "ghost", "instance": "ghost-1", "recorded": 0, "final": true}); code != http.StatusOK {
				t.Errorf("a report of nothing: %d %s", code, body)
			}
		})
		all(t, infos, "with a stream that reports nothing recorded", complete)
	})

	// The same caller with events at the collector in its name: now it took
	// part, by the collector's count, and its report is waited for. It holds
	// each drain for the grace and the session is incomplete.
	t.Run("a+d: a silent stream with events at the collector in its name", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		f.stream("ghost", "ghost-1")
		infos, longest := f.each(n, func(id string) {
			f.honest(id)
			f.ingest(id, "ghost", "ghost-1", 2)
		})
		all(t, infos, "with a silent stream that has events", incompleteFor("the collector has 2 events from ghost/ghost-1, which did not report how many it recorded"))
		if longest > healthyDrain+time.Second {
			t.Errorf("the drain was held for %v, the grace is 1s", longest)
		}
	})

	// A forged report cannot clear a shortfall: the collector's count for the
	// instance decides, and it has nothing.
	t.Run("a forged report of everything delivered", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		infos, _ := f.each(n, func(id string) {
			f.shortfall(id)
			if code, body := f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 10, "final": true}); code != http.StatusOK || !strings.Contains(body, `"accepted":true`) {
				t.Errorf("the forged report: %d %s", code, body)
			}
		})
		all(t, infos, "a forged report over a real shortfall", incompleteFor("service real (instance real-1): 0 of 10 recorded events reached the collector"))
	})

	// (c) The same, and the caller ingests events of 513 made-up instances
	// that sort before the real one, so that the collector's list of
	// instances is cut before it. The control plane asks about the instance
	// by name. This made a session with a real shortfall complete.
	t.Run("c: a forged report behind a crowded list of instances", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		crowd := func(id string) {
			for k := range 513 {
				f.ingest(id, fmt.Sprintf("a%04d", k), fmt.Sprintf("x%04d", k), 1)
			}
		}
		infos, _ := f.each(n, func(id string) {
			f.shortfall(id)
			crowd(id)
			f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 10, "final": true})
		})
		all(t, infos, "a forged report and a crowded list", incompleteFor("service real (instance real-1): 0 of 10 recorded events reached the collector"))
		// An honest instance behind the same crowd is still complete.
		infos, _ = f.each(n, func(id string) {
			crowd(id)
			f.honest(id)
		})
		all(t, infos, "an honest session with a crowded list", complete)
	})

	// (e) The caller fills the 256 places a session has for reporting
	// instances, so that the real report is not stored (409). It is kept for
	// the verdict all the same and held against the collector's count like a
	// stored one: ten events dropped, and (the third review's c3) a report
	// that is not final and still holds everything, of an instance that is
	// killed before a final one. The first left the session
	// delivery_unconfirmed only, then the second did.
	t.Run("e: the instance cap filled before the real report", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		fill := func(id string) {
			for k := range 256 {
				inst := fmt.Sprintf("f-%03d", k)
				f.ingest(id, "f", inst, 1)
				f.report(id, forgedReport{"service": "f", "instance": inst, "recorded": 1, "delivered": 1, "final": true})
			}
		}
		infos, _ := f.each(n, func(id string) {
			fill(id)
			if code, body := f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 0, "dropped": 10, "final": true}); code != http.StatusConflict {
				t.Errorf("the real report at the cap: %d %s, want 409", code, body)
			}
		})
		all(t, infos, "a real shortfall at the cap", incompleteFor("service real (instance real-1): 0 of 10 recorded events reached the collector, 10 dropped by the SDK"))
		infos, _ = f.each(n, func(id string) {
			fill(id)
			if code, body := f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "buffered": 10}); code != http.StatusConflict {
				t.Errorf("the real report at the cap: %d %s, want 409", code, body)
			}
		})
		all(t, infos, "a report at the cap that is not final and holds everything",
			incompleteFor("service real (instance real-1): 0 of 10 recorded events reached the collector; the instance stopped reporting before the session ended"))
		// An honest instance behind the same crowd is still complete.
		infos, _ = f.each(n, func(id string) {
			fill(id)
			f.ingest(id, "real", "real-1", 10)
			f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 10, "final": true})
		})
		all(t, infos, "an honest instance at the cap", complete)
	})

	// (f) The third review's c1: the caller posts a report in the real
	// instance's name before its own, with one event recorded and dropped. The
	// real reports name more events, so they are later ones and replace it.
	// They used to be stale (their dropped was lower), and a session that lost
	// ten events of a hundred was complete.
	t.Run("f: a report in the instance's name before its own", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		infos, _ := f.each(n, func(id string) {
			f.ingest(id, "real", "real-1", 90)
			f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 1, "dropped": 1, "final": true})
			if code, body := f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 100, "delivered": 90, "buffered": 10, "final": true}); code != http.StatusOK || !strings.Contains(body, `"accepted":true`) {
				t.Errorf("the real report after a forged one that names fewer events: %d %s", code, body)
			}
		})
		all(t, infos, "a forged report before the real ones", incompleteFor("service real (instance real-1): 90 of 100 recorded events reached the collector"))
	})

	// (g) What the counts cannot defend against either, pinned so that the
	// docs stay true (the third review's c2b): an instance took part, half of
	// its events arrived, and its own reports do not arrive at all. The session
	// is incomplete for it. A caller that posts a final report in its name
	// that names what the collector has makes the session complete: the report
	// and the count agree, and nothing says that the instance recorded more.
	t.Run("g: a final report in the name of an instance whose own do not arrive (not defended)", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		f.stream("victim", "v-1")
		infos, _ := f.each(n, func(id string) { f.ingest(id, "victim", "v-1", 5) })
		all(t, infos, "an instance that took part and did not report", incompleteFor("the collector has 5 events from victim/v-1, which did not report how many it recorded"))
		infos, _ = f.each(n, func(id string) {
			f.ingest(id, "victim", "v-1", 5)
			f.report(id, forgedReport{"service": "victim", "instance": "v-1", "recorded": 5, "delivered": 5, "final": true})
		})
		all(t, infos, "the documented limit (a final report forged for an instance whose own do not arrive)", complete)
	})

	// (d) What the counts cannot defend against, pinned so that the docs
	// stay true: the caller ingests events in the instance's own name. The
	// collector then has as many as the instance recorded, and nothing tells
	// the forged events from the real ones. The session is complete.
	t.Run("d: events ingested in the instance's name (not defended)", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		infos, _ := f.each(n, func(id string) {
			f.shortfall(id)
			f.ingest(id, "real", "real-1", 10)
		})
		all(t, infos, "the documented limit (events forged in the instance's name)", complete)
	})

	// A caller can always make a session incomplete: a report in a real
	// instance's name with counters ahead of the real ones (which are then
	// ignored as stale), or of a made-up instance that owes events. It can
	// hold a drain for as long as it keeps reporting progress, up to
	// max_drain. Pinned, not prevented.
	t.Run("a caller can fail a session and hold its drain (not defended)", func(t *testing.T) {
		t.Parallel()
		f := &forgeEnv{t, newInfra(t, store.NewMem())}
		infos, _ := f.each(n, func(id string) {
			f.ingest(id, "real", "real-1", 10)
			f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 1 << 40, "delivered": 1 << 40, "final": true})
			if code, body := f.report(id, forgedReport{"service": "real", "instance": "real-1", "recorded": 10, "delivered": 10, "final": true}); code != http.StatusOK || !strings.Contains(body, `"accepted":false`) {
				t.Errorf("the real report behind a forged one: %d %s", code, body)
			}
		})
		all(t, infos, "counters forged ahead of the real ones", incompleteFor("service real (instance real-1): 10 of 1099511627776 recorded events reached the collector"))

		stop, stopped := make(chan struct{}), make(chan struct{})
		infos, longest := f.each(1, func(id string) {
			f.honest(id)
			go func() {
				defer close(stopped)
				for k := 1; ; k++ {
					body := fmt.Sprintf(`{"service":"h","instance":"h-1","recorded":5,"buffered":5,"delivered_total":%d}`, k)
					if resp, err := deliveryHTTP.Post(f.in.cpSrv.URL+"/v1/sessions/"+id+"/delivery", "application/json", strings.NewReader(body)); err == nil {
						resp.Body.Close()
					}
					select {
					case <-stop:
						return
					case <-time.After(300 * time.Millisecond):
					}
				}
			}()
			time.Sleep(100 * time.Millisecond)
		})
		close(stop)
		<-stopped
		all(t, infos, "a made-up instance that reports progress forever", func(info client.Info) bool { return info.Incomplete && info.DrainTimedOut })
		if longest < 5*time.Second {
			t.Errorf("drain %v: expected it to be held until max_drain (6s)", longest)
		}
	})
}
