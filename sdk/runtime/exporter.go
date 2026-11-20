package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// ExporterConfig tunes batching and retry. Zero values get defaults.
type ExporterConfig struct {
	CollectorURL  string
	BatchSize     int           // events per POST (default 256)
	FlushInterval time.Duration // default 100ms
	MaxRetries    int           // extra attempts per batch in one pass (default 2)
	RetryBackoff  time.Duration // first backoff, doubles (default 100ms)
	MaxCooldown   time.Duration // cap on the pause between passes while the collector is failing (default 5s)
	HTTPTimeout   time.Duration // per request (default 2s)
	Encoder       BatchEncoder  // wire format (default ProtobufEncoder)
}

// Exporter ships batches from a Recorder to the collector from a single
// background goroutine. A batch leaves the ring only once the collector
// accepted it or refused it for good; while the collector is unreachable or
// answers 5xx/429 the batch stays buffered and is retried with backoff, so
// events are lost only when the ring overflows.
type Exporter struct {
	cfg    ExporterConfig
	rec    *Recorder
	client *http.Client

	flushReq chan chan bool
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	// ctx is cancelled by Close so a request in flight to an unresponsive
	// collector is abandoned instead of holding up shutdown.
	ctx    context.Context
	cancel context.CancelFunc

	failures int
	nextTry  time.Time
	// down and refusalLogged make the exporter say once when delivery stops,
	// once when it resumes, and once when the collector refuses events.
	down          bool
	outageLogged  bool      // the current outage was logged, so its recovery is too
	lastOutageLog time.Time // when an outage was last logged
	refusalLogged bool

	// pending is the unresolved rest of the batch being exported: the first
	// pendingRing events are still at the front of the ring, the others are
	// sdk.dropped events taken from the recorder. It is resent unchanged (same
	// local_seq, so the collector deduplicates) until it is resolved. Only the
	// export goroutine touches these.
	pending     []core.Event
	pendingRing int
}

// NewExporter starts the export goroutine.
func NewExporter(rec *Recorder, cfg ExporterConfig) *Exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	} else if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 2
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.MaxCooldown <= 0 {
		cfg.MaxCooldown = 5 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 2 * time.Second
	}
	if cfg.Encoder == nil {
		cfg.Encoder = ProtobufEncoder{}
	}
	cfg.CollectorURL = strings.TrimRight(cfg.CollectorURL, "/")
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exporter{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		rec:      rec,
		client:   &http.Client{Timeout: cfg.HTTPTimeout, Transport: newPlainTransport()},
		flushReq: make(chan chan bool),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go e.loop()
	return e
}

func newPlainTransport() *http.Transport {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return &http.Transport{}
}

func (e *Exporter) loop() {
	defer close(e.done)
	t := time.NewTicker(e.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-t.C:
			if time.Now().After(e.nextTry) {
				e.drain()
			}
		case <-e.rec.wake:
			if time.Now().After(e.nextTry) {
				e.drain()
			}
		case ack := <-e.flushReq:
			ack <- e.drain() // buffered: never blocks on a caller that gave up
		}
	}
}

// drain exports batches until nothing is left to send (true) or the collector
// fails transiently (false). In the second case the unsent events stay in the
// ring and the next pass is delayed by a growing cooldown.
func (e *Exporter) drain() bool {
	for {
		if len(e.pending) == 0 {
			e.pending = e.rec.Peek(e.cfg.BatchSize)
			e.pendingRing = len(e.pending)
			e.pending = append(e.pending, e.rec.TakeDrops()...)
			if len(e.pending) == 0 {
				return true
			}
		}
		hadRing := e.pendingRing > 0
		done, lost, ok := e.send(e.pending)
		ring := min(done, e.pendingRing)
		e.rec.Commit(ring)
		e.pendingRing -= ring
		e.rec.AddDrops(lost)
		e.pending = e.pending[done:]
		if len(e.pending) == 0 {
			e.pending = nil // release the batch
		}
		// A refused batch that held only sdk.dropped events had its counts
		// returned to the recorder, which would hand them straight back: pause
		// like for a transient failure instead of spinning.
		refusedDrops := ok && !hadRing && len(lost) > 0
		if ok && len(lost) > 0 && !e.refusalLogged {
			e.refusalLogged = true
			log.Printf("tsession: the collector at %s refused events and they were dropped; check that TSESSION_COLLECTOR_URL points at a tsession collector of a matching version", DisplayURL(e.cfg.CollectorURL))
		}
		if !ok || refusedDrops {
			if !ok && !e.down {
				// A wrong collector URL otherwise shows up only as an empty
				// session. At most one outage (and its recovery) is logged
				// per outageLogGap, so a flapping collector cannot flood the
				// host's log.
				e.down = true
				if now := time.Now(); e.lastOutageLog.IsZero() || now.Sub(e.lastOutageLog) >= outageLogGap {
					e.lastOutageLog, e.outageLogged = now, true
					log.Printf("tsession: cannot deliver events to the collector at %s; they stay buffered and are retried, and are dropped when the buffer fills", DisplayURL(e.cfg.CollectorURL))
				}
			}
			e.failures++
			cd := e.cfg.FlushInterval << min(e.failures, 6)
			e.nextTry = time.Now().Add(min(cd, e.cfg.MaxCooldown))
			return ok
		}
		if e.down {
			e.down = false
			if e.outageLogged {
				e.outageLogged = false
				log.Printf("tsession: delivering events to the collector at %s again", DisplayURL(e.cfg.CollectorURL))
			}
		}
		e.failures = 0
		e.nextTry = time.Time{}
	}
}

// outageLogGap is the least time between two logged outages.
const outageLogGap = time.Minute

// DisplayURL returns raw in a form that is safe to log: without userinfo,
// query or fragment, any of which can carry a credential.
func DisplayURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparseable URL>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

type postResult int

const (
	postOK        postResult = iota
	postTooLarge             // 413: bisect
	postPermanent            // refused for good (4xx, encode error): drop
	postTransient            // unreachable, 5xx, 429: keep and retry later
)

// send ships batch in order. done is the length of the prefix that is
// resolved: delivered, or refused for good and returned in lost (to be counted
// as drops). ok is false when a transient failure stopped it; batch[done:] is
// then still to be sent. A 413 bisects the batch so one oversize event only
// drops itself.
func (e *Exporter) send(batch []core.Event) (done int, lost []core.Event, ok bool) {
	switch e.post(batch) {
	case postOK:
		return len(batch), nil, true
	case postTooLarge:
		if len(batch) == 1 {
			switch e.post([]core.Event{oversizeStub(batch[0])}) {
			case postOK:
				return 1, nil, true
			case postTransient:
				return 0, nil, false
			}
			return 1, batch, true
		}
		mid := len(batch) / 2
		done, lost, ok = e.send(batch[:mid])
		if !ok {
			return done, lost, false
		}
		d2, l2, ok := e.send(batch[mid:])
		// lost may alias batch: cap it so the append cannot write into batch.
		return done + d2, append(lost[:len(lost):len(lost)], l2...), ok
	case postPermanent:
		return len(batch), batch, true
	default:
		return 0, nil, false
	}
}

// oversizeStub replaces an event the collector refuses as too large with a
// minimal one that keeps its identity (so ordering/idempotency still hold).
func oversizeStub(ev core.Event) core.Event {
	return core.Event{
		SessionID: ev.SessionID, TraceID: ev.TraceID, SpanID: ev.SpanID, ParentID: ev.ParentID,
		Service: ev.Service, Instance: ev.Instance, Kind: ev.Kind, Name: ev.Name,
		HLC: ev.HLC, EndHLC: ev.EndHLC, LocalSeq: ev.LocalSeq,
		StartNs: ev.StartNs, EndNs: ev.EndNs, TxID: ev.TxID, Attempt: ev.Attempt,
		Err:   "event exceeded collector size limit; payload dropped",
		Flags: []string{"oversize_dropped"},
	}
}

func (e *Exporter) post(batch []core.Event) postResult {
	body, err := e.cfg.Encoder.Encode(batch)
	if err != nil {
		return postPermanent // encoding is deterministic; retrying will not help
	}
	inst := core.GetConfig().Instance
	backoff := e.cfg.RetryBackoff
	for attempt := 0; attempt <= e.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
			case <-e.stop:
				return postTransient
			}
			backoff *= 2
		}
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, e.cfg.CollectorURL+"/v1/ingest", bytes.NewReader(body))
		if err != nil {
			return postPermanent // malformed collector URL; retrying will not help
		}
		req.Header.Set("Content-Type", e.cfg.Encoder.ContentType())
		req.Header.Set("X-TSession-Instance", inst)
		resp, err := e.client.Do(req)
		if err != nil {
			continue
		}
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		switch {
		case resp.StatusCode/100 == 2:
			var ir struct {
				Rejected int `json:"rejected"`
			}
			if json.Unmarshal(rb, &ir) == nil && ir.Rejected > 0 {
				// Not counted for batches made only of sdk.dropped events, or a
				// collector that rejects them would feed itself forever.
				for i := range batch {
					if batch[i].Kind != core.KindSDKDropped {
						e.rec.AddDropCount(batch[i].SessionID, uint64(ir.Rejected))
						break
					}
				}
			}
			return postOK
		case resp.StatusCode == http.StatusRequestEntityTooLarge:
			return postTooLarge
		case resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests:
			return postPermanent
		}
	}
	return postTransient
}

// Flush exports everything buffered and reports whether the ring was emptied
// within timeout. While the collector is failing it keeps trying, pausing
// RetryBackoff between passes, and returns false when the time is up with
// events still buffered (they stay buffered for the background loop).
func (e *Exporter) Flush(timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		ack := make(chan bool, 1)
		select {
		case e.flushReq <- ack:
		case <-e.done:
			return false
		case <-deadline.C:
			return false
		}
		select {
		case ok := <-ack:
			if ok {
				return true
			}
		case <-deadline.C:
			return false
		}
		select {
		case <-time.After(e.cfg.RetryBackoff):
		case <-e.done:
			return false
		case <-deadline.C:
			return false
		}
	}
}

// Close flushes (bounded by timeout) and stops the goroutine. Events the
// collector did not accept by then are discarded with the exporter.
func (e *Exporter) Close(timeout time.Duration) {
	e.once.Do(func() {
		e.Flush(timeout)
		close(e.stop)
		e.cancel()
		<-e.done
	})
}
