package runtime

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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
	MaxRetries    int           // extra attempts per batch (default 2)
	RetryBackoff  time.Duration // first backoff, doubles (default 100ms)
	MaxCooldown   time.Duration // pause after a failed batch, capped (default 5s)
	HTTPTimeout   time.Duration // per request (default 2s)
	Encoder       BatchEncoder  // wire format (default NDJSON)
}

// Exporter drains a Recorder and ships batches to the collector from a single
// background goroutine.
type Exporter struct {
	cfg    ExporterConfig
	rec    *Recorder
	client *http.Client

	flushReq chan chan struct{}
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once

	failures int
	nextTry  time.Time
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
		cfg.Encoder = NDJSONEncoder{}
	}
	cfg.CollectorURL = strings.TrimRight(cfg.CollectorURL, "/")
	e := &Exporter{
		cfg:      cfg,
		rec:      rec,
		client:   &http.Client{Timeout: cfg.HTTPTimeout, Transport: newPlainTransport()},
		flushReq: make(chan chan struct{}),
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
			e.drain()
			close(ack)
		}
	}
}

// drain sends batches until the ring is empty or a batch fails.
func (e *Exporter) drain() {
	for {
		batch := e.rec.Drain(e.cfg.BatchSize)
		if len(batch) == 0 {
			return
		}
		if failed := e.send(batch); len(failed) > 0 {
			e.rec.AddDrops(failed)
			e.failures++
			cd := e.cfg.FlushInterval << min(e.failures, 6)
			e.nextTry = time.Now().Add(min(cd, e.cfg.MaxCooldown))
			return
		}
		e.failures = 0
		e.nextTry = time.Time{}
	}
}

type postResult int

const (
	postOK postResult = iota
	postTooLarge
	postFailed
)

// send ships batch and returns the events that were lost (to be counted as
// drops). A 413 bisects the batch so one oversize event only drops itself.
func (e *Exporter) send(batch []core.Event) []core.Event {
	switch e.post(batch) {
	case postOK:
		return nil
	case postTooLarge:
		if len(batch) == 1 {
			stub := oversizeStub(batch[0])
			if e.post([]core.Event{stub}) == postOK {
				return nil
			}
			return batch
		}
		mid := len(batch) / 2
		return append(e.send(batch[:mid]), e.send(batch[mid:])...)
	default:
		return batch
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
		return postFailed // encoding is deterministic; retrying will not help
	}
	inst := core.GetConfig().Instance
	backoff := e.cfg.RetryBackoff
	for attempt := 0; attempt <= e.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff):
			case <-e.stop:
				return postFailed
			}
			backoff *= 2
		}
		req, err := http.NewRequest(http.MethodPost, e.cfg.CollectorURL+"/v1/ingest", bytes.NewReader(body))
		if err != nil {
			return postFailed
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
			return postFailed // permanent; retrying will not help
		}
	}
	return postFailed
}

// Flush asks the exporter to drain now and waits up to timeout.
func (e *Exporter) Flush(timeout time.Duration) bool {
	ack := make(chan struct{})
	select {
	case e.flushReq <- ack:
	case <-e.done:
		return false
	case <-time.After(timeout):
		return false
	}
	select {
	case <-ack:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Close flushes (bounded by timeout) and stops the goroutine.
func (e *Exporter) Close(timeout time.Duration) {
	e.once.Do(func() {
		e.Flush(timeout)
		close(e.stop)
		<-e.done
	})
}
