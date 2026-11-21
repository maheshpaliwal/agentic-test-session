package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DrainStatus mirrors the collector's drain-status response.
type DrainStatus struct {
	Pending int `json:"pending"`
	// LastEventUnixNs and ReceivedUnixNs are collector-side values. The control
	// plane never compares them with its own clock; it only detects change.
	LastEventUnixNs int64 `json:"last_event_unix_ns"`
	ReceivedUnixNs  int64 `json:"received_unix_ns,omitempty"`
	EventCount      int64 `json:"event_count"`
	DroppedEvents   int64 `json:"dropped_events"`
	// Instances is how many events of the session the collector has from
	// every SDK instance, stored or still in its reorder window; the delivery
	// reports are held against it (see deliveryVerdict). Nil from a collector
	// that does not send it. With InstancesTruncated the list is not complete:
	// an instance it does not name may have sent events, and the collector is
	// asked about the ones that matter by name (see InstanceCounter).
	Instances          []InstanceCount `json:"instances"`
	InstancesTruncated bool            `json:"instances_truncated,omitempty"`
}

// InstanceCount is one entry of DrainStatus.Instances.
type InstanceCount struct {
	Service  string `json:"service,omitempty"`
	Instance string `json:"instance"`
	Events   int64  `json:"events"`
}

// SealResult mirrors the collector's seal response.
type SealResult struct {
	Incomplete bool     `json:"incomplete"`
	Orphaned   []string `json:"orphaned"`
	BundleURI  string   `json:"bundle_uri"`
}

// Collector is the control plane's view of the collector internal API.
type Collector interface {
	DrainStatus(ctx context.Context, id string) (DrainStatus, error)
	Seal(ctx context.Context, id string) (SealResult, error)
	// Delete removes everything the collector recorded for the session. It
	// must be idempotent: deleting an unknown session succeeds.
	Delete(ctx context.Context, id string) error
}

// HTTPCollector talks to a collector over HTTP.
type HTTPCollector struct {
	BaseURL string
	// Client is the client to use; nil for one that keeps its connections
	// (see NewCollectorClient).
	Client *http.Client
}

// NewCollectorClient returns an HTTP client for the control plane's requests
// to the collector. A drain asks the collector about its session several
// times a second, and hundreds of sessions drain at once when a test run
// ends. http.DefaultTransport keeps two idle connections per host, so most of
// those requests would open a connection of their own: a thousand a second,
// and the machine runs out of local ports. This one keeps as many as it used.
func NewCollectorClient(timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns, t.MaxIdleConnsPerHost = 1024, 1024
	return &http.Client{Timeout: timeout, Transport: t}
}

var defaultCollectorClient = NewCollectorClient(30 * time.Second)

// do calls /v1/internal/sessions/{id}[/suffix] and decodes the JSON reply
// into out (skipped when out is nil). in, when not nil, is sent as JSON.
func (c *HTTPCollector) do(ctx context.Context, method, id, suffix string, in, out any) error {
	cl := c.Client
	if cl == nil {
		cl = defaultCollectorClient
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/v1/internal/sessions/" + url.PathEscape(id)
	if suffix != "" {
		u += "/" + suffix
	}
	var body io.Reader
	if method == http.MethodPost {
		var b []byte
		if in != nil {
			var err error
			if b, err = json.Marshal(in); err != nil {
				return err
			}
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("collector %s %s: %s: %s", method, suffix, resp.Status, bytes.TrimSpace(b))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)) // let the connection be reused
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Delete implements Collector.
func (c *HTTPCollector) Delete(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, id, "", nil, nil)
}

// DrainStatus implements Collector.
func (c *HTTPCollector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	var d DrainStatus
	err := c.do(ctx, http.MethodGet, id, "drain-status", nil, &d)
	return d, err
}

// Seal implements Collector.
func (c *HTTPCollector) Seal(ctx context.Context, id string) (SealResult, error) {
	var r SealResult
	err := c.do(ctx, http.MethodPost, id, "seal", nil, &r)
	return r, err
}

// InstanceCounts implements InstanceCounter. A collector without the
// endpoint answers 404, which is an error like any other: the counts stay
// unknown.
func (c *HTTPCollector) InstanceCounts(ctx context.Context, id string, instances []InstanceCount) ([]InstanceCount, error) {
	var out struct {
		Instances []InstanceCount `json:"instances"`
	}
	err := c.do(ctx, http.MethodPost, id, "instance-counts", map[string][]InstanceCount{"instances": instances}, &out)
	return out.Instances, err
}
