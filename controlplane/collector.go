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
	// an instance it does not name may have sent events.
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
	Client  *http.Client
}

// do calls /v1/internal/sessions/{id}[/suffix] and decodes the JSON reply
// into out (skipped when out is nil).
func (c *HTTPCollector) do(ctx context.Context, method, id, suffix string, out any) error {
	cl := c.Client
	if cl == nil {
		cl = http.DefaultClient
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/v1/internal/sessions/" + url.PathEscape(id)
	if suffix != "" {
		u += "/" + suffix
	}
	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
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
	return c.do(ctx, http.MethodDelete, id, "", nil)
}

// DrainStatus implements Collector.
func (c *HTTPCollector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	var d DrainStatus
	err := c.do(ctx, http.MethodGet, id, "drain-status", &d)
	return d, err
}

// Seal implements Collector.
func (c *HTTPCollector) Seal(ctx context.Context, id string) (SealResult, error) {
	var r SealResult
	err := c.do(ctx, http.MethodPost, id, "seal", &r)
	return r, err
}
