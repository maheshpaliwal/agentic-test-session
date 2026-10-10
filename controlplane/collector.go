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
	Pending         int   `json:"pending"`
	LastEventUnixNs int64 `json:"last_event_unix_ns"`
	EventCount      int64 `json:"event_count"`
	DroppedEvents   int64 `json:"dropped_events"`
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
}

// HTTPCollector talks to a collector over HTTP.
type HTTPCollector struct {
	BaseURL string
	Client  *http.Client
}

func (c *HTTPCollector) do(ctx context.Context, method, id, suffix string, out any) error {
	cl := c.Client
	if cl == nil {
		cl = http.DefaultClient
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/v1/internal/sessions/" + url.PathEscape(id) + "/" + suffix
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
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *HTTPCollector) DrainStatus(ctx context.Context, id string) (DrainStatus, error) {
	var d DrainStatus
	err := c.do(ctx, http.MethodGet, id, "drain-status", &d)
	return d, err
}

func (c *HTTPCollector) Seal(ctx context.Context, id string) (SealResult, error) {
	var r SealResult
	err := c.do(ctx, http.MethodPost, id, "seal", &r)
	return r, err
}
