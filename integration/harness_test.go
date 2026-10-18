package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"tsession/collector"
	"tsession/collector/store"
	"tsession/controlplane"
	"tsession/core"
	"tsession/integration/app"
	"tsession/sdk/client"
	"tsession/sdk/runtime"
)

func itEnv(t *testing.T) (pgDSN, redisAddr string) {
	pgDSN, redisAddr = os.Getenv("TSESSION_IT_PG_DSN"), os.Getenv("TSESSION_IT_REDIS_ADDR")
	if pgDSN == "" || redisAddr == "" {
		t.Skip("TSESSION_IT_PG_DSN / TSESSION_IT_REDIS_ADDR not set (use scripts/it.sh)")
	}
	return
}

var plain = &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{}}

// infra is a real collector + control plane on 127.0.0.1 ephemeral ports.
type infra struct {
	col    *collector.Collector
	colSrv *httptest.Server
	cp     *controlplane.Manager
	cpSrv  *httptest.Server
}

func withParam(dsn, k, v string) string {
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set(k, v)
	u.RawQuery = q.Encode()
	return u.String()
}

func newInfra(t *testing.T, st store.Store) *infra {
	t.Helper()
	col := collector.New(st, collector.Config{Window: 100 * time.Millisecond})
	colSrv := httptest.NewServer(col.Handler())
	mgr := controlplane.NewManager(controlplane.Config{
		Collector: &controlplane.HTTPCollector{BaseURL: colSrv.URL},
		TimeUnit:  100 * time.Millisecond, PollInterval: 100 * time.Millisecond,
		HeartbeatPeriod: time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Run(ctx)
	cpSrv := httptest.NewServer(mgr.Handler())
	in := &infra{col: col, colSrv: colSrv, cp: mgr, cpSrv: cpSrv}
	t.Cleanup(func() { cancel(); cpSrv.Close(); mgr.Close(); colSrv.Close(); col.Close() })
	return in
}

func (in *infra) startRuntime(t *testing.T, service string) {
	t.Helper()
	if err := runtime.Start(runtime.Config{Service: service, ControlPlaneURL: in.cpSrv.URL,
		CollectorURL: in.colSrv.URL, PatchDefaultTransport: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Stop)
}

// session options: quiesce 8 units * 100ms = 0.8s, max drain 10s.
func (in *infra) start(t *testing.T, mode string) *client.Session {
	t.Helper()
	s, err := client.Start(context.Background(), client.Options{BaseURL: in.cpSrv.URL, Mode: mode, Name: "it",
		TTLSeconds: 600, QuiesceSeconds: 8, MaxDrainSeconds: 100, PollInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	return s
}

// waitActive blocks until this process' SDK has seen the session in the active set.
func waitActive(t *testing.T, id string) {
	t.Helper()
	waitFor(t, 5*time.Second, "sdk sees session active", func() bool {
		_, sp := core.Begin(core.ContextWith(context.Background(), core.SpanContext{SessionID: id, TraceID: core.NewTraceID()}), core.KindAPIIn, "probe")
		return sp != nil // never ended => never recorded
	})
}

func waitFor(t *testing.T, d time.Duration, what string, f func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func (in *infra) seal(t *testing.T, s *client.Session) client.Info {
	t.Helper()
	runtime.Flush(3 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := s.StopAndWait(ctx)
	if err != nil {
		t.Fatalf("stop/wait: %v (info=%+v)", err, info)
	}
	return info
}

// ---- collector read API ----

type colAPI struct{ base string }

func (c colAPI) get(t *testing.T, path string, out any) {
	t.Helper()
	resp, err := plain.Get(c.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", path, resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("GET %s: %v: %s", path, err, b)
	}
}

type eventsResp struct {
	Events  []core.Event `json:"events"`
	NextSeq uint64       `json:"next_seq"`
	Sealed  bool         `json:"sealed"`
}

func (c colAPI) events(t *testing.T, id string) []core.Event {
	var r eventsResp
	c.get(t, "/v1/sessions/"+id+"/events?from_seq=1&limit=10000", &r)
	return r.Events
}

func (c colAPI) trace(t *testing.T, id string) collector.Trace {
	var r collector.Trace
	c.get(t, "/v1/sessions/"+id+"/trace", &r)
	return r
}

func (c colAPI) summary(t *testing.T, id string) collector.Summary {
	var r collector.Summary
	c.get(t, "/v1/sessions/"+id+"/summary", &r)
	return r
}

func (c colAPI) history(t *testing.T, id, st, entity, pk string) collector.EntityHistory {
	var r collector.EntityHistory
	c.get(t, "/v1/sessions/"+id+"/entities/"+st+"/"+entity+"/"+pk, &r)
	return r
}

func (c colAPI) sessions(t *testing.T) []store.SessionInfo {
	var r []store.SessionInfo
	c.get(t, "/v1/sessions", &r)
	return r
}

// ---- event helpers ----

func ofKind(evs []core.Event, k core.Kind) (out []core.Event) {
	for _, e := range evs {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return
}

func named(evs []core.Event, k core.Kind, sub string) (out []core.Event) {
	for _, e := range ofKind(evs, k) {
		if strings.Contains(e.Name, sub) {
			out = append(out, e)
		}
	}
	return
}

func inline(p core.PayloadRef) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(p.Inline, &m)
	return m
}

func anyContains(evs []core.Event, sub string) bool {
	for _, e := range evs {
		if b, _ := json.Marshal(e); bytes.Contains(b, []byte(sub)) {
			return true
		}
	}
	return false
}

// ---- app driving ----

type orderBody struct {
	SKU      string   `json:"sku"`
	Qty      int      `json:"qty"`
	Customer string   `json:"customer"`
	Password string   `json:"password,omitempty"`
	Jobs     []string `json:"jobs,omitempty"`
}

func doReq(t *testing.T, method, u string, body any, hdr http.Header) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, u, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := plain.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

type appEnv struct {
	a    *app.App
	base string
	rdb  *redis.Client
	ns   string
}

// newApp builds the example app (needs the runtime started first so the
// wrappers are active) on a loopback listener.
func newApp(t *testing.T, appDSN, redisAddr, ns string) *appEnv {
	t.Helper()
	a, err := app.New(app.Config{PGDSN: appDSN, RedisAddr: redisAddr, Namespace: ns, Workers: 3})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(func() { srv.Close(); a.Close() })
	return &appEnv{a: a, base: srv.URL, ns: ns, rdb: redis.NewClient(&redis.Options{Addr: redisAddr})}
}

func (e *appEnv) createOrder(t *testing.T, h http.Header, b orderBody) (int, app.Order) {
	code, raw := doReq(t, "POST", e.base+"/orders", b, h)
	var o app.Order
	_ = json.Unmarshal(raw, &o)
	if code != 201 {
		t.Logf("POST /orders -> %d %s", code, raw)
	}
	return code, o
}

// schemaSetup creates schemas in the throwaway PG and returns DSNs + cleanup.
func schemaSetup(t *testing.T, dsn string, names ...string) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, n := range names {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+n+" CASCADE"); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "CREATE SCHEMA "+n); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return
		}
		defer p.Close()
		for _, n := range names {
			_, _ = p.Exec(ctx, "DROP SCHEMA IF EXISTS "+n+" CASCADE")
		}
	})
}

func delNS(t *testing.T, addr, ns string) {
	t.Cleanup(func() {
		r := redis.NewClient(&redis.Options{Addr: addr})
		defer r.Close()
		ctx := context.Background()
		keys, _ := r.Keys(ctx, ns+"*").Result()
		keys2, _ := r.Keys(ctx, "ts:ctx:*").Result()
		if len(keys)+len(keys2) > 0 {
			r.Del(ctx, append(keys, keys2...)...)
		}
	})
}

func fmtSeq(evs []core.Event) string {
	var sb strings.Builder
	for _, e := range evs {
		fmt.Fprintf(&sb, "%d %s %q att=%d err=%q\n", e.Seq, e.Kind, e.Name, e.Attempt, e.Err)
	}
	return sb.String()
}
