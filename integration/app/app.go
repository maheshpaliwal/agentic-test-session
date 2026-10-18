// Package app is a small, realistic service used by the end-to-end tests. It
// adopts tsession exactly as a service owner would: thttp.Handler on the mux,
// tsql.Open, tredis.Wrap, and tqueue over a Redis-list job queue with a Redis
// side channel. Synthetic data only.
package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"tsession/contrib/tqueue"
	"tsession/contrib/tredis"
	"tsession/contrib/tsql"
	"tsession/thttp"
)

const (
	JobQueue = "jobs"
	DLQ      = "jobs:dlq"
)

// Config is env/flag driven by cmd/exampleapp; defaults are loopback.
type Config struct {
	PGDSN         string
	RedisAddr     string
	DownstreamURL string // empty => start an in-process downstream on 127.0.0.1:0
	Workers       int
	Namespace     string // redis key prefix, isolates runs
}

type App struct {
	cfg  Config
	db   *sql.DB
	rdb  redis.UniversalClient
	side tqueue.SideChannel
	hc   *http.Client

	downSrv *http.Server
	downURL string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Visible to tests.
	DLQCount func() int64
}

// New connects, creates the schema + seed data, and starts the worker pool.
func New(cfg Config) (*App, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 3
	}
	a := &App{cfg: cfg}
	var err error
	if a.db, err = tsql.Open("pgx", cfg.PGDSN, tsql.WithStore("postgres")); err != nil {
		return nil, err
	}
	a.rdb = tredis.Wrap(redis.NewClient(&redis.Options{Addr: cfg.RedisAddr}), tredis.WithBeforeImage(a.key("order:*")))
	a.side = tqueue.RedisSideChannel{Client: goRedisAdapter{a.rdb}}
	a.hc = thttp.Client(&http.Client{Timeout: 5 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	if err := a.migrate(ctx); err != nil {
		cancel()
		return nil, err
	}
	a.DLQCount = func() int64 { n, _ := a.rdb.LLen(context.Background(), a.key(DLQ)).Result(); return n }

	a.downURL = cfg.DownstreamURL
	if a.downURL == "" {
		if err := a.startDownstream(); err != nil {
			cancel()
			return nil, err
		}
	}
	for i := 0; i < cfg.Workers; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
	return a, nil
}

func (a *App) key(k string) string { return a.cfg.Namespace + k }

func (a *App) Close() {
	a.cancel()
	a.wg.Wait()
	if a.downSrv != nil {
		_ = a.downSrv.Close()
	}
	_ = a.db.Close()
	_ = a.rdb.Close()
}

func (a *App) DownstreamURL() string { return a.downURL }

func (a *App) migrate(ctx context.Context) error {
	// Schema/seed run before any session can exist; they are not traced unless
	// a session is active (they carry no session context).
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS inventory (id serial PRIMARY KEY, sku text UNIQUE NOT NULL, stock int NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, sku text NOT NULL, qty int NOT NULL,
			customer text NOT NULL, status text NOT NULL DEFAULT 'new', created_at timestamptz NOT NULL DEFAULT now())`,
		`INSERT INTO inventory (sku, stock) VALUES ('widget', 100), ('gadget', 50) ON CONFLICT (sku) DO NOTHING`,
	} {
		if _, err := a.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// ---- HTTP ----

type orderReq struct {
	SKU      string   `json:"sku"`
	Qty      int      `json:"qty"`
	Customer string   `json:"customer"`
	Password string   `json:"password,omitempty"`
	Jobs     []string `json:"jobs,omitempty"` // extra jobs: flaky_job, poison_job
}

type Order struct {
	ID       int64  `json:"id"`
	SKU      string `json:"sku"`
	Qty      int    `json:"qty"`
	Customer string `json:"customer"`
	Status   string `json:"status"`
}

// Handler returns the traced HTTP handler.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("POST /orders/fail", a.failOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "pong") })
	return thttp.Handler(mux)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *App) createOrder(w http.ResponseWriter, r *http.Request) {
	var req orderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Qty <= 0 || req.SKU == "" {
		writeJSON(w, 400, map[string]string{"error": "bad request"})
		return
	}
	ctx := r.Context()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()
	var stock int
	if err := tx.QueryRowContext(ctx, `SELECT stock FROM inventory WHERE sku = $1`, req.SKU).Scan(&stock); err != nil {
		writeJSON(w, 404, map[string]string{"error": "unknown sku"})
		return
	}
	if stock < req.Qty {
		writeJSON(w, 409, map[string]string{"error": "insufficient stock"})
		return
	}
	o := Order{SKU: req.SKU, Qty: req.Qty, Customer: req.Customer, Status: "new"}
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO orders (sku, qty, customer, status) VALUES ($1, $2, $3, $4) RETURNING id`,
		o.SKU, o.Qty, o.Customer, o.Status).Scan(&o.ID); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory SET stock = stock - $1 WHERE sku = $2`, o.Qty, o.SKU); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if err := tx.Commit(); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	b, _ := json.Marshal(o)
	if err := a.rdb.Set(ctx, a.key("order:"+strconv.FormatInt(o.ID, 10)), b, 10*time.Minute).Err(); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	jobs := append([]string{"send_receipt"}, req.Jobs...)
	for _, j := range jobs {
		if err := a.publish(ctx, j, map[string]any{"order_id": o.ID, "customer": o.Customer, "job": j}); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
	}
	writeJSON(w, 201, o)
}

func (a *App) failOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	var id int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO orders (sku, qty, customer, status) VALUES ('widget', 1, 'rollback-user', 'new') RETURNING id`).Scan(&id); err != nil {
		_ = tx.Rollback()
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	_ = tx.Rollback()
	writeJSON(w, 422, map[string]any{"error": "order rolled back", "id": id})
}

func (a *App) getOrder(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), r.PathValue("id")
	if v, err := a.rdb.Get(ctx, a.key("order:"+id)).Result(); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "hit")
		io.WriteString(w, v)
		return
	}
	var o Order
	err := a.db.QueryRowContext(ctx, `SELECT id, sku, qty, customer, status FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.SKU, &o.Qty, &o.Customer, &o.Status)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	} else if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, o)
}

// ---- downstream "receipt printer" ----

func (a *App) startDownstream() error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /receipts", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeJSON(w, 200, map[string]string{"receipt": "printed"})
	})
	ln, err := listenLoopback()
	if err != nil {
		return err
	}
	a.downSrv = &http.Server{Handler: thttp.Handler(mux), ReadHeaderTimeout: 5 * time.Second}
	a.downURL = "http://" + ln.Addr().String()
	go a.downSrv.Serve(ln)
	return nil
}

// ---- queue ----

type jobMsg struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Params  map[string]any `json:"params"`
	Attempt int            `json:"attempt"`
}

var jobSeq struct {
	sync.Mutex
	n int
}

func newJobID() string {
	jobSeq.Lock()
	defer jobSeq.Unlock()
	jobSeq.n++
	return fmt.Sprintf("job-%d-%d", time.Now().UnixNano(), jobSeq.n)
}

func maxAttempts(job string) int {
	switch job {
	case "flaky_job":
		return 3
	case "poison_job":
		return 2
	}
	return 1
}

func (a *App) publish(ctx context.Context, name string, params map[string]any) error {
	m := jobMsg{ID: newJobID(), Name: name, Params: params, Attempt: 1}
	return tqueue.PublishVia(ctx, a.side, m.ID, tqueue.Message{System: "redis-list", Queue: JobQueue, JobName: name, Params: params},
		func(c context.Context) error { return a.push(c, JobQueue, m) })
}

func (a *App) push(ctx context.Context, q string, m jobMsg) error {
	b, _ := json.Marshal(m)
	return a.rdb.LPush(ctx, a.key(q), b).Err()
}

func (a *App) worker(ctx context.Context) {
	defer a.wg.Done()
	for ctx.Err() == nil {
		res, err := a.rdb.BRPop(ctx, time.Second, a.key(JobQueue)).Result()
		if err != nil || len(res) < 2 {
			continue
		}
		var m jobMsg
		if json.Unmarshal([]byte(res[1]), &m) != nil {
			continue
		}
		a.handle(ctx, m)
	}
}

func (a *App) handle(ctx context.Context, m jobMsg) {
	d := tqueue.Delivery{System: "redis-list", Queue: JobQueue, JobName: m.Name, Params: m.Params,
		Attempt: m.Attempt, MessageID: m.ID, MaxAttempts: maxAttempts(m.Name)}
	err := tqueue.ConsumeVia(ctx, a.side, d, func(c context.Context) error { return a.run(c, m) })
	if err == nil {
		return
	}
	bg := context.Background()
	if m.Attempt < d.MaxAttempts {
		m.Attempt++
		_ = a.push(bg, JobQueue, m)
	} else {
		_ = a.push(bg, DLQ, m)
	}
}

func (a *App) run(ctx context.Context, m jobMsg) error {
	oid, _ := m.Params["order_id"].(float64)
	switch m.Name {
	case "send_receipt":
		if _, err := a.db.ExecContext(ctx, `UPDATE orders SET status = $1 WHERE id = $2`, "receipt_sent", int64(oid)); err != nil {
			return err
		}
		a.refreshCache(ctx, int64(oid), "receipt_sent") // best effort; gives the cache key a before/after image
		return a.callDownstream(ctx, int64(oid))
	case "flaky_job":
		if m.Attempt <= 2 {
			return fmt.Errorf("transient failure (attempt %d)", m.Attempt)
		}
		_, err := a.db.ExecContext(ctx, `UPDATE orders SET status = $1 WHERE id = $2`, "flaky_done", int64(oid))
		return err
	case "poison_job":
		return errors.New("poison pill: cannot process")
	}
	return fmt.Errorf("unknown job %q", m.Name)
}

func (a *App) callDownstream(ctx context.Context, orderID int64) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, a.downURL+"/receipts",
		strings.NewReader(fmt.Sprintf(`{"order_id":%d}`, orderID)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("downstream status %d", resp.StatusCode)
	}
	return nil
}

// goRedisAdapter adapts go-redis to tqueue.RedisClient.
type goRedisAdapter struct{ c redis.UniversalClient }

func (g goRedisAdapter) SetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return g.c.SetEx(ctx, key, value, ttl).Err()
}

func (g goRedisAdapter) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := g.c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	return v, err == nil, err
}

func (a *App) refreshCache(ctx context.Context, id int64, status string) {
	k := a.key("order:" + strconv.FormatInt(id, 10))
	v, err := a.rdb.Get(ctx, k).Result()
	if err != nil {
		return
	}
	var o Order
	if json.Unmarshal([]byte(v), &o) != nil {
		return
	}
	o.Status = status
	b, _ := json.Marshal(o)
	_ = a.rdb.Set(ctx, k, b, 10*time.Minute).Err()
}
