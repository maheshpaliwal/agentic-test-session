package tsql

import (
	"strings"
	"time"
)

type config struct {
	store        string // "postgres" | "mysql" | "" (infer)
	images       bool
	captureAfter bool
	skipTables   map[string]struct{}
	pkCols       map[string][]string
	defaultPK    []string
	maxRows      int // rows captured for images / returning
	sampleRows   int // rows captured as a read sample
	maxArgs      int
	maxArgLen    int
	captureTO    time.Duration
}

func defaultConfig() *config {
	return &config{
		images:     true,
		skipTables: map[string]struct{}{},
		pkCols:     map[string][]string{},
		defaultPK:  []string{"id"},
		maxRows:    50,
		sampleRows: 5,
		maxArgs:    64,
		maxArgLen:  256,
		captureTO:  5 * time.Second,
	}
}

// Option configures the tracing layer.
type Option func(*config)

// WithStore forces the Mutation.Store label ("postgres" or "mysql") instead of
// detecting it from the driver name.
func WithStore(store string) Option { return func(c *config) { c.store = store } }

// WithImages toggles before/after image capture (extra same-connection
// SELECTs around UPDATE/DELETE and PK re-reads). Default true.
func WithImages(on bool) Option { return func(c *config) { c.images = on } }

// WithCaptureAfterImage lets the layer append "RETURNING *" to Postgres
// INSERT/UPDATE statements that run through the Query path and lack a
// RETURNING clause, so the after image comes straight from the write. The
// application then receives those extra rows. Default false; without it the
// after image is re-read by primary key when one is known.
func WithCaptureAfterImage(on bool) Option { return func(c *config) { c.captureAfter = on } }

// WithSkipTables disables argument, sample and image capture for the named
// tables ("orders" or "schema.orders"; case-insensitive). The statements are
// still recorded, flagged "table_skipped".
func WithSkipTables(tables ...string) Option {
	return func(c *config) {
		for _, t := range tables {
			c.skipTables[strings.ToLower(t)] = struct{}{}
		}
	}
}

// WithPKColumns declares the primary-key columns of a table (default "id").
func WithPKColumns(table string, cols ...string) Option {
	return func(c *config) {
		lc := make([]string, len(cols))
		for i, s := range cols {
			lc[i] = strings.ToLower(s)
		}
		c.pkCols[strings.ToLower(table)] = lc
	}
}

// WithDefaultPK sets the PK columns assumed for tables without WithPKColumns.
func WithDefaultPK(cols ...string) Option {
	return func(c *config) {
		c.defaultPK = nil
		for _, s := range cols {
			c.defaultPK = append(c.defaultPK, strings.ToLower(s))
		}
	}
}

// WithMaxImageRows caps rows captured per statement for images (default 50).
// Statements affecting more rows are flagged "image_truncated".
func WithMaxImageRows(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxRows = n
		}
	}
}

// WithSampleRows caps the rows kept in a read summary sample (default 5).
func WithSampleRows(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.sampleRows = n
		}
	}
}

func buildConfig(opts []Option) *config {
	c := defaultConfig()
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	return c
}

func (c *config) skipped(schema, table string) bool {
	if len(c.skipTables) == 0 || table == "" {
		return false
	}
	if _, ok := c.skipTables[strings.ToLower(table)]; ok {
		return true
	}
	if schema != "" {
		_, ok := c.skipTables[strings.ToLower(schema+"."+table)]
		return ok
	}
	return false
}

func (c *config) pk(schema, table string) []string {
	if v, ok := c.pkCols[strings.ToLower(table)]; ok {
		return v
	}
	if schema != "" {
		if v, ok := c.pkCols[strings.ToLower(schema+"."+table)]; ok {
			return v
		}
	}
	return c.defaultPK
}

// detectStore guesses the database from a driver name, type name or DSN.
func detectStore(hints ...string) string {
	for _, h := range hints {
		h = strings.ToLower(h)
		switch {
		case strings.Contains(h, "mysql"), strings.Contains(h, "maria"):
			return "mysql"
		case strings.Contains(h, "pgx"), strings.Contains(h, "postgres"), strings.Contains(h, "pq."),
			h == "pq", strings.Contains(h, "cockroach"), strings.Contains(h, "stdlib"):
			return "postgres"
		case strings.Contains(h, "@tcp("), strings.Contains(h, "@unix("):
			return "mysql"
		case strings.Contains(h, "host="), strings.Contains(h, "sslmode="):
			return "postgres"
		}
	}
	return ""
}
