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
	pkLookup     bool // read primary keys from the catalog
	upsertBefore bool // read the row an upsert can update before it runs
	maxRows      int  // rows captured for images / returning
	sampleRows   int  // rows captured as a read sample
	maxArgs      int
	maxArgLen    int
	captureTO    time.Duration
}

func defaultConfig() *config {
	return &config{
		images:       true,
		skipTables:   map[string]struct{}{},
		pkCols:       map[string][]string{},
		defaultPK:    []string{"id"},
		pkLookup:     true,
		upsertBefore: true,
		maxRows:      50,
		sampleRows:   5,
		maxArgs:      64,
		maxArgLen:    256,
		captureTO:    500 * time.Millisecond,
	}
}

// Option configures the tracing layer.
type Option func(*config)

// WithStore forces the Mutation.Store label ("postgres" or "mysql") instead of
// detecting it from the driver name.
func WithStore(store string) Option { return func(c *config) { c.store = store } }

// WithImages toggles before/after image capture (extra same-connection
// SELECTs around UPDATE/DELETE and PK re-reads). Default true.
//
// Caveat: the pre-image SELECT is a plain unlocked read issued before the
// application's statement. Outside a transaction (or under READ COMMITTED) a
// concurrent writer can change the rows between the pre-image, the statement
// and the re-read, so before/after can show another writer's change or miss
// rows; images are best-effort evidence, not an atomic snapshot. Inside a
// Postgres transaction the capture runs in a SAVEPOINT so a failing capture
// cannot abort the application's transaction; it is skipped under SERIALIZABLE.
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

// WithPKColumns declares the primary-key columns of a table ("orders" or
// "schema.orders"). A declared key always wins and the catalog is not asked
// about that table. It is needed where the key cannot be looked up: a table
// without a primary key, a view, or a database other than Postgres and MySQL.
func WithPKColumns(table string, cols ...string) Option {
	return func(c *config) {
		lc := make([]string, len(cols))
		for i, s := range cols {
			lc[i] = strings.ToLower(s)
		}
		c.pkCols[strings.ToLower(table)] = lc
	}
}

// WithDefaultPK sets the PK columns assumed for a table that has no
// WithPKColumns and whose key the catalog did not give (default "id").
func WithDefaultPK(cols ...string) Option {
	return func(c *config) {
		c.defaultPK = nil
		for _, s := range cols {
			c.defaultPK = append(c.defaultPK, strings.ToLower(s))
		}
	}
}

// WithPKLookup toggles reading a table's primary key from the database
// catalog (default true; see "Primary keys" in the package documentation).
// With it off, and always with WithImages(false), keys come from
// WithPKColumns and WithDefaultPK only.
func WithPKLookup(on bool) Option { return func(c *config) { c.pkLookup = on } }

// WithUpsertBefore toggles the read of the row an upsert can update before
// the statement runs (default true; see "Upserts" in the package
// documentation). It is one capture: three round trips on Postgres, one on
// MySQL. With it off, Postgres still tells a row the upsert inserted and
// nothing has locked since (recorded as with it on); every other row is
// recorded with its new values only, no before image and no diff, under the
// flag "upsert_unknown". On MySQL every upsert is then flagged
// "upsert_unknown".
func WithUpsertBefore(on bool) Option { return func(c *config) { c.upsertBefore = on } }

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

// WithCaptureTimeout bounds each capture query (pre-image and re-read);
// default 500ms. A capture also never uses more than a quarter of the time
// the caller context's deadline leaves, and is skipped (flag
// "image_skipped:deadline") when that is under 10ms. On timeout
// the image is dropped and the event is flagged "image_timeout". Inside a
// Postgres transaction the limit is also enforced server-side with a
// transaction-local statement_timeout, so no client-side cancel (which can
// drop the connection) is normally needed.
func WithCaptureTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.captureTO = d
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

// explicitPK returns the key declared for the table with WithPKColumns.
func (c *config) explicitPK(schema, table string) ([]string, bool) {
	if v, ok := c.pkCols[strings.ToLower(table)]; ok {
		return v, true
	}
	if schema != "" {
		if v, ok := c.pkCols[strings.ToLower(schema+"."+table)]; ok {
			return v, true
		}
	}
	return nil, false
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
