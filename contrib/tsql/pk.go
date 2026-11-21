package tsql

import (
	"database/sql/driver"
	"slices"
	"sync"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

const (
	// maxPKTables bounds the tables one handle remembers; when it is full the
	// entries that are out of date go, or failing that any one entry.
	maxPKTables = 1024
	// maxPKLookups is how many failed lookups of one table a handle makes
	// before it stops asking and keeps the default key.
	maxPKLookups = 3
	// maxPKCols is more than any key has (Postgres allows 32 index columns,
	// MySQL 16).
	maxPKCols = 32
)

// pkTTL is how long a handle trusts what the catalog said. A key can change
// in ways no statement on the handle shows (another connection's DDL, a
// search_path that is set by a function or reverts with its transaction, a
// table created after it was first named), so every answer is asked for
// again after this long, a failed lookup's fallback included.
var pkTTL = 30 * time.Second

// pkCache remembers, for one database handle, what the catalog said the
// primary key of each table is. Tables are keyed by their name as written in
// the statement, which is also how the server resolved it.
type pkCache struct {
	mu sync.Mutex
	m  map[string]pkEntry
	// nosys are the tables whose rows could not be read with their xmax (a
	// view has none), and when that was seen; see upsert.go.
	nosys map[string]time.Time
}

// pkEntry is the outcome of looking one table up: its key columns in key
// order, none (no primary key, a view, a table the user cannot see), or the
// number of lookups that failed so far.
type pkEntry struct {
	cols  []string
	fails int
	at    time.Time // when the catalog was asked
}

// settled reports whether the table needs no further lookup.
func (e pkEntry) settled() bool { return e.fails == 0 || e.fails >= maxPKLookups }

func (p *pkCache) get(table string) (pkEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.m[table]
	if ok && time.Since(e.at) > pkTTL {
		delete(p.m, table)
		return pkEntry{}, false
	}
	return e, ok
}

func (p *pkCache) put(table string, e pkEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[table]; !ok && len(p.m) >= maxPKTables {
		for k, v := range p.m {
			if time.Since(v.at) > pkTTL {
				delete(p.m, k)
			}
		}
		for k := range p.m { // still full: make room for one
			if len(p.m) < maxPKTables {
				break
			}
			delete(p.m, k)
		}
	}
	if p.m == nil {
		p.m = map[string]pkEntry{}
	}
	e.at = time.Now()
	p.m[table] = e
}

// drop forgets the given tables.
func (p *pkCache) drop(tables ...string) {
	p.mu.Lock()
	for _, t := range tables {
		delete(p.m, t)
	}
	p.mu.Unlock()
}

// failed counts one failed lookup of table.
func (p *pkCache) failed(table string) {
	e, _ := p.get(table)
	p.put(table, pkEntry{fails: e.fails + 1})
}

func (p *pkCache) reset() {
	p.mu.Lock()
	clear(p.m)
	clear(p.nosys)
	p.mu.Unlock()
}

// noSys reports whether table was found to have no xmax to read. Like a key,
// that is trusted for pkTTL: the name may come to mean another table.
func (p *pkCache) noSys(table string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	at, ok := p.nosys[table]
	if ok && time.Since(at) > pkTTL {
		delete(p.nosys, table)
		return false
	}
	return ok
}

func (p *pkCache) setNoSys(table string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.nosys) >= maxPKTables {
		clear(p.nosys)
	}
	if p.nosys == nil {
		p.nosys = map[string]time.Time{}
	}
	p.nosys[table] = time.Now()
}

// The catalog queries return one row per key column, in key order. The
// Postgres one hands the table name as written to to_regclass, so quoting,
// case folding and search_path are resolved by the server exactly as they are
// for the application's statement; a name that does not resolve, a view and a
// table without a primary key all give no rows.
const (
	pgPKQuery = "SELECT a.attname FROM pg_catalog.pg_index i" +
		" CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)" +
		" JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum" +
		" WHERE i.indrelid = pg_catalog.to_regclass($1) AND i.indisprimary ORDER BY k.ord"
	mysqlPKQuery = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE" +
		" WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
	mysqlPKQuerySchema = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE" +
		" WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
)

// knownPK returns the key columns to use for the statement's table without
// asking the database: declared with WithPKColumns, else found in the catalog
// earlier, else the default. catalog reports that the names are the catalog's
// own spelling.
func (c *conn) knownPK(in mutation.Info) (cols []string, catalog bool) {
	if v, ok := c.cfg.explicitPK(in.Schema, in.Table); ok {
		return v, false
	}
	if in.TableSQL != "" {
		if e, ok := c.pks.get(in.TableSQL); ok && len(e.cols) > 0 {
			return e.cols, true
		}
	}
	return c.cfg.defaultPK, false
}

// lookupPK reads the primary key of the statement's table from the catalog
// the first time a traced write names the table on this handle. It runs
// through capture, so it is fenced and bounded like an image read and cannot
// fail or alter the application's transaction. The outcome is cached, a
// failure included: after maxPKLookups failures the table keeps the default
// key. A lookup that could not run at all (SERIALIZABLE, too little time
// left, a transaction that is already aborted) is made by a later statement.
func (t *stmtTrace) lookupPK() {
	in := t.info
	if !t.write || !t.cfg.images || !t.cfg.pkLookup || t.skip || in.Bulk || in.TableSQL == "" {
		return
	}
	switch in.Kind {
	case mutation.Insert, mutation.Update, mutation.Delete, mutation.Upsert:
	default:
		return
	}
	pg := t.pg()
	if !pg && t.cfg.store != "mysql" {
		return // no catalog query for this database
	}
	if _, ok := t.cfg.explicitPK(in.Schema, in.Table); ok {
		return
	}
	if e, ok := t.c.pks.get(in.TableSQL); ok && e.settled() {
		return
	}
	q, vals := pgPKQuery, []driver.Value{in.TableSQL}
	switch {
	case pg:
	case in.Schema != "":
		q, vals = mysqlPKQuerySchema, []driver.Value{in.Schema, in.Table}
	default:
		q, vals = mysqlPKQuery, []driver.Value{in.Table}
	}
	rs, st := t.c.capture(t.ctx, pg, q, vals, maxPKCols+1, nil)
	switch st {
	case capOK:
		cols, ok := pkNames(rs)
		if !ok {
			t.c.pks.failed(in.TableSQL)
			t.flag("pk_lookup_failed")
			return
		}
		t.c.pks.put(in.TableSQL, pkEntry{cols: cols})
		t.c.notePKLookup(in.TableSQL)
		if len(cols) > 0 {
			t.pk, t.pkCatalog = cols, true
		}
	case capSerializable, capDeadline:
	case capAborted:
		t.aborted = true // says nothing about the table, and the pre-image need not try again
	default:
		t.c.pks.failed(in.TableSQL)
		t.flag("pk_lookup_failed")
	}
}

// pkNames reads the column names out of a catalog answer: one text column per
// row. ok is false for an answer of any other shape.
func pkNames(rs []img) (cols []string, ok bool) {
	if len(rs) > maxPKCols {
		return nil, false
	}
	for _, r := range rs {
		if len(r.disp) != 1 {
			return nil, false
		}
		for _, v := range r.disp {
			s, isStr := v.(string)
			if !isStr || s == "" || slices.Contains(cols, s) {
				return nil, false
			}
			cols = append(cols, s)
		}
	}
	return cols, true
}

// schemaWords are the words of a statement that is neither a read nor a
// write of rows but can change what an unqualified table name means.
var schemaWords = []string{"search_path", "schema", "role", "authorization", "discard", "reset", "use"}

// noteSchema drops the cached keys after a traced statement that can change
// them: DDL, or one that can change what an unqualified table name means
// (search_path and its other spellings, SET ROLE, RESET, DISCARD, MySQL's
// USE). Inside a transaction the change may be undone when it ends, so the
// keys are dropped again then.
func (c *conn) noteSchema(in mutation.Info) {
	hit := in.Kind == mutation.DDL || slices.Contains(in.Idents, "search_path") || slices.Contains(in.Idents, "set_config")
	if !hit && in.Kind == mutation.Other {
		hit = slices.ContainsFunc(schemaWords, func(w string) bool { return slices.Contains(in.Idents, w) })
	}
	if !hit {
		return
	}
	c.pks.reset()
	c.mu.Lock()
	c.pkSchemaDirty = c.txID != "" || c.rawTx
	c.mu.Unlock()
}

// notePKLookup remembers that table's key was read inside the open
// transaction: if that transaction is rolled back, what the lookup saw (DDL
// of the same transaction) may never have happened.
func (c *conn) notePKLookup(table string) {
	c.mu.Lock()
	if c.txID != "" || c.rawTx {
		c.pkTxTables = append(c.pkTxTables, table)
	}
	c.mu.Unlock()
}

// endPKTx is called when the connection's transaction ends.
func (c *conn) endPKTx(committed bool) {
	c.mu.Lock()
	dirty, tables := c.pkSchemaDirty, c.pkTxTables
	c.pkSchemaDirty, c.pkTxTables = false, nil
	c.mu.Unlock()
	switch {
	case dirty:
		c.pks.reset()
	case !committed:
		c.pks.drop(tables...)
	}
}
