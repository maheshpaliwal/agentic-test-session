package tsql

import (
	"database/sql/driver"
	"slices"
	"sync"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

const (
	// maxPKTables bounds the tables one handle remembers; the cache is emptied
	// when it is full.
	maxPKTables = 1024
	// maxPKLookups is how many failed lookups of one table a handle makes
	// before it stops asking and keeps the default key.
	maxPKLookups = 3
	// maxPKCols is more than any key has (Postgres allows 32 index columns,
	// MySQL 16).
	maxPKCols = 32
)

// pkCache remembers, for one database handle, what the catalog said the
// primary key of each table is. Tables are keyed by their name as written in
// the statement, which is also how the server resolved it.
type pkCache struct {
	mu sync.Mutex
	m  map[string]pkEntry
}

// pkEntry is the outcome of looking one table up: its key columns in key
// order, none (no primary key, a view, a table the user cannot see), or the
// number of lookups that failed so far.
type pkEntry struct {
	cols  []string
	fails int
}

// settled reports whether the table needs no further lookup.
func (e pkEntry) settled() bool { return e.fails == 0 || e.fails >= maxPKLookups }

func (p *pkCache) get(table string) (pkEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.m[table]
	return e, ok
}

func (p *pkCache) put(table string, e pkEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.m[table]; !ok && len(p.m) >= maxPKTables {
		clear(p.m)
	}
	if p.m == nil {
		p.m = map[string]pkEntry{}
	}
	p.m[table] = e
}

// failed counts one failed lookup of table.
func (p *pkCache) failed(table string) {
	e, _ := p.get(table)
	p.put(table, pkEntry{fails: e.fails + 1})
}

func (p *pkCache) reset() {
	p.mu.Lock()
	clear(p.m)
	p.mu.Unlock()
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

// noteSchema drops the cached keys after a traced statement that can change
// them: DDL, or one that names search_path (which decides what an unqualified
// table name means).
func (c *conn) noteSchema(in mutation.Info) {
	if in.Kind == mutation.DDL || (in.Kind == mutation.Other && slices.Contains(in.Idents, "search_path")) {
		c.pks.reset()
	}
}
