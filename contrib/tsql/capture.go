package tsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

type capStatus int

const (
	capOK capStatus = iota
	capFailed
	capTimeout
	capSerializable
)

// serverGrace is how long the client waits beyond the server-side
// statement_timeout so the server cancels first (a client-side cancel can
// force the driver to drop the connection).
const serverGrace = 200 * time.Millisecond

// capture runs a read-only capture query on the application's own connection
// without being able to hurt it:
//   - on Postgres it runs in SAVEPOINT ts_capture inside the app's transaction
//     (rolled back on any error, so the app's transaction is never aborted),
//     or in its own read-only transaction when the app is in autocommit, with
//     a transaction-local statement_timeout that is restored afterwards;
//   - inside a SERIALIZABLE transaction nothing runs (extra reads could cause
//     serialization failures);
//   - the time spent never exceeds the capture timeout or the caller's deadline.
func (c *conn) capture(ctx context.Context, pg bool, q string, vals []driver.Value, limit int, pkCols []string) (out []img, st capStatus) {
	defer func() {
		if r := recover(); r != nil {
			out, st = nil, capFailed
		}
	}()
	inTx, serial := c.txState()
	if inTx && serial {
		return nil, capSerializable
	}
	if ctx.Err() != nil {
		return nil, capFailed
	}
	eff := c.cfg.captureTO
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl)
		if rem <= 0 {
			return nil, capTimeout
		}
		if rem < eff {
			eff = rem
		}
	}
	total := eff
	if pg || c.cfg.store == "mysql" {
		total += serverGrace // the server enforces the limit; the client cancel is only a backstop
	}
	cctx, cancel := context.WithTimeout(ctx, total)
	defer cancel()

	if !pg {
		if c.cfg.store == "mysql" {
			q = mysqlTimeoutHint(q, eff)
		}
		out, err := c.fetchImgs(cctx, q, vals, limit, pkCols)
		return out, classify(err, cctx)
	}

	// Postgres: a client-side cancel makes the driver drop the connection and
	// any error aborts an open transaction, so the capture is fenced with a
	// SAVEPOINT (inside the app's tx) or its own read-only transaction (when
	// the app is in autocommit), and bounded server-side by statement_timeout.
	begin, abortSQL, endSQL := "SAVEPOINT ts_capture", []string{"ROLLBACK TO SAVEPOINT ts_capture", "RELEASE SAVEPOINT ts_capture"}, []string{"RELEASE SAVEPOINT ts_capture"}
	if !inTx {
		begin, abortSQL, endSQL = "BEGIN READ ONLY", []string{"ROLLBACK"}, []string{"ROLLBACK"}
	}
	if _, err := c.execInner(cctx, begin, nil); err != nil {
		return nil, classify(err, cctx)
	}
	clean, ccancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer ccancel()
	run := func(stmts []string) error {
		var first error
		for _, s := range stmts {
			if _, err := c.execInner(clean, s, nil); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	ms := max(eff.Milliseconds(), 1)
	prev := ""
	_, rs, err := c.queryAll(cctx, fmt.Sprintf("SELECT current_setting('statement_timeout'), set_config('statement_timeout', '%d', true)", ms), nil, 1)
	if err != nil {
		run(abortSQL)
		return nil, classify(err, cctx)
	}
	if inTx && len(rs) == 1 && len(rs[0]) > 0 {
		switch v := rs[0][0].(type) {
		case string:
			prev = v
		case []byte:
			prev = string(v)
		}
		if !simpleSetting(prev) {
			prev = ""
		}
	}
	out, err = c.fetchImgs(cctx, q, vals, limit, pkCols)
	if err != nil {
		run(abortSQL)
		return nil, classify(err, cctx)
	}
	if prev != "" {
		if _, _, err := c.queryAll(clean, fmt.Sprintf("SELECT set_config('statement_timeout', '%s', true)", prev), nil, 1); err != nil {
			run(abortSQL)
			return out, capOK
		}
	}
	if err := run(endSQL); err != nil {
		run(abortSQL)
	}
	return out, capOK
}

// mysqlTimeoutHint adds a server-side MAX_EXECUTION_TIME optimizer hint to a
// capture SELECT (ignored as a comment by servers that do not know it).
func mysqlTimeoutHint(q string, d time.Duration) string {
	const p = "SELECT "
	if strings.HasPrefix(q, p) {
		return fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ %s", max(d.Milliseconds(), 1), q[len(p):])
	}
	return q
}

func simpleSetting(s string) bool {
	if s == "" || len(s) > 32 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

func classify(err error, cctx context.Context) capStatus {
	if err == nil {
		return capOK
	}
	if errors.Is(err, context.DeadlineExceeded) || cctx.Err() == context.DeadlineExceeded {
		return capTimeout
	}
	m := err.Error()
	if strings.Contains(m, "statement timeout") || strings.Contains(m, "57014") {
		return capTimeout
	}
	return capFailed
}

// queryAll runs q on the wrapped connection and returns up to limit rows of
// copied raw driver values.
func (c *conn) queryAll(ctx context.Context, q string, vals []driver.Value, limit int) (cols []string, out [][]driver.Value, err error) {
	nv := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		nv[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	r, rel, err := c.queryInner(ctx, q, nv)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		r.Close()
		if rel != nil {
			rel()
		}
	}()
	cols = r.Columns()
	for len(out) < limit {
		dest := make([]driver.Value, len(cols))
		if err := r.Next(dest); err != nil {
			if err != io.EOF {
				return nil, nil, err
			}
			break
		}
		for i, v := range dest {
			if b, ok := v.([]byte); ok {
				dest[i] = append([]byte(nil), b...)
			}
		}
		out = append(out, dest)
	}
	return cols, out, nil
}

func (c *conn) fetchImgs(ctx context.Context, q string, vals []driver.Value, limit int, pkCols []string) ([]img, error) {
	cols, rs, err := c.queryAll(ctx, q, vals, limit)
	if err != nil {
		return nil, err
	}
	out := make([]img, len(rs))
	for i, row := range rs {
		out[i] = img{disp: rowMap(cols, row), raw: rawPK(cols, row, pkCols)}
	}
	return out, nil
}
