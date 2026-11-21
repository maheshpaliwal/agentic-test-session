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
	capDeadline
)

// A capture may spend at most 1/captureShare of the time the caller's
// deadline leaves, so the application's own statement keeps most of it; with
// less than minCaptureBudget available the capture is skipped.
const (
	captureShare     = 4
	minCaptureBudget = 10 * time.Millisecond
)

// serverGrace is how long the client waits beyond the server-side
// statement_timeout so the server cancels first (a client-side cancel can
// force the driver to drop the connection).
const serverGrace = 200 * time.Millisecond

// capture runs a read-only capture query on the application's own connection
// without being able to hurt it:
//   - on Postgres it runs in SAVEPOINT ts_capture inside the app's transaction
//     or in its own read-only transaction when the app is in autocommit, with
//     a statement_timeout set (SET LOCAL) inside that fence. The fence is
//     always closed by rolling it back, after a capture that worked as after
//     one that failed: that ends an aborted subtransaction, so the app's
//     transaction is never aborted, and it reverts the timeout to whatever
//     the application had, so the old value is neither read nor restored;
//   - inside a SERIALIZABLE transaction nothing runs (extra reads could cause
//     serialization failures);
//   - the time spent never exceeds the capture timeout, nor more than a
//     fraction (captureShare) of what the caller's deadline leaves.
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
	eff, grace := c.cfg.captureTO, serverGrace
	if dl, ok := ctx.Deadline(); ok {
		budget := time.Until(dl) / captureShare
		if budget < minCaptureBudget {
			return nil, capDeadline
		}
		eff, grace = min(eff, budget), min(grace, budget)
	}
	total := eff
	if pg || c.cfg.store == "mysql" {
		total += grace // the server enforces the limit; the client cancel is only a backstop
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
	begin, closeSQL := fenceSavepoint, []string{fenceRollbackTo, fenceRelease}
	if !inTx {
		begin, closeSQL = fenceBegin, []string{fenceRollback}
	}
	timeout := fmt.Sprintf("SET LOCAL statement_timeout = %d", max(eff.Milliseconds(), 1))
	if _, err := c.execInner(cctx, begin, nil); err != nil {
		return nil, classify(err, cctx)
	}
	clean, ccancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer ccancel()
	closeFence := func() {
		for _, s := range closeSQL {
			c.execInner(clean, s, nil)
		}
	}
	if _, err := c.execInner(cctx, timeout, nil); err != nil {
		st = classify(err, cctx)
		closeFence()
		return nil, st
	}
	out, err := c.fetchImgs(cctx, q, vals, limit, pkCols)
	st = classify(err, cctx)
	closeFence()
	if err != nil {
		return nil, st
	}
	return out, capOK
}

// The statements of the Postgres capture fence.
const (
	fenceSavepoint  = "SAVEPOINT ts_capture"
	fenceRollbackTo = "ROLLBACK TO SAVEPOINT ts_capture"
	fenceRelease    = "RELEASE SAVEPOINT ts_capture"
	fenceBegin      = "BEGIN READ ONLY"
	fenceRollback   = "ROLLBACK"
)

// mysqlTimeoutHint adds a server-side MAX_EXECUTION_TIME optimizer hint to a
// capture SELECT (ignored as a comment by servers that do not know it).
func mysqlTimeoutHint(q string, d time.Duration) string {
	const p = "SELECT "
	if strings.HasPrefix(q, p) {
		return fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ %s", max(d.Milliseconds(), 1), q[len(p):])
	}
	return q
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
