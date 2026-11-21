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
	capAborted // the transaction is already aborted: the fence could not open and nothing ran
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
//     the application had, so the old value is neither read nor restored.
//     That is three round trips per capture (open, SELECT, close) where the
//     driver sends two statements in one text, five or four where it cannot;
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
	inTx, iso := c.txState()
	if inTx && iso == isoSerial {
		return nil, capSerializable
	}
	// A transaction whose level no statement showed: the fence asks (iso.go).
	ask := pg && inTx && iso == isoUnknown
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
	//
	// The two statements that open the fence, and the two that close it inside
	// a transaction, take no arguments, so each pair can go to the server as
	// one text in one round trip where the driver sends an argument-less Exec
	// with the simple query protocol (pgx, lib/pq). A driver that prepares
	// every statement gets such a text refused as a whole; inside a
	// transaction that refusal is an error like any other and would abort it.
	// So a pair is sent as one text only where a refusal is harmless: the
	// closing pair (under the fence's own savepoint, and the two statements
	// sent one by one afterwards recover from it), the opening pair in
	// autocommit, and the opening pair inside a transaction only once this
	// connection has accepted such a text (c.multi).
	begin := fenceSavepoint
	if !inTx {
		begin = fenceBegin
	}
	timeout := fmt.Sprintf("SET LOCAL statement_timeout = %d", max(eff.Milliseconds(), 1))
	if ask {
		timeout = fmt.Sprintf(isoAskTimeout, max(eff.Milliseconds(), 1))
	}
	closeFence := func() {
		// Its own second, counted from now: the capture may have used up any
		// earlier deadline, and a fence left open keeps the timeout and the
		// savepoint (or, in autocommit, a read-only transaction) in place.
		clean, ccancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer ccancel()
		if !inTx {
			c.execInner(clean, fenceRollback, nil)
			return
		}
		if c.multi != multiNo {
			if _, err := c.execInner(clean, fenceRollbackTo+"; "+fenceRelease, nil); err == nil {
				c.multi = multiYes
				return
			}
			if clean.Err() == nil { // refused, not merely out of time
				c.multi = multiNo
			}
		}
		c.execInner(clean, fenceRollbackTo, nil)
		c.execInner(clean, fenceRelease, nil)
	}

	opened := false
	if c.multi == multiYes || (!inTx && c.multi == multiUnknown) {
		res, err := c.execInner(cctx, begin+"; "+timeout, nil)
		switch {
		case err == nil:
			c.multi, opened = multiYes, true
			if ask && c.learnIso(res) {
				closeFence()
				return nil, capSerializable
			}
		case inTx:
			// The text itself is known to be accepted here, so one of the two
			// statements failed. In a transaction that was already aborted
			// that is the SAVEPOINT: there is no fence then, and nothing more
			// is sent (a ROLLBACK TO could hit a savepoint of the same name
			// that belongs to the application).
			if txAborted(err) {
				return nil, capAborted
			}
			st = classify(err, cctx)
			closeFence()
			return nil, st
		default:
			st = classify(err, cctx)
			if txAborted(err) {
				// The server is in an aborted transaction this connection did
				// not see begin (the application sent "BEGIN; ..." as one
				// text). It is the application's: a ROLLBACK here would end it
				// and let the statement that follows commit on its own.
				return nil, capAborted
			}
			// Autocommit: nothing ran, or the BEGIN did and the transaction it
			// opened is tsql's own. Ending it is right in both cases.
			closeFence()
			if c.multi == multiYes || cctx.Err() != nil {
				return nil, st
			}
			c.multi = multiNo // statement by statement from here on
		}
	}
	if !opened {
		if _, err := c.execInner(cctx, begin, nil); err != nil {
			if txAborted(err) {
				return nil, capAborted
			}
			return nil, classify(err, cctx)
		}
		res, err := c.execInner(cctx, timeout, nil)
		if err != nil {
			st = classify(err, cctx)
			closeFence()
			return nil, st
		}
		if ask && c.learnIso(res) {
			closeFence()
			return nil, capSerializable
		}
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
	// The session default may make this transaction SERIALIZABLE. That is
	// harmless: it is always rolled back, and the reads of a transaction
	// that rolled back take part in no serialization conflict.
	fenceBegin    = "BEGIN READ ONLY"
	fenceRollback = "ROLLBACK"
)

// multiStmt is what a connection is known to do with a text that holds two
// statements.
type multiStmt uint8

const (
	multiUnknown multiStmt = iota
	multiYes               // accepted one: the driver sends it in one round trip
	multiNo                // refused one: every statement is sent on its own
)

// txAborted reports whether err is Postgres refusing a statement because the
// transaction is already aborted (SQLSTATE 25P02). The code is read from the
// driver's error where it offers one (pgx, lib/pq); the message is the
// fallback, and is in English only when the server's lc_messages is.
func txAborted(err error) bool {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState() == "25P02"
	}
	m := err.Error()
	return strings.Contains(m, "25P02") || strings.Contains(m, "current transaction is aborted")
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
