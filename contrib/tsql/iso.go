package tsql

import (
	"database/sql/driver"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql/mutation"
)

// Captures must not run inside a SERIALIZABLE transaction: on Postgres their
// reads take predicate locks and can make it, or a concurrent one, fail with
// a serialization error the untraced run would not have had; on MySQL a plain
// SELECT is a locking read there. So the level of the open transaction is
// tracked per connection.
//
// It is known without asking when a statement on the connection set it, traced
// or not: database/sql's BeginTx options, the ISOLATION LEVEL mode of BEGIN /
// START TRANSACTION, SET TRANSACTION, and for a transaction that states no
// level the session default set by SET SESSION CHARACTERISTICS or
// default_transaction_isolation (see mutation.Info.TxIso and DefIso). Inside
// a transaction known to be SERIALIZABLE nothing is sent.
//
// A default no statement shows (role or database setting, the connection
// string's options, a function) leaves the level unknown. On Postgres the
// server is then asked by the statement that sets the timeout of the
// transaction's first fence, so without a round trip or even a statement of
// its own (isoAskTimeout). What it says holds for
// the rest of the transaction, and a SERIALIZABLE default is remembered for
// the connection until a statement changes it. On MySQL there is no fence to
// ask in and an unknown level is read as before.

// isoState is what is known of the isolation level of the open transaction.
type isoState uint8

const (
	isoUnknown isoState = iota
	isoSerial           // SERIALIZABLE: nothing may be read
	isoOther            // stated by a statement or by the server not to be
)

// isoAskTimeout sets the fence's statement_timeout (%d, in ms) as SET LOCAL
// does, but only in a transaction that is not SERIALIZABLE, and says so in
// its row count, which an Exec reports without reading rows (for a text of
// several statements it is the count of the last). 1: the timeout is set and
// the transaction is not SERIALIZABLE. Anything else (0, or a driver that
// reports no count) is taken as SERIALIZABLE.
const isoAskTimeout = "SELECT set_config('statement_timeout', '%d', true) WHERE current_setting('transaction_isolation') <> 'serializable'"

func isoOf(l mutation.IsoLevel) isoState {
	switch l {
	case mutation.IsoSerializable:
		return isoSerial
	case mutation.IsoOther:
		return isoOther
	}
	return isoUnknown
}

func (c *conn) inTxLocked() bool { return c.txID != "" || c.rawTx }

// beginIso sets the level of a transaction that begins; mutation.IsoUnset is
// the session default. c.mu is held.
func (c *conn) beginIso(l mutation.IsoLevel) {
	c.iso, c.isoDefault = isoOf(l), l == mutation.IsoUnset
	if c.isoDefault && (c.defSerial || c.nextSerial) {
		c.iso = isoSerial
	}
	c.nextSerial = false
}

// endIso forgets the level of the transaction that ended. A Postgres session
// default set inside a transaction that did not commit is not the session's.
// c.mu is held.
func (c *conn) endIso(committed bool) {
	c.iso, c.isoDefault = isoUnknown, false
	if c.defInTx && !committed {
		c.defSerial = false
	}
	c.defInTx = false
}

// noteIso applies what a statement that ran did to isolation levels and to
// the transaction they belong to. c.mu is held.
func (c *conn) noteIso(in mutation.Info) {
	mysql := c.cfg.store == "mysql"
	if in.DefIso != mutation.IsoUnset {
		// A level the text does not show: Postgres is asked when it matters,
		// MySQL cannot be and is taken to be SERIALIZABLE.
		c.defSerial = in.DefIso == mutation.IsoSerializable || mysql && in.DefIso == mutation.IsoUnknown
		c.defInTx = !mysql && (c.inTxLocked() || in.Tx.Opens()) // MySQL's SET is not undone by a rollback
		if c.defInTx {
			// Set inside a transaction, the default can be undone by a
			// rollback to a savepoint, which is not tracked, and it is never
			// the level of the transaction it was set in. It is not believed:
			// the next transaction's fence asks the server.
			c.defSerial = false
		}
	}
	switch tc := in.Tx; {
	case tc == mutation.TxBegin:
		c.beginIso(in.TxIso)
	case tc.Ends():
		c.endIso(tc == mutation.TxCommit)
	case in.TxIso == mutation.IsoUnset:
		// nothing said; after COMMIT AND CHAIN the new transaction has the
		// level of the one it follows
	case c.inTxLocked() || tc == mutation.TxChain:
		c.iso, c.isoDefault = isoOf(in.TxIso), false
	case mysql && in.TxIso != mutation.IsoOther:
		c.nextSerial = true
	}
}

// learnIso records the server's answer to isoAskTimeout for the open
// transaction.
// One that took the session default tells what that default is.
func (c *conn) learnIso(res driver.Result) (serializable bool) {
	serializable = true
	if res != nil {
		if n, err := res.RowsAffected(); err == nil && n == 1 {
			serializable = false
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.iso = isoOther
	if serializable {
		c.iso = isoSerial
	}
	if c.isoDefault {
		c.defSerial = serializable
	}
	return serializable
}
