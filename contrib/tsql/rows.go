package tsql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
)

// rows wraps driver.Rows, copying up to cap rows (as column->value maps) of
// the first result set and counting all of them. onClose runs once, after the
// inner rows are closed.
type rows struct {
	inner   driver.Rows
	cols    []string
	cap     int
	drain   bool // consume the remainder on Close (write ... RETURNING)
	data    []map[string]any
	raws    []map[string]driver.Value // PK values parallel to data
	pkCols  []string
	count   int
	eof     bool
	err     error
	later   bool // moved past the first result set
	release func()
	onClose func(r *rows)
	once    sync.Once
}

func (r *rows) Columns() []string { return r.inner.Columns() }

func (r *rows) Next(dest []driver.Value) error {
	err := r.inner.Next(dest)
	switch {
	case err == nil:
		r.note(dest)
	case err == io.EOF:
		r.eof = true
	default:
		r.err = err
	}
	return err
}

func (r *rows) note(dest []driver.Value) {
	if r.later {
		return
	}
	r.count++
	if len(r.data) < r.cap {
		if r.cols == nil {
			r.cols = r.inner.Columns()
		}
		r.data = append(r.data, rowMap(r.cols, dest))
		r.raws = append(r.raws, rawPK(r.cols, dest, r.pkCols))
	}
}

func (r *rows) Close() error {
	if r.drain && !r.eof && r.err == nil {
		dest := make([]driver.Value, len(r.inner.Columns()))
		for {
			if err := r.Next(dest); err != nil {
				break
			}
		}
	}
	err := r.inner.Close()
	if r.release != nil {
		r.release()
	}
	r.once.Do(func() {
		if r.onClose != nil {
			safe(func() { r.onClose(r) })
		}
	})
	return err
}

func (r *rows) HasNextResultSet() bool {
	if n, ok := r.inner.(driver.RowsNextResultSet); ok {
		return n.HasNextResultSet()
	}
	return false
}

func (r *rows) NextResultSet() error {
	n, ok := r.inner.(driver.RowsNextResultSet)
	if !ok {
		return io.EOF
	}
	err := n.NextResultSet()
	if err == nil {
		r.later = true
	}
	return err
}

func (r *rows) ColumnTypeScanType(i int) reflect.Type {
	if t, ok := r.inner.(driver.RowsColumnTypeScanType); ok {
		return t.ColumnTypeScanType(i)
	}
	return reflect.TypeFor[any]()
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	if t, ok := r.inner.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return t.ColumnTypeDatabaseTypeName(i)
	}
	return ""
}

func (r *rows) ColumnTypeLength(i int) (int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeLength); ok {
		return t.ColumnTypeLength(i)
	}
	return 0, false
}

func (r *rows) ColumnTypeNullable(i int) (bool, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeNullable); ok {
		return t.ColumnTypeNullable(i)
	}
	return false, false
}

func (r *rows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypePrecisionScale); ok {
		return t.ColumnTypePrecisionScale(i)
	}
	return 0, 0, false
}
