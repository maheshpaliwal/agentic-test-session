package tsql

import (
	"context"
	"database/sql/driver"
)

type stmt struct {
	inner driver.Stmt
	c     *conn
	query string
}

func (s *stmt) Close() error  { return s.inner.Close() }
func (s *stmt) NumInput() int { return s.inner.NumInput() }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.doExec(ctx, s.query, args, func() (driver.Result, error) { return stmtExec(ctx, s.inner, args) })
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.doQuery(ctx, s.query, args, false, func(string) (driver.Rows, func(), error) {
		rows, err := stmtQuery(ctx, s.inner, args)
		return rows, nil, err
	})
}

// ColumnConverter forwards to the wrapped statement so its argument coercion
// is preserved; otherwise the standard converter applies, as it would unwrapped.
func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
	if cc, ok := s.inner.(driver.ColumnConverter); ok {
		return cc.ColumnConverter(idx)
	}
	return driver.DefaultParameterConverter
}

func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if ck, ok := s.inner.(driver.NamedValueChecker); ok {
		return ck.CheckNamedValue(nv)
	}
	return s.c.CheckNamedValue(nv)
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
