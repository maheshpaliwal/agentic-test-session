package tsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"tsession/core"
)

// Open is a drop-in replacement for sql.Open. If the SDK is disabled it is
// sql.Open.
func Open(driverName, dsn string, opts ...Option) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil || !core.Enabled() {
		return db, err
	}
	drv := db.Driver()
	if _, ok := drv.(*wrapDriver); ok {
		return db, nil
	}
	cfg := buildConfig(opts)
	if cfg.store == "" {
		cfg.store = detectStore(driverName, fmt.Sprintf("%T", drv), dsn)
	}
	_ = db.Close() // sql.Open does not dial; discard the unwrapped handle
	wd := &wrapDriver{inner: drv, cfg: cfg}
	conn, err := wd.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(conn), nil
}

// OpenDB is a drop-in replacement for sql.OpenDB.
func OpenDB(c driver.Connector, opts ...Option) *sql.DB {
	if !core.Enabled() {
		return sql.OpenDB(c)
	}
	if _, ok := c.(*wrapConnector); ok {
		return sql.OpenDB(c)
	}
	cfg := buildConfig(opts)
	if cfg.store == "" {
		cfg.store = detectStore(fmt.Sprintf("%T", c.Driver()))
	}
	return sql.OpenDB(&wrapConnector{inner: c, cfg: cfg, drv: &wrapDriver{inner: c.Driver(), cfg: cfg}})
}

// Register registers d under name wrapped with tracing (when enabled).
func Register(name string, d driver.Driver) {
	sql.Register(name, WrapDriver(d))
}

// WrapDriver returns d with tracing; d itself when the SDK is disabled or d
// is already wrapped.
func WrapDriver(d driver.Driver, opts ...Option) driver.Driver {
	if d == nil || !core.Enabled() {
		return d
	}
	if _, ok := d.(*wrapDriver); ok {
		return d
	}
	cfg := buildConfig(opts)
	if cfg.store == "" {
		cfg.store = detectStore(fmt.Sprintf("%T", d))
	}
	return &wrapDriver{inner: d, cfg: cfg}
}

type wrapDriver struct {
	inner driver.Driver
	cfg   *config
}

func (d *wrapDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return newConn(c, d.cfg), nil
}

func (d *wrapDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.inner.(driver.DriverContext); ok {
		c, err := dc.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &wrapConnector{inner: c, cfg: d.cfg, drv: d}, nil
	}
	return &wrapConnector{inner: dsnConnector{drv: d.inner, dsn: name}, cfg: d.cfg, drv: d}, nil
}

type dsnConnector struct {
	drv driver.Driver
	dsn string
}

func (c dsnConnector) Connect(context.Context) (driver.Conn, error) { return c.drv.Open(c.dsn) }
func (c dsnConnector) Driver() driver.Driver                        { return c.drv }

type wrapConnector struct {
	inner driver.Connector
	cfg   *config
	drv   *wrapDriver
}

func (c *wrapConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(conn, c.cfg), nil
}

func (c *wrapConnector) Driver() driver.Driver { return c.drv }

// Close lets sql.DB.Close release connectors that own resources.
func (c *wrapConnector) Close() error {
	if cl, ok := c.inner.(interface{ Close() error }); ok {
		return cl.Close()
	}
	return nil
}
