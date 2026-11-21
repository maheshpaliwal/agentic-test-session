package tsql_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/contrib/tsql"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// A handle opened before the SDK starts is the plain driver's and records
// nothing afterwards. The SDK says so at start, if the wrapper tells it.
func TestOpenBeforeStartIsNoted(t *testing.T) {
	if core.Enabled() {
		t.Skip("the SDK is already running in this test binary")
	}
	f := &fakeDB{}
	db := tsql.OpenDB(fakeConnector{f})
	defer db.Close()
	if d := tsql.WrapDriver(f); d != f {
		panic("WrapDriver wrapped although the SDK is off")
	}
	for _, want := range []string{"tsql.OpenDB", "tsql.WrapDriver"} {
		if !slices.Contains(core.WrappersWhileDisabled(), want) {
			panic(fmt.Sprintf("noted %q, want %s among them", core.WrappersWhileDisabled(), want))
		}
	}
	rec, ctx := setup(t)
	if _, err := db.ExecContext(ctx, "UPDATE t SET a = 1 WHERE id = 1"); err != nil {
		panic(err)
	}
	if n := len(rec.Events()); n != 0 {
		panic(fmt.Sprintf("%d events from a handle opened before the start: the note would be wrong", n))
	}
}

type fakeConnector struct{ f *fakeDB }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.f.Open("") }
func (c fakeConnector) Driver() driver.Driver                        { return c.f }
