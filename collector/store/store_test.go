package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/collector/store/storetest"
)

func TestMem(t *testing.T) { storetest.Run(t, store.NewMem()) }

func TestPG(t *testing.T) {
	dsn := os.Getenv("TSESSION_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TSESSION_TEST_PG_DSN not set")
	}
	pg, err := store.OpenPG(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	storetest.Run(t, pg)
}
