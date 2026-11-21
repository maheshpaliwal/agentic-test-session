package collector

import (
	"context"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/collector/store"
	"github.com/maheshpaliwal/agentic-test-session/core"
)

// hangSeal hangs GetSeal (the first store call of an actor's load, made with
// the REQUEST context) until its context ends or it is released.
type hangSeal struct {
	store.Store
	release chan struct{}
	entered chan struct{}
}

func (h *hangSeal) GetSeal(ctx context.Context, id string) (*store.SealInfo, error) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		return h.Store.GetSeal(ctx, id)
	}
}

// An ingest whose handler is still inside the store when Close runs (what is
// left after http.Server.Shutdown timed out: Shutdown does not cancel request
// contexts) must not hold Close past ShutdownTimeout.
func TestCloseIsBoundedWithRequestStuckInStore(t *testing.T) {
	h := &hangSeal{Store: store.NewMem(), release: make(chan struct{}), entered: make(chan struct{}, 1)}
	cfg := DefaultConfig()
	cfg.Window = 0
	cfg.ShutdownTimeout = 100 * time.Millisecond
	c := New(h, cfg)
	go c.Ingest(context.Background(), "i1", []core.Event{ev("a", core.KindDBRead, 1, 1)})
	<-h.entered
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		close(h.release)
		<-closed
		panic("Close still blocked 3s after it began with ShutdownTimeout=100ms")
	}
}
