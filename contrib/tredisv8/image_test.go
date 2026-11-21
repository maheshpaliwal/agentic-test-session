package tredisv8

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/redisx"
)

// countHook counts single-command round trips, image reads included (the
// client's own connection handshake excluded).
type countHook struct{ n atomic.Int64 }

func (h *countHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if !redisx.IsHandshake(cmd.Name()) {
		h.n.Add(1)
	}
	return ctx, nil
}

func (h *countHook) AfterProcess(context.Context, redis.Cmder) error { return nil }

func (h *countHook) BeforeProcessPipeline(ctx context.Context, _ []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (h *countHook) AfterProcessPipeline(context.Context, []redis.Cmder) error { return nil }

func TestImageReadsCappedOutsidePipeline(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("*"))
	keys := make([]string, 500)
	kv := make([]any, 0, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		kv = append(kv, keys[i], "v")
	}
	c.MSet(context.Background(), kv...)
	ch := &countHook{}
	c.AddHook(ch)
	if n, err := c.Del(ctx, keys...).Result(); err != nil || n != 500 {
		panic(fmt.Sprint(n, err))
	}
	// 1 DEL + at most 20 keys x (TYPE+GET before, TYPE after).
	if n := ch.n.Load(); n > 1+20*3 {
		panic(fmt.Sprintf("one DEL of 500 keys cost %d round trips", n))
	}
	e := rec.Events()[0]
	if e.Attrs["image_skipped"] != "cap" || len(e.Mutations) != 500 {
		panic(fmt.Sprintf("attrs=%v mutations=%d", e.Attrs, len(e.Mutations)))
	}
	imaged := 0
	for _, m := range e.Mutations {
		if !m.Before.IsZero() {
			imaged++
		}
	}
	if imaged != 20 {
		panic(fmt.Sprintf("before-images=%d, want the default cap of 20", imaged))
	}
}

func TestImageDeadline(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("*"), WithImageDeadline(time.Nanosecond))
	if err := c.MSet(ctx, "a", "1", "b", "2").Err(); err != nil {
		panic(err)
	}
	e := rec.Events()[0]
	if e.Attrs["image_skipped"] != "deadline" || len(e.Mutations) != 2 || !e.Mutations[0].Before.IsZero() {
		panic(fmt.Sprintf("%+v", e))
	}
	if string(e.Mutations[1].After.Inline) != `"2"` {
		panic(fmt.Sprintf("without an image the argument is the after value: %s", e.Mutations[1].After.Inline))
	}
}

func TestCollectionImagesCapped(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("big:*"))
	core.Configure(core.Config{Service: "svc", Enabled: true, MaxInlineBytes: 1 << 20}) // keep the images whole
	bg := context.Background()
	members := make([]any, 1500)
	fields := make([]any, 0, 3000)
	for i := range members {
		members[i] = fmt.Sprintf("m%d", i)
		fields = append(fields, fmt.Sprintf("f%d", i), "v")
	}
	c.SAdd(bg, "big:set", members...)
	c.HSet(bg, "big:hash", fields...)

	c.SAdd(ctx, "big:set", "one-more")
	c.HSet(ctx, "big:hash", "one-more", "v")
	evs := rec.Events()
	var set []string
	if err := json.Unmarshal(evs[0].Mutations[0].Before.Inline, &set); err != nil || len(set) != redisx.ImageLimit {
		panic(fmt.Sprintf("set image: %d elements, err=%v, truncated=%v", len(set), err, evs[0].Mutations[0].Before.Truncated))
	}
	var hash map[string]string
	if err := json.Unmarshal(evs[1].Mutations[0].Before.Inline, &hash); err != nil || len(hash) != redisx.ImageLimit {
		panic(fmt.Sprintf("hash image: %d fields, err=%v", len(hash), err))
	}
}

func TestPipelineSameKeyImages(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("cnt"))
	c.Set(context.Background(), "cnt", "0", 0)
	ch := &countHook{}
	c.AddHook(ch)
	if _, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "cnt", "1", 0)
		p.Set(ctx, "cnt", "2", 0)
		p.Set(ctx, "cnt", "3", 0)
		return nil
	}); err != nil {
		panic(err)
	}
	evs := rec.Events()
	if len(evs) != 3 {
		panic(fmt.Sprintf("events=%d", len(evs)))
	}
	want := [][2]string{{`"0"`, `"1"`}, {``, `"2"`}, {``, `"3"`}}
	for i, w := range want {
		m := evs[i].Mutations[0]
		if string(m.Before.Inline) != w[0] || string(m.After.Inline) != w[1] {
			t.Errorf("write %d: %s -> %s, want %s -> %s", i, m.Before.Inline, m.After.Inline, w[0], w[1])
		}
	}
	if n := ch.n.Load(); n != 4 { // TYPE+GET before the first write, TYPE+GET after the last
		panic(fmt.Sprintf("image round trips=%d, want 4", n))
	}
}

func TestPipelineAfterImageSkipsFailedWrite(t *testing.T) {
	c, rec, ctx := setup(t, WithBeforeImage("h"))
	c.HSet(context.Background(), "h", "f", "0")
	c.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, "h", "f", "1")
		p.Incr(ctx, "h") // WRONGTYPE
		return nil
	})
	evs := rec.Events()
	m := evs[0].Mutations[0]
	if string(m.Before.Inline) != `{"f":"0"}` || string(m.After.Inline) != `{"f":"1"}` {
		panic(fmt.Sprintf("%s -> %s", m.Before.Inline, m.After.Inline))
	}
	if evs[1].Err == "" || len(evs[1].Mutations) != 0 {
		panic(fmt.Sprintf("%+v", evs[1]))
	}
}
