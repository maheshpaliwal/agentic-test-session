package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// fakeCmd is a command as the tracer sees it.
type fakeCmd struct {
	args  []any
	reply any
	err   error
}

func (c *fakeCmd) Name() string { return strings.ToLower(Str(c.args[0])) }
func (c *fakeCmd) Args() []any  { return c.args }
func (c *fakeCmd) Err() error   { return c.err }

// serverErr is an error reply, as both go-redis versions mark theirs.
type serverErr string

func (e serverErr) Error() string { return string(e) }
func (serverErr) RedisError()     {}

var errNil = errors.New("redis: nil")

// fakeStore is the Redis the image reads see: hashes and strings by key.
type fakeStore struct {
	hashes  map[string]map[string]string
	strings map[string]string
}

func (s *fakeStore) Type(_ context.Context, key string) (string, error) {
	if _, ok := s.hashes[key]; ok {
		return "hash", nil
	}
	if _, ok := s.strings[key]; ok {
		return "string", nil
	}
	return "none", nil
}
func (s *fakeStore) Get(_ context.Context, key string) (string, error) { return s.strings[key], nil }
func (s *fakeStore) HScan(_ context.Context, key string, _ int64) ([]string, bool, error) {
	var kv []string
	for f, v := range s.hashes[key] {
		kv = append(kv, f, v)
	}
	return kv, false, nil
}
func (s *fakeStore) LRange(context.Context, string, int64) ([]string, error) { return nil, nil }
func (s *fakeStore) SScan(context.Context, string, int64) ([]string, bool, error) {
	return nil, false, nil
}
func (s *fakeStore) ZRange(context.Context, string, int64) ([]Z, error) { return nil, nil }

// traced returns a tracer that records into rec inside the session of ctx.
func traced(t *testing.T) (*Tracer, *core.MemRecorder, context.Context) {
	t.Helper()
	rec := &core.MemRecorder{}
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(rec)
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	t.Cleanup(func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) })
	tr := NewTracer()
	tr.Nil = errNil
	tr.PoppedKey = func(Cmd) string { return "" }
	tr.Reply = func(c Cmd) (any, bool, bool) {
		r := c.(*fakeCmd).reply
		_, scalar := r.(int64)
		return r, scalar, r != nil
	}
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: core.NewTraceID()})
	return &tr, rec, ctx
}

// run traces one command; between stands for the server running it.
func run(tr *Tracer, ctx context.Context, cmd *fakeCmd, between func()) {
	o := tr.Begin(ctx, cmd)
	if between != nil {
		between()
	}
	tr.End(ctx, o, cmd, cmd.err)
}

// An error can hold any piece of an argument, in any form: the server quotes
// a prefix, a script builds its own text. Whatever the piece and the form,
// nothing of a masked argument is recorded with the error.
func TestRecordedErrorHoldsNothingOfAMaskedArgument(t *testing.T) {
	tr, rec, ctx := traced(t)
	rng := rand.New(rand.NewSource(1))
	// A digit in every window of four and no hex letter: no window can be
	// part of the fixed text of an event, a hash or an id by chance.
	secret := func() string {
		const letters = "ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ"
		b := make([]byte, 18+3*rng.Intn(6))
		for i := range b {
			if b[i] = letters[rng.Intn(len(letters))]; i%3 == 0 {
				b[i] = byte('0' + rng.Intn(10))
			}
		}
		return string(b)
	}
	reverse := func(s string) string {
		b := []byte(s)
		slices.Reverse(b)
		return string(b)
	}
	forms := []func(string) string{
		func(s string) string { return s },
		strings.ToUpper, strings.ToLower, reverse,
		func(s string) string { return s[:4+rng.Intn(len(s)-4)] }, // a prefix
		func(s string) string { return s[rng.Intn(len(s)-4):] },   // a suffix
		func(s string) string { i := rng.Intn(len(s) - 6); return s[i : i+4+rng.Intn(len(s)-i-4)] },
	}
	texts := []string{
		"ERR unknown command 'foo.bar', with args beginning with: '%s' ",
		"ERR Error running script (call to f_1): @user_script:1: user_script:1: %s",
		"bad %s", "%s", "WRONGTYPE %s", "%s ERR", "NOPERM this user has no permissions to access the '%s' key",
	}
	const cases = 2000
	for range cases {
		s := secret()
		var cmd *fakeCmd
		switch rng.Intn(5) {
		case 0: // a secret key name
			cmd = &fakeCmd{args: []any{"get", "session:" + s}}
		case 1: // a value under a sensitive key
			cmd = &fakeCmd{args: []any{"set", "password:alice", s}}
		case 2: // a command the tracer does not know
			cmd = &fakeCmd{args: []any{"foo.bar", "x", "Session:" + s, "y"}}
		case 3: // script KEYS
			cmd = &fakeCmd{args: []any{"eval", "return 1", "1", "sess:" + s, "v"}}
		default: // script ARGV under a sensitive key
			cmd = &fakeCmd{args: []any{"evalsha", "ffff", "1", "user:1:token", s}}
		}
		echoed := forms[rng.Intn(len(forms))](s)
		text := strings.Replace(texts[rng.Intn(len(texts))], "%s", echoed, 1)
		if rng.Intn(4) == 0 {
			cmd.err = errors.New(text) // not an error reply: a client-side failure
		} else {
			cmd.err = serverErr(text)
		}
		from := len(rec.Events())
		run(tr, ctx, cmd, nil)
		evs := rec.Events()[from:]
		if len(evs) != 1 || evs[0].Err == "" {
			t.Fatalf("%v: recorded %+v", cmd.args, evs)
		}
		b, err := json.Marshal(evs[0])
		if err != nil {
			t.Fatal(err)
		}
		for _, form := range []string{s, strings.ToUpper(s), strings.ToLower(s), reverse(s)} {
			for i := 0; i+4 <= len(form); i++ {
				if strings.Contains(string(b), form[i:i+4]) {
					t.Fatalf("%v failing with %q records %q of the secret %q: %s", cmd.args, text, form[i:i+4], s, b)
				}
			}
		}
	}
}

func TestRecordedError(t *testing.T) {
	tr, rec, ctx := traced(t)
	tr.TxFailed = errors.New("redis: transaction failed")
	closed := errors.New("redis: client is closed")
	tr.FixedErrs = []error{closed}
	const withheld = " [text withheld: the command has masked arguments]"
	secret := []any{"get", "session:Ab9S3CR3TxYz"}
	for _, tc := range []struct {
		args []any
		err  error
		want string
	}{
		// a masked argument: the code and a known phrase
		{secret, serverErr("WRONGTYPE Operation against a key holding the wrong kind of value"),
			"WRONGTYPE Operation against a key holding the wrong kind of value" + withheld},
		{secret, serverErr("ERR unknown command 'get', with args beginning with: 'session:Ab9"), "ERR unknown command" + withheld},
		{secret, serverErr("NOSCRIPT No matching script. Please use EVAL."), "NOSCRIPT No matching script" + withheld},
		{secret, serverErr("MOVED 3999 127.0.0.1:6381"), "MOVED" + withheld},
		{secret, serverErr("ERR something new about session:Ab9S3CR3TxYz"), "ERR" + withheld},
		{secret, serverErr("SECRETWORD is not a code"), "error" + withheld},
		{secret, serverErr("bad session:Ab9S3"), "error" + withheld},
		// client-side errors with a fixed text
		{secret, context.DeadlineExceeded, "context deadline exceeded"},
		{secret, tr.TxFailed, "redis: transaction failed"},
		{secret, closed, "redis: client is closed"},
		{secret, errors.New("redis: can't parse reply=\"Ab9S3CR3T\""), "error" + withheld},
		// nothing masked: the text, scrubbed like a value
		{[]any{"get", "user:1"}, serverErr("WRONGTYPE Operation against a key holding the wrong kind of value"),
			"WRONGTYPE Operation against a key holding the wrong kind of value"},
		{[]any{"eval", "return redis.call('GET', 'k')", "0"}, serverErr("ERR value of 'session:Ab9S3CR3TxYz' password=hunter2"),
			"ERR value of 'session:" + tr.ShowKey("s1", "session:Ab9S3CR3TxYz")[len("session:"):] + "' password=[REDACTED]"},
	} {
		from := len(rec.Events())
		run(tr, ctx, &fakeCmd{args: tc.args, err: tc.err}, nil)
		if got := rec.Events()[from].Err; got != tc.want {
			t.Errorf("%v failing with %q is recorded as %q, want %q", tc.args, tc.err, got, tc.want)
		}
	}
}

// The before and after images are two reads around the command. A change
// another client makes in between ends up in the diff: the event is flagged
// when the diff holds something the command cannot have done.
func TestConcurrentWriterIsFlagged(t *testing.T) {
	tr, rec, ctx := traced(t)
	store := &fakeStore{hashes: map[string]map[string]string{"user:1": {"name": "ann"}}, strings: map[string]string{}}
	tr.Reader, tr.Patterns = store, []string{"user:*"}
	flagged := func() bool {
		evs := rec.Events()
		return slices.Contains(evs[len(evs)-1].Flags, "diff_concurrent")
	}

	hset := &fakeCmd{args: []any{"hset", "user:1", "name", "bob"}, reply: int64(0)}
	run(tr, ctx, hset, func() { store.hashes["user:1"]["name"] = "bob" })
	if flagged() {
		t.Error("the command's own change is flagged")
	}
	run(tr, ctx, hset, func() {
		store.hashes["user:1"]["name"] = "bob"
		store.hashes["user:1"]["visits"] = "7" // another client
	})
	if evs := rec.Events(); !flagged() || len(evs[len(evs)-1].Mutations[0].Diff) != 1 {
		t.Errorf("another client's field in the diff: flags %v, %+v", evs[len(evs)-1].Flags, evs[len(evs)-1].Mutations)
	}

	set := &fakeCmd{args: []any{"set", "user:2", "mine"}, reply: "OK"}
	run(tr, ctx, set, func() { store.strings["user:2"] = "mine" })
	if flagged() {
		t.Error("the command's own value is flagged")
	}
	run(tr, ctx, set, func() { store.strings["user:2"] = "theirs" })
	if !flagged() {
		t.Error("the key does not hold what SET wrote")
	}

	del := &fakeCmd{args: []any{"del", "user:2"}, reply: int64(1)}
	run(tr, ctx, del, func() { store.strings["user:2"] = "again" }) // deleted, then written by another client
	if !flagged() {
		t.Error("the key holds a value after DEL")
	}
	run(tr, ctx, del, func() { delete(store.strings, "user:2") })
	if flagged() {
		t.Error("a plain DEL is flagged")
	}
	if !core.UncertainFlag("diff_concurrent") {
		t.Error("diff_concurrent does not mark the mutation as uncertain")
	}
}
