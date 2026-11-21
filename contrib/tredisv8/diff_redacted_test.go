package tredisv8

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDiffSaysARedactedValueChanged(t *testing.T) {
	servers(t, func(t *testing.T, addr string) {
		c, rec, ctx := setupAt(t, addr, WithBeforeImage("user:*", "doc:*", "tags"))

		c.HSet(ctx, "user:1", "password", "hunter2", "name", "ann")
		c.HSet(ctx, "user:1", "password", "hunter3")
		wantDiff(t, rec, "secret field changed", `password: "[REDACTED]" -> "[REDACTED]"`)
		m := rec.Events()[1].Mutations[0]
		if string(m.Before.Inline) != string(m.After.Inline) {
			t.Errorf("the images differ: %s -> %s", m.Before.Inline, m.After.Inline)
		}
		c.HSet(ctx, "user:1", "password", "hunter3", "name", "bob")
		wantDiff(t, rec, "secret field rewritten with the same value", `name: "ann" -> "bob"`)

		c.Set(ctx, "doc:1", `{"name":"ann","password":"hunter2"}`, 0)
		c.Set(ctx, "doc:1", `{"name":"ann","password":"hunter3"}`, 0)
		wantDiff(t, rec, "secret inside a JSON value changed",
			`value: "{\"name\":\"ann\",\"password\":\"[REDACTED]\"}" -> "{\"name\":\"ann\",\"password\":\"[REDACTED]\"}"`)

		c.SAdd(ctx, "tags", "Authorization: Bearer hunter2")
		c.SRem(ctx, "tags", "Authorization: Bearer hunter2")
		if d := diffOf(t, rec); len(d) != 1 || !strings.HasPrefix(d[0], "removed: [") {
			t.Errorf("redacted set member removed: %q", d)
		}

		// The value of a sensitive key is masked without being read.
		c.Set(ctx, "user:1:password", "hunter2", 0)
		c.Set(ctx, "user:1:password", "hunter3", 0)
		wantDiff(t, rec, "sensitive key")
		evs := rec.Events()
		if a := evs[len(evs)-1].Attrs["diff_skipped"]; a != "sensitive_key" {
			t.Errorf("diff_skipped=%q", a)
		}

		b, _ := json.Marshal(evs)
		if strings.Contains(string(b), "hunter") {
			t.Errorf("a secret is recorded: %s", b)
		}
	})
}
