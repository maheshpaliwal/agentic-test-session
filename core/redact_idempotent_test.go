package core

import "testing"

// Text can pass through RedactText more than once (a wrapper scrubs it, then
// Payload does): the second pass must change nothing.
func TestRedactTextIsIdempotent(t *testing.T) {
	for _, in := range []string{
		"password=hunter2&a=1",
		`{"password": "hunter2", "user": "u"`,
		"Authorization: Bearer abcdefghijklmnop",
		"token: abc, secret=xyz; note=ok",
		"<password>hunter2</password>",
	} {
		once := RedactText(in)
		if twice := RedactText(once); twice != once {
			t.Errorf("%q: first pass %q, second pass %q", in, once, twice)
		}
	}
	if got := RedactText("password=hunter2&a=1"); got != "password=[REDACTED]&a=1" {
		t.Errorf("got %q", got)
	}
}
