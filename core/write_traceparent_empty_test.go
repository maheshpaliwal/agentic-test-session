package core

import "testing"

// With nothing to propagate, WriteTraceparent leaves the carrier alone: empty
// keys would overwrite a context that is there, or add junk to a message
// outside any session.
func TestWriteTraceparentWithNothingToWriteLeavesTheCarrier(t *testing.T) {
	for _, in := range [][2]string{{"", ""}, {"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", ""}, {"", "42"}} {
		c := MapCarrier{KeyTraceparent: "app", "other": "x"}
		WriteTraceparent(c, in[0], in[1])
		if len(c) != 2 || c[KeyTraceparent] != "app" || c["other"] != "x" {
			t.Errorf("WriteTraceparent(%q, %q) changed the carrier: %v", in[0], in[1], c)
		}
	}
}
