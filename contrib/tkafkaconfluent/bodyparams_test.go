package tkafkaconfluent

import (
	"encoding/json"
	"strings"
	"testing"
)

// The record value is recorded inside a larger object, where nothing scrubs
// string leaves later: text must be scrubbed here, also when it arrives as a
// JSON string literal.
func TestBodyParamsScrubsText(t *testing.T) {
	for _, body := range []string{
		`"password=hunter2&a=1"`,
		`password=hunter2&a=1`,
		`note password=hunter2 end`,
		`{"password":"hunter2","ok":1}`,
	} {
		b, err := json.Marshal(map[string]any{"value": bodyParams([]byte(body))})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "hunter2") {
			t.Errorf("body %s recorded as %s", body, b)
		}
	}
	if bodyParams(nil) != nil {
		t.Error("an empty value must record nothing")
	}
}
