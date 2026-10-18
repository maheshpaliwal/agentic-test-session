package tmongo

import (
	"strings"

	"tsession/core"
)

// redactDoc masks sensitive field names at every depth (core.RedactAny) and
// additionally handles aggregation expression arrays such as
// {"$eq": ["$password", "x"]}, where the sensitive name is a "$field" path
// and the value sits next to it in the array.
func redactDoc(v any) any {
	return exprArrays(core.RedactAny(v))
}

func exprArrays(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = exprArrays(x)
		}
	case []any:
		sens := false
		for _, e := range t {
			if s, ok := e.(string); ok && strings.HasPrefix(s, "$") && !strings.HasPrefix(s, "$$") && sensitivePath(s[1:]) {
				sens = true
			}
		}
		for i, e := range t {
			switch x := e.(type) {
			case string:
				if sens && !strings.HasPrefix(x, "$") {
					t[i] = core.RedactedValue
				}
			case map[string]any, []any:
				t[i] = exprArrays(e)
			default:
				if sens && e != nil {
					t[i] = core.RedactedValue
				}
			}
		}
	}
	return v
}

func sensitivePath(p string) bool {
	if core.IsSensitiveKey(p) {
		return true
	}
	if i := strings.LastIndex(p, "."); i >= 0 {
		return core.IsSensitiveKey(p[i+1:])
	}
	return false
}
