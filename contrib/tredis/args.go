package tredis

import (
	"fmt"
	"strings"
)

const maxReqArgs = 64

// str renders one command argument as text.
func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

// reqArgs returns the printable argument vector with credentials masked and
// the number of arguments capped; core.Payload applies size caps/redaction.
func reqArgs(args []any) []string {
	out := make([]string, 0, min(len(args), maxReqArgs+1))
	maskNext := false
	for i, a := range args {
		if i >= maxReqArgs {
			out = append(out, fmt.Sprintf("...(%d more)", len(args)-i))
			break
		}
		s := str(a)
		switch {
		case maskNext:
			s, maskNext = "[REDACTED]", false
		case i > 0 && strings.EqualFold(s, "auth"):
			maskNext = true
		}
		out = append(out, s)
	}
	if len(out) > 0 && strings.EqualFold(out[0], "auth") {
		for i := 1; i < len(out); i++ {
			out[i] = "[REDACTED]"
		}
	}
	return out
}

// spanName builds "GET key" style names, capped in length.
func spanName(args []any, keys []int) string {
	if len(args) == 0 {
		return "redis"
	}
	n := strings.ToUpper(str(args[0]))
	if len(keys) > 0 {
		n += " " + str(args[keys[0]])
	}
	if len(n) > 200 {
		n = n[:200]
	}
	return n
}

// keyPattern collapses id-like segments so mutations of user:1, user:2 group
// under one entity "user:*". The exact key is always kept in Mutation.PK.
func keyPattern(key string) string {
	parts := strings.Split(key, ":")
	for i, p := range parts {
		if idLike(p) {
			parts[i] = "*"
		}
	}
	return strings.Join(parts, ":")
}

func idLike(p string) bool {
	if p == "" {
		return false
	}
	digits, hexy := true, len(p) >= 8
	for _, c := range p {
		if c < '0' || c > '9' {
			digits = false
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' || c == '-') {
			hexy = false
		}
	}
	return digits || hexy
}
