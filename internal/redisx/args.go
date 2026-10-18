// Package redisx holds the Redis-client-independent parts of the tredis and
// tredisv8 wrappers: command classification, argument rendering and redaction.
package redisx

import (
	"fmt"
	"strings"
)

const maxReqArgs = 64

// Str renders one command argument as text.
func Str(v any) string {
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

// CapArgs caps the number of arguments recorded; core.Payload applies size caps.
func CapArgs(in []string) []string {
	if len(in) <= maxReqArgs {
		return in
	}
	out := append([]string(nil), in[:maxReqArgs]...)
	return append(out, fmt.Sprintf("...(%d more)", len(in)-maxReqArgs))
}

// SpanName builds "GET key" style names, capped in length.
func SpanName(args []any, keys []int) string {
	if len(args) == 0 {
		return "redis"
	}
	n := strings.ToUpper(Str(args[0]))
	if len(keys) > 0 {
		n += " " + Str(args[keys[0]])
	}
	if len(n) > 200 {
		n = n[:200]
	}
	return n
}

// KeyPattern collapses id-like segments so mutations of user:1, user:2 group
// under one entity "user:*". The exact key is always kept in Mutation.PK.
func KeyPattern(key string) string {
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
