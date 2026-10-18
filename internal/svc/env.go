// Package svc holds the process plumbing shared by the tsession binaries:
// environment-backed flag defaults, CSV lists and graceful HTTP serving.
package svc

import (
	"cmp"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env returns the environment variable k, or def when it is unset or empty.
func Env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// EnvInt returns k parsed as an int, or def when unset or malformed.
func EnvInt(k string, def int) int { return envParse(k, def, strconv.Atoi) }

// EnvDur returns k parsed as a duration, or def when unset or malformed.
func EnvDur(k string, def time.Duration) time.Duration { return envParse(k, def, time.ParseDuration) }

// EnvPositiveInt is EnvInt but falls back to def for values <= 0.
func EnvPositiveInt(k string, def int) int { return positive(EnvInt(k, def), def) }

// EnvPositiveDur is EnvDur but falls back to def for values <= 0.
func EnvPositiveDur(k string, def time.Duration) time.Duration { return positive(EnvDur(k, def), def) }

func envParse[T any](k string, def T, parse func(string) (T, error)) T {
	if v, err := parse(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func positive[T cmp.Ordered](v, def T) T {
	var zero T
	if v > zero {
		return v
	}
	return def
}

// SplitCSV splits a comma-separated list, trimming blanks and dropping empties.
func SplitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
