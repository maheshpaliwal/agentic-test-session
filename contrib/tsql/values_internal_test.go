package tsql

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

type testValuer struct{ v driver.Value }

func (t testValuer) Value() (driver.Value, error) { return t.v, nil }

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("no") }

type panickingValuer struct{}

func (panickingValuer) Value() (driver.Value, error) { panic("no") }

type namedBytes []byte

type stringerID [16]byte

func (stringerID) String() string { return "id-1" }

func TestKeyValueForms(t *testing.T) {
	uuid := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	const uuidText = "01234567-89ab-cdef-0123-456789abcdef"
	var nilValuer *sql.NullString
	for _, tc := range []struct {
		name     string
		in       any
		key, val any // keyValue and toValue
	}{
		{"nil", nil, nil, nil},
		{"int", int64(7), int64(7), int64(7)},
		{"text", "abc", "abc", "abc"},
		{"text bytes", []byte("abc"), "abc", "abc"},
		{"binary", []byte{0xff, 0xfe, 0x00, 0x01}, `\xfffe0001`, "<4 bytes>"},
		{"empty bytes", []byte{}, "", ""},
		{"bytes that read like hex", []byte(`\xfffe`), `\x5c7866666665`, `\xfffe`},
		{"control bytes", []byte{0x00, 0x01}, "\x00\x01", "\x00\x01"},
		{"named byte slice", namedBytes{0xff, 0x01}, `\xff01`, "<2 bytes>"},
		{"byte array", [3]byte{0xff, 0x00, 0x01}, `\xff0001`, "<3 bytes>"},
		{"uuid as 16 bytes", uuid, uuidText, uuidText},
		{"valuer of text", testValuer{uuidText}, uuidText, uuidText},
		{"valuer of bytes", testValuer{[]byte{0xff, 0x01}}, `\xff01`, "<2 bytes>"},
		{"valuer of NULL", sql.NullString{}, nil, nil},
		{"valuer of int", sql.NullInt64{Int64: 5, Valid: true}, int64(5), int64(5)},
		{"nil pointer valuer", nilValuer, nil, nil},
		{"valuer that fails", failingValuer{}, "{}", "{}"},
		{"valuer that panics", panickingValuer{}, "{}", "{}"},
		{"stringer", stringerID{1}, "id-1", "id-1"},
	} {
		if got := keyValue(tc.in); got != tc.key {
			t.Errorf("%s: keyValue = %#v, want %#v", tc.name, got, tc.key)
		}
		if got := toValue(tc.in, maxCellLen); got != tc.val {
			t.Errorf("%s: toValue = %#v, want %#v", tc.name, got, tc.val)
		}
	}
}

// Different keys never share a form, and one key always has the same form;
// a long one is bounded.
func TestKeyValueIsBoundedStableAndInjective(t *testing.T) {
	long := func(fill, last byte, n int) []byte { return append(bytes.Repeat([]byte{fill}, n-1), last) }
	keys := []any{
		[]byte{0xff, 0xfe, 0x01}, []byte{0xff, 0xfe, 0x02}, []byte{0xff, 0xfe}, []byte{0xff, 0xfe, 0x01, 0x00},
		[]byte(`\xfffe01`), `\xfffe010`, []byte("abc"), "abd",
		long(0xff, 1, maxKeyBytes), long(0xff, 2, maxKeyBytes), long(0xff, 1, maxKeyBytes+1), long(0xff, 2, maxKeyBytes+1),
		long(0xff, 1, 1<<20), long(0xff, 2, 1<<20),
		string(long('a', 'b', maxCellLen)), string(long('a', 'c', maxCellLen)),
		string(long('a', 'b', maxCellLen+1)), string(long('a', 'c', maxCellLen+1)), string(long('a', 'b', maxCellLen+2)),
		strings.Repeat("é", maxCellLen), strings.Repeat("é", maxCellLen) + "x",
	}
	seen := map[string]int{}
	for i, k := range keys {
		s, ok := keyValue(k).(string)
		if !ok {
			t.Fatalf("key %d: %#v", i, keyValue(k))
		}
		if j, dup := seen[s]; dup {
			t.Errorf("keys %d and %d are both shown as %.60q", j, i, s)
		}
		seen[s] = i
		if again := keyValue(k); again != s {
			t.Errorf("key %d: %.60q, then %.60q", i, s, again)
		}
		if limit := maxCellLen + 64; len(s) > limit {
			t.Errorf("key %d: %d bytes shown, more than %d", i, len(s), limit)
		}
		if !strings.HasPrefix(s, `\x`) && strings.ContainsRune(s, '�') {
			t.Errorf("key %d: %.60q is not valid text", i, s)
		}
	}
	sum := sha256.Sum256(long(0xff, 1, 100))
	want := `\x` + strings.Repeat("ff", keyPrefixBytes) + "...[100 bytes sha256:" + hex.EncodeToString(sum[:keyHashBytes]) + "]"
	if got := keyValue(long(0xff, 1, 100)); got != want {
		t.Errorf("long binary key: %v, want %s", got, want)
	}
}
