package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func benchRedactJSON(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"user":"u1","password":"synthetic-pw","token":"synthetic-tok","items":[`)
	for i := 0; b.Len() < n-40; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"sku":"S%06d","qty":%d,"secret":"synthetic","note":"hello"}`, i, i%7)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

var benchSizes = []struct {
	n string
	v int
}{{"100B", 100}, {"8KB", 8 << 10}, {"64KB", 64 << 10}}

func BenchmarkRedactJSON(b *testing.B) {
	for _, s := range benchSizes {
		in := benchRedactJSON(s.v)
		b.Run(s.n, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				RedactJSON(in)
			}
		})
	}
}

func BenchmarkPayload(b *testing.B) {
	Configure(Config{Service: "svc", Enabled: true, MaxInlineBytes: 8192})
	defer Configure(Config{})
	for _, s := range benchSizes {
		in := benchRedactJSON(s.v)
		raw := json.RawMessage(in)
		b.Run("rawjson/"+s.n, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				Payload(raw)
			}
		})
		var m any
		json.Unmarshal(in, &m)
		b.Run("struct/"+s.n, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(in)))
			for i := 0; i < b.N; i++ {
				Payload(m)
			}
		})
	}
}
