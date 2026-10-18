package thttp

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type benchNopRec struct{}

func (benchNopRec) Record(core.Event) {}

type benchRW struct{ h http.Header }

func (w *benchRW) Header() http.Header         { return w.h }
func (w *benchRW) Write(p []byte) (int, error) { return len(p), nil }
func (w *benchRW) WriteHeader(int)             {}

// benchJSON builds a JSON object of about n bytes with some sensitive keys.
func benchJSON(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"user":"u1","password":"synthetic-pw","token":"synthetic-tok","items":[`)
	for i := 0; b.Len() < n-40; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"sku":"S%06d","qty":%d,"note":"hello world"}`, i, i%7)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

func benchHandler(resp []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})
}

func runHandlerBench(b *testing.B, enabled bool, session string, size int) {
	core.Configure(core.Config{Service: "svc", Enabled: enabled})
	core.SetRecorder(benchNopRec{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()
	body := benchJSON(size)
	h := Handler(benchHandler(body))
	req, _ := http.NewRequest("POST", "http://x/orders?a=1", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer synthetic")
	if session != "" {
		req.Header.Set(core.KeySession, session)
	}
	w := &benchRW{h: http.Header{}}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Body = io.NopCloser(bytes.NewReader(body))
		for k := range w.h {
			delete(w.h, k)
		}
		h.ServeHTTP(w, req)
	}
}

func BenchmarkHandler(b *testing.B) {
	for _, sz := range []struct {
		n string
		v int
	}{{"100B", 100}, {"8KB", 8 << 10}, {"64KB", 64 << 10}} {
		b.Run("disabled/"+sz.n, func(b *testing.B) { runHandlerBench(b, false, "s1", sz.v) })
		b.Run("untraced/"+sz.n, func(b *testing.B) { runHandlerBench(b, true, "", sz.v) })
		b.Run("active/"+sz.n, func(b *testing.B) { runHandlerBench(b, true, "s1", sz.v) })
	}
}
