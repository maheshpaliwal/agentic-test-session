package thttp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

type slowBody struct {
	n int
}

func (s *slowBody) Read(p []byte) (int, error) {
	if s.n <= 0 {
		return 0, io.EOF
	}
	s.n--
	time.Sleep(time.Millisecond)
	n := copy(p, bytes.Repeat([]byte("x"), 512))
	return n, nil
}

// The server answers before the request body is fully sent: http.Transport
// keeps reading the (tee'd) request body on its write goroutine while the span
// is finished from the goroutine that closed the response body. Run with
// -race: the capture buffer must be safe for that.
func TestClientBodyCaptureIsSafeWhenReplyComesEarly(t *testing.T) {
	core.Configure(core.Config{Service: "svc", Enabled: true})
	core.SetRecorder(&core.MemRecorder{})
	core.SetSessions(core.StaticSessions{Active: map[string]bool{"s1": true}})
	defer func() { core.Configure(core.Config{}); core.SetRecorder(nil); core.SetSessions(nil) }()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("early"))
		_ = rc.Flush()
		io.Copy(io.Discard, r.Body)
	}))
	defer srv.Close()
	cl := &http.Client{Transport: Transport(&http.Transport{})}
	ctx := core.ContextWith(context.Background(), core.SpanContext{SessionID: "s1", TraceID: "t1"})
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL, &slowBody{n: 40})
		req.Header.Set("Content-Type", "text/plain")
		resp, err := cl.Do(req)
		if err != nil {
			t.Logf("do: %v", err)
			continue
		}
		buf := make([]byte, 5)
		io.ReadFull(resp.Body, buf)
		resp.Body.Close() // caller is done with the response; span finishes here
	}
	time.Sleep(150 * time.Millisecond)
}
