package thttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

// recordedReq serves one request through a traced handler that drains and
// echoes the body, and returns the recorded request and response bodies.
func recordedReq(t *testing.T, rec *core.MemRecorder, contentType string, body io.Reader) (req, resp string) {
	t.Helper()
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		io.Copy(w, r.Body)
	}))
	r := httptest.NewRequest("POST", "/login", body)
	r.Header.Set(core.KeySession, "s1")
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	h.ServeHTTP(httptest.NewRecorder(), r)
	evs := kind(rec.Events(), core.KindAPIIn)
	if len(evs) == 0 {
		panic("no api.in recorded")
	}
	e := evs[len(evs)-1]
	return recordedBody(t, e.Req.Inline), recordedBody(t, e.Resp.Inline)
}

// recordedBody extracts the "body" of a recorded message: the decoded text
// for string bodies, the raw JSON otherwise.
func recordedBody(t *testing.T, inline []byte) string {
	t.Helper()
	var m struct {
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(inline, &m); err != nil {
		panic(fmt.Sprintf("payload %s: %v", inline, err))
	}
	var text string
	if json.Unmarshal(m.Body, &text) == nil {
		return text
	}
	return string(m.Body)
}

func TestBodiesWithoutRedactorNotRecordedVerbatim(t *testing.T) {
	rec := setup(t, active("s1"))

	var mp bytes.Buffer
	mw := multipart.NewWriter(&mp)
	mw.WriteField("password", "SYNTHSECRET")
	mw.Close()
	mpLen := mp.Len()

	const xml = `<login><password>SYNTHSECRET</password></login>`
	cases := []struct {
		name, ct string
		body     io.Reader
		want     string // placeholder expected in the recorded body
	}{
		{"multipart", mw.FormDataContentType(), &mp, "<multipart/form-data, " + strconv.Itoa(mpLen) + " bytes>"},
		{"text/xml", "text/xml; charset=utf-8", strings.NewReader(xml), "<text/xml, " + strconv.Itoa(len(xml)) + " bytes>"},
		{"application/soap+xml", "application/soap+xml", strings.NewReader(xml), "<application/soap+xml, "},
		{"xml without content type", "", strings.NewReader(xml), "<text/xml, "},
		{"opaque application type", "application/x-custom", strings.NewReader("password SYNTHSECRET"), "<application/x-custom, "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, resp := recordedReq(t, rec, c.ct, c.body)
			for dir, got := range map[string]string{"req": req, "resp": resp} {
				if strings.Contains(got, "SYNTHSECRET") {
					t.Errorf("%s recorded verbatim: %s", dir, got)
				}
				if !strings.Contains(got, c.want) {
					t.Errorf("%s: want placeholder %q in %s", dir, c.want, got)
				}
			}
		})
	}
}

func TestRedactableBodiesStillRecorded(t *testing.T) {
	rec := setup(t, active("s1"))
	cases := []struct{ name, ct, body, want, leak string }{
		{"json", "application/json", `{"user":"ann","password":"SYNTHSECRET"}`, `"user":"ann"`, "SYNTHSECRET"},
		{"json without content type", "", `{"user":"ann"}`, `"user":"ann"`, ""},
		{"json under another type", "application/octet-stream", `{"user":"ann"}`, `"user":"ann"`, ""},
		{"form", "application/x-www-form-urlencoded", "user=ann&password=SYNTHSECRET", "user=ann", "SYNTHSECRET"},
		{"text", "text/plain", "hello ann password=SYNTHSECRET", "hello ann", "SYNTHSECRET"},
		{"text without content type", "", "hello ann", "hello ann", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, _ := recordedReq(t, rec, c.ct, strings.NewReader(c.body))
			if !strings.Contains(req, c.want) {
				t.Errorf("body dropped: %s", req)
			}
			if c.leak != "" && strings.Contains(req, c.leak) {
				t.Errorf("leak: %s", req)
			}
		})
	}
}

func TestPlaceholderMediaTypeBounded(t *testing.T) {
	l := newLimitBuf()
	l.add([]byte("payload"))
	got, _ := l.body("application/" + strings.Repeat("x", 500) + "; q=1").(string)
	if len(got) > maxMediaType+32 || !strings.HasSuffix(got, ", 7 bytes>") {
		panic(fmt.Sprintf("placeholder = %q", got))
	}
}
