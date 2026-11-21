package tsessiontest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

// A control plane (or anything answering for it) chooses the session id; it
// must not be able to place the artifact outside the directory.
func TestArtifactStaysInsideItsDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "artifacts")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/sessions" {
			json.NewEncoder(w).Encode(map[string]any{"id": "/../../outside", "state": "ACTIVE"})
			return
		}
		w.Write([]byte("{}\n"))
	}))
	defer srv.Close()
	s, err := client.Start(context.Background(), client.Options{BaseURL: srv.URL, CollectorURL: srv.URL})
	if err != nil {
		panic(err)
	}
	p, err := saveArtifact(context.Background(), s, dir, "TestX")
	if _, serr := os.Stat(filepath.Join(root, "outside.ndjson")); serr == nil {
		panic(fmt.Sprintf("artifact written outside %s: %s", dir, p))
	}
	if err == nil && filepath.Dir(p) != dir {
		panic(fmt.Sprintf("artifact path %s is not inside %s", p, dir))
	}
}
