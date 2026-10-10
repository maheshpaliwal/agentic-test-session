package collector

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"

	"tsession/collector/store"
	"tsession/core"
)

// BundleFile describes one file in a bundle manifest.
type BundleFile struct {
	Name   string `json:"name"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Manifest is written next to the NDJSON as manifest.json.
type Manifest struct {
	SessionID  string       `json:"session_id"`
	EventCount int          `json:"event_count"`
	Incomplete bool         `json:"incomplete"`
	Orphaned   []string     `json:"orphaned"`
	SealedAt   string       `json:"sealed_at"`
	Files      []BundleFile `json:"files"`
}

// writeBundle writes <dir>/<session>/{events.ndjson,manifest.json} and
// returns a file:// URI of the bundle directory.
func writeBundle(dir, id string, evs []core.Event, si store.SealInfo) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range evs {
		if err := enc.Encode(&evs[i]); err != nil {
			return "", err
		}
	}
	sum := sha256.Sum256(buf.Bytes())
	man := Manifest{
		SessionID: id, EventCount: len(evs), Incomplete: si.Incomplete, Orphaned: nonNil(si.Orphaned),
		SealedAt: si.SealedAt.Format("2006-01-02T15:04:05Z07:00"),
		Files:    []BundleFile{{Name: "events.ndjson", Bytes: buf.Len(), SHA256: hex.EncodeToString(sum[:])}},
	}
	mb, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(filepath.Join(dir, id))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(abs, "events.ndjson"), buf.Bytes(), 0o640); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(abs, "manifest.json"), append(mb, '\n'), 0o640); err != nil {
		return "", err
	}
	return "file://" + filepath.ToSlash(abs), nil
}
