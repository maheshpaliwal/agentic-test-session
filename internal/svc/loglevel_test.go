package svc

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// An error is logged at error level, never as a warning: a warning that
// carries an error is the line an operator filters out and then needs. The
// services' program code is read for Warn calls with an error attribute.
func TestErrorsAreNotLoggedAsWarnings(t *testing.T) {
	// The call up to the line that closes it; none of them nests a call that
	// ends a line.
	call := regexp.MustCompile(`(?s)\.Warn(?:Context)?\(.*?\)\n`)
	carriesErr := regexp.MustCompile(`"err(?:or)?",|\berr\b\)`)
	calls := 0
	for _, dir := range []string{"../../collector", "../../controlplane", "../../sdk", "../../cmd", "../../dashboard"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, m := range call.FindAll(src, -1) {
				calls++
				if carriesErr.Match(m) {
					t.Errorf("%s logs an error at warn level; use Error:\n\t%s", path, strings.TrimSpace(string(m)))
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	if calls == 0 { // the scan saw no Warn call at all: it reads the wrong place
		panic(fmt.Sprintf("no Warn call found under %s", "collector, controlplane, sdk, cmd, dashboard"))
	}
}
