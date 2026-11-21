package dashboard

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
)

// flagSites are the ways the code puts a flag on an event: the SDK wrappers'
// flag("x") and Span.AddFlag("x"), a named constant (FlagAttrLimit = "x"),
// and the literals the collector and the exporter append.
var flagSites = []*regexp.Regexp{
	regexp.MustCompile(`(?:\.flag|AddFlag)\("([a-z_:]+)"\)`),
	regexp.MustCompile(`\b[fF]lag[A-Za-z]*\s+= "([a-z_:]+)"`),
	regexp.MustCompile(`Flags(?:\.\.\.\))?, "([a-z_:]+)"\)`),
	regexp.MustCompile(`Flags: \[\]string\{"([a-z_:]+)"\}`),
}

// emittedFlags scans the program code of the SDKs and the collector for the
// flags it can put on an event.
func emittedFlags(t *testing.T) set.Set[string] {
	t.Helper()
	flags := set.Set[string]{}
	for _, dir := range []string{"../contrib", "../internal", "../core", "../sdk", "../thttp", "../collector"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, re := range flagSites {
				for _, m := range re.FindAllSubmatch(src, -1) {
					flags.Add(string(m[1]))
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	if len(flags) < 30 { // the scan found next to nothing: the code moved, and the test would pass on air
		panic(fmt.Sprintf("only %d flags found in the code: %v", len(flags), set.Sorted(flags)))
	}
	return flags
}

// jsKeys returns the keys of the object literal that follows decl in a file
// of the web app.
func jsKeys(t *testing.T, file, decl string) set.Set[string] {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}
	_, rest, ok := strings.Cut(string(src), decl)
	if !ok {
		panic(fmt.Sprintf("%s: no %q", file, decl))
	}
	body, _, ok := strings.Cut(rest, "};")
	if !ok {
		panic(fmt.Sprintf("%s: %q is not closed", file, decl))
	}
	keys := set.Set[string]{}
	for _, m := range regexp.MustCompile(`[\s{,']([a-z_]+(?::[a-z_]+)?)'?: `).FindAllStringSubmatch(body, -1) {
		keys.Add(m[1])
	}
	return keys
}

// A flag the dashboard has no words for is a bare token to its reader. Every
// flag the code can emit has help text, under its full name (a reason after a
// colon included).
func TestFlagHelpCoversEveryFlag(t *testing.T) {
	help := jsKeys(t, "web/app.js", "const FLAG_HELP = {")
	for _, f := range set.Sorted(emittedFlags(t)) {
		if !help.Has(f) {
			t.Errorf("flag %q has no help text in FLAG_HELP (web/app.js)", f)
		}
	}
}

// The dashboard marks the same changes uncertain as the collector counts:
// util.js mirrors core.UncertainFlag.
func TestUncertainFlagsMirrorCore(t *testing.T) {
	js := jsKeys(t, "web/util.js", "const UNCERTAIN = {")
	if len(js) == 0 {
		panic("no keys read from UNCERTAIN")
	}
	all := emittedFlags(t)
	all.AddSeq(maps.Keys(js))
	for _, f := range set.Sorted(all) {
		base, _, _ := strings.Cut(f, ":")
		if got, want := js.Has(base), core.UncertainFlag(f); got != want {
			t.Errorf("flag %q: uncertain in util.js = %v, core.UncertainFlag = %v", f, got, want)
		}
	}
}
