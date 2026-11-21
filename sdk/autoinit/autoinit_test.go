package autoinit

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/core"
)

func TestNoopWhenDisabled(t *testing.T) {
	// Importing the package (init already ran without TSESSION_ENABLED=true)
	// must leave everything untouched.
	if core.Enabled() {
		panic("core enabled")
	}
	if _, ok := http.DefaultTransport.(*http.Transport); !ok {
		panic(fmt.Sprintf("DefaultTransport replaced: %T", http.DefaultTransport))
	}
}

// TSESSION_OVERWRITE_TRACEPARENT is read by init, so the test runs itself
// again as a child process with the environment set and asks the child what
// core was configured with.
func TestOverwriteTraceparentEnv(t *testing.T) {
	if os.Getenv("AUTOINIT_TEST_CHILD") == "1" {
		if !core.Enabled() {
			panic("child: SDK not enabled")
		}
		fmt.Printf("overwrite=%v\n", core.GetConfig().OverwriteTraceparent)
		return
	}
	for _, tc := range []struct {
		val  string
		want bool
	}{{"true", true}, {"", false}, {"1", false}, {"TRUE", false}} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestOverwriteTraceparentEnv$", "-test.v")
		cmd.Env = append(os.Environ(), "AUTOINIT_TEST_CHILD=1", "TSESSION_ENABLED=true", "TSESSION_SERVICE=child",
			// Nothing listens here: the SDK only retries in the background.
			"TSESSION_CONTROLPLANE_URL=http://127.0.0.1:1", "TSESSION_COLLECTOR_URL=http://127.0.0.1:1",
			"TSESSION_OVERWRITE_TRACEPARENT="+tc.val)
		out, err := cmd.CombinedOutput()
		if err != nil {
			panic(fmt.Sprintf("child (%q): %v\n%s", tc.val, err, out))
		}
		if want := fmt.Sprintf("overwrite=%v\n", tc.want); !strings.Contains(string(out), want) {
			t.Errorf("TSESSION_OVERWRITE_TRACEPARENT=%q: want %q in child output:\n%s", tc.val, want, out)
		}
	}
}

func TestBindPriorityEnvParse(t *testing.T) {
	got := core.ParseBindPriority("header:X-User-Id,jwt:sub,bad")
	if len(got) != 2 || got[0].String() != "header:x-user-id" || got[1].String() != "jwt_claim:sub" {
		panic(fmt.Sprintf("%v", got))
	}
}
