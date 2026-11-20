// Package autoinit enables the tsession SDK when imported for side effects:
//
//	import _ "github.com/maheshpaliwal/agentic-test-session/sdk/autoinit"
//
// It does nothing unless TSESSION_ENABLED is exactly "true". The settings are
// read from TSESSION_SERVICE, TSESSION_INSTANCE, TSESSION_CONTROLPLANE_URL and
// TSESSION_COLLECTOR_URL (see docs/PROTOCOL.md for the rest).
//
// The wrappers (thttp.Handler, tsql.Register, ...) decide once, when they are
// called, whether the SDK is on. Call them from main or from functions it
// calls, which run after every package's init. A package that wraps in a
// package-level variable or an init function must import autoinit itself:
// the order of imports in a source file does not decide initialisation order.
//
// When TSESSION_ENABLED is set, it logs one line at start saying whether the
// SDK records and where it sends; it logs nothing when the variable is unset.
package autoinit

import (
	"cmp"
	"log"
	"os"
	"strings"

	"github.com/maheshpaliwal/agentic-test-session/core"
	"github.com/maheshpaliwal/agentic-test-session/sdk/runtime"
)

func init() {
	switch v := os.Getenv("TSESSION_ENABLED"); {
	case v == "true":
	case v == "":
		return
	default:
		log.Printf("tsession: disabled: TSESSION_ENABLED is %q, and only \"true\" enables the SDK", v)
		return
	}
	service := os.Getenv("TSESSION_SERVICE")
	if service == "" {
		log.Print("tsession: TSESSION_SERVICE is not set; this service's events will carry no service name")
	}
	err := runtime.Start(runtime.Config{
		Service:               service,
		Instance:              os.Getenv("TSESSION_INSTANCE"),
		ControlPlaneURL:       os.Getenv("TSESSION_CONTROLPLANE_URL"),
		CollectorURL:          os.Getenv("TSESSION_COLLECTOR_URL"),
		PatchDefaultTransport: true,
		PropagateHosts:        splitList(os.Getenv("TSESSION_PROPAGATE_HOSTS")),
		BindPriority:          core.ParseBindPriority(os.Getenv("TSESSION_BIND_PRIORITY")),
		NoPropagate:           splitList(os.Getenv("TSESSION_NO_PROPAGATE_HOSTS")),
	})
	if err != nil {
		log.Printf("tsession: disabled: %v", err)
		return
	}
	log.Printf("tsession: enabled for service %q (control plane %s, collector %s)", service,
		cmp.Or(os.Getenv("TSESSION_CONTROLPLANE_URL"), "http://127.0.0.1:7070"), cmp.Or(os.Getenv("TSESSION_COLLECTOR_URL"), "http://127.0.0.1:7071"))
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
