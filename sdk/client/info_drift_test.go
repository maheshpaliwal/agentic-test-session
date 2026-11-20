package client_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/maheshpaliwal/agentic-test-session/controlplane"
	"github.com/maheshpaliwal/agentic-test-session/internal/set"
	"github.com/maheshpaliwal/agentic-test-session/sdk/client"
)

func jsonFields(t reflect.Type) set.Set[string] {
	out := set.Set[string]{}
	for i := 0; i < t.NumField(); i++ {
		if name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); name != "" && name != "-" {
			out.Add(name)
		}
	}
	return out
}

// client.Info mirrors the control plane's session document by hand; a field
// added there must be added here, or clients silently lose it.
func TestInfoMatchesControlPlaneSession(t *testing.T) {
	cp, cl := jsonFields(reflect.TypeOf(controlplane.Session{})), jsonFields(reflect.TypeOf(client.Info{}))
	for f := range cp {
		if !cl.Has(f) {
			t.Errorf("controlplane.Session field %q is missing from client.Info", f)
		}
	}
	for f := range cl {
		if !cp.Has(f) {
			t.Errorf("client.Info field %q does not exist on controlplane.Session", f)
		}
	}
}
