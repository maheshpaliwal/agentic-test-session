// Package autoinit enables the tsession SDK when imported for side effects:
//
//	import _ "tsession/sdk/autoinit"
//
// It does nothing unless TSESSION_ENABLED=true.
package autoinit

import (
	"os"

	"tsession/sdk/runtime"
)

func init() {
	if os.Getenv("TSESSION_ENABLED") != "true" {
		return
	}
	_ = runtime.Start(runtime.Config{
		Service:               os.Getenv("TSESSION_SERVICE"),
		Instance:              os.Getenv("TSESSION_INSTANCE"),
		ControlPlaneURL:       os.Getenv("TSESSION_CONTROLPLANE_URL"),
		CollectorURL:          os.Getenv("TSESSION_COLLECTOR_URL"),
		PatchDefaultTransport: true,
	})
}
