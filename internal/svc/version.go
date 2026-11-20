package svc

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
)

// buildVersion is set at link time by builds that have no VCS information for
// the toolchain to record, such as the container images:
//
//	go build -ldflags "-X github.com/maheshpaliwal/agentic-test-session/internal/svc.buildVersion=v1.2.3"
var buildVersion string

// Version returns the version of the running binary: the one set at link
// time if any, else the main module's ("(devel)" under `go run`) with the VCS
// revision appended when the toolchain recorded one that the version does not
// already contain. goVersion is the Go version it was built with.
func Version() (version, goVersion string) {
	version, goVersion = "unknown", runtime.Version()
	if buildVersion != "" {
		return buildVersion, goVersion
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return version, goVersion
	}
	if bi.Main.Version != "" {
		version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		// Recent toolchains already stamp a pseudo-version that ends in the
		// revision; only add it when the version does not say it.
		if s.Key == "vcs.revision" && len(s.Value) >= 12 && !strings.Contains(version, s.Value[:12]) {
			version += "+" + s.Value[:12]
		}
	}
	return version, goVersion
}

// VersionFlag registers -version on the default flag set. Call the returned
// function right after flag.Parse: when the flag was given it prints
// "<name> <version> (<go version>)" and exits 0.
func VersionFlag(name string) (exitIfSet func()) {
	show := flag.Bool("version", false, "print the version and exit")
	return func() {
		if *show {
			v, g := Version()
			fmt.Printf("%s %s (%s)\n", name, v, g)
			os.Exit(0)
		}
	}
}
