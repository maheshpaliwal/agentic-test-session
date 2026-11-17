package svc

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
)

// Version returns the version of the running binary's main module ("(devel)"
// for a build from a working tree) with the VCS revision appended when the
// toolchain recorded one, and the Go version it was built with.
func Version() (version, goVersion string) {
	version, goVersion = "unknown", runtime.Version()
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return version, goVersion
	}
	if bi.Main.Version != "" {
		version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 12 {
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
