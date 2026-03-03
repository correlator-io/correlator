package main

import "fmt"

// Build-time information variables (set via -ldflags during compilation).
//
//nolint:gochecknoglobals // Required for build-time version injection via -ldflags -X
var (
	version   = "0.0.1-dev" // Version of correlator (set at build time)
	gitCommit = "unknown"   // Git commit hash (set at build time)
	buildTime = "unknown"   // Build timestamp (set at build time)
)

//nolint:forbidigo
func runVersion() {
	fmt.Printf("correlator v%s\n", version)
	fmt.Printf("Git Commit: %s\n", gitCommit)
	fmt.Printf("Build Time: %s\n", buildTime)
}
