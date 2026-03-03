// Package main provides the Correlator incident correlation service.
package main

import (
	"fmt"
	"os"
)

const minArgs = 2

func main() {
	if len(os.Args) < minArgs {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		runStart(os.Args[2:])
	case "generate-key":
		runGenerateKey(os.Args[2:])
	case "version":
		runVersion()
	case "help", "--help", "-h":
		printUsage()
	default:
		_, _ = fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])

		printUsage()
		os.Exit(1)
	}
}
