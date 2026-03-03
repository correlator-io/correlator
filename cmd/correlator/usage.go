package main

import "fmt"

//nolint:forbidigo
func printUsage() {
	fmt.Printf("correlator v%s — incident correlation engine\n\n", version)
	fmt.Println("Usage: correlator <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  start          Start the Correlator server")
	fmt.Println("  generate-key   Generate an API key for OpenLineage integrations")
	fmt.Println("  version        Show version information")
	fmt.Println("  help           Show this help message")
	fmt.Println()
	fmt.Println("Run 'correlator <command> --help' for more information on a command.")
}
