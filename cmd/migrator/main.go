// Package main provides the database migration CLI tool for Correlator.
//
// This migrator implements a clean architecture with embedded migrations,
// supporting up/down/status/version commands for zero-config deployment.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

// Version information
const (
	version = "1.0.0-dev"
	name    = "migrator"
)

func main() {
	// Command line flags
	var (
		configHelp  = flag.Bool("help", false, "Show help information")
		showVersion = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("%s v%s\n", name, version)
		os.Exit(0)
	}

	// Handle help flag or no arguments
	if *configHelp || len(os.Args) < 2 {
		printUsage()
		os.Exit(0)
	}

	// Parse command from arguments
	command := os.Args[1]

	// Load configuration from environment
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create migration runner
	runner, err := NewMigrationRunner(config)
	if err != nil {
		log.Fatalf("Failed to create migration runner: %v", err)
	}
	defer runner.Close()

	// Execute command
	if err := executeCommand(command, runner); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}

// executeCommand runs the specified migration command
func executeCommand(command string, runner MigrationRunner) error {
	switch command {
	case "up":
		return runner.Up()
	case "down":
		return runner.Down()
	case "status":
		return runner.Status()
	case "version":
		return runner.Version()
	case "drop":
		fmt.Print("WARNING: This will drop all tables. Are you sure? (y/N): ")
		var response string
		fmt.Scanln(&response)
		if response == "y" || response == "Y" {
			return runner.Drop()
		}
		fmt.Println("Operation cancelled.")
		return nil
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

// printUsage displays usage information
func printUsage() {
	fmt.Printf(`%s v%s - Database Migration Tool for Correlator

USAGE:
    %s [OPTIONS] COMMAND

COMMANDS:
    up      Apply all pending migrations
    down    Rollback the last migration
    status  Show migration status
    version Show current migration version
    drop    Drop all tables (requires confirmation)

OPTIONS:
    --help     Show this help message
    --version  Show version information

ENVIRONMENT VARIABLES:
    DATABASE_URL    PostgreSQL connection string (REQUIRED)
    
    MIGRATIONS_PATH Path to migration files directory  
                   (default: ./migrations)
    
    MIGRATION_TABLE Name of migration tracking table
                   (default: schema_migrations)

EXAMPLES:
    %s up                    # Apply all pending migrations
    %s status               # Show current migration status
    %s down                 # Rollback last migration
    %s --version           # Show version information

For zero-config deployment, run without environment variables to use defaults.
`, name, version, name, name, name, name, name)
}
