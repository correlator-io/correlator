// Package main provides the database migration CLI tool for Correlator.
//
// This migrator implements a clean architecture with embedded migrations,
// supporting up/down/status/version commands for zero-config deployment.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

// Build-time information variables (set via -ldflags during compilation).
//
//nolint:gochecknoglobals // Required for build-time version injection via -ldflags -X
var (
	version   = "1.0.0-dev" // Version of the migrator (unexported for clean API)
	gitCommit = "unknown"   // Git commit hash
	buildTime = "unknown"   // Build timestamp
	name      = "migrator"  // Application name
)

// Version returns the build version.
func Version() string { return version }

// GitCommit returns the git commit hash.
func GitCommit() string { return gitCommit }

// BuildTime returns the build timestamp.
func BuildTime() string { return buildTime }

// Name returns the application name.
func Name() string { return name }

var (
	// ErrUnknownCommand is a custom error.
	ErrUnknownCommand = errors.New("unknown command")
	// ErrDropRequiresForce is returned when drop command is used without --force flag.
	ErrDropRequiresForce = errors.New(
		"drop command requires --force flag for safety (this will destroy all data)",
	)
)

func main() {
	// Command line flags
	var (
		configHelp  = flag.Bool("help", false, "Show help information")
		showVersion = flag.Bool("version", false, "Show version information")
		force       = flag.Bool("force", false, "Force dangerous operations without confirmation")
	)
	flag.Parse()

	// Handle version flag
	if *showVersion {
		printVersionInfo()
		os.Exit(0)
	}

	// Handle help flag
	if *configHelp {
		printUsage()
		os.Exit(0)
	}

	// Get non-flag arguments after flag parsing
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Parse command from non-flag arguments
	command := args[0]

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

	defer func() {
		_ = runner.Close()
	}()

	// Execute command
	err = executeCommand(command, runner, *force)
	if err != nil {
		log.Printf("Migration failed: %v\n", err)
	}
}

// executeCommand runs the specified migration command.
func executeCommand(command string, runner MigrationRunner, force bool) error {
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
		if !force {
			return ErrDropRequiresForce
		}

		return runner.Drop()
	default:
		return fmt.Errorf("%w: %s", ErrUnknownCommand, command)
	}
}

// getMaxSchemaVersion automatically detects the highest migration sequence number
// from embedded migration files, enabling zero-config schema version tracking.
func getMaxSchemaVersion() int {
	embeddedMigration := NewEmbeddedMigration(nil)

	files, err := embeddedMigration.ListEmbeddedMigrations()
	if err != nil {
		return 0 // If we can't read migrations, assume no schema
	}

	maxSequence := 0

	for _, filename := range files {
		matches := migrationFilenameRegex.FindStringSubmatch(filename)
		if len(matches) >= expectedRegexMatches-2 { // Need at least sequence + name parts
			if sequence, err := strconv.Atoi(matches[1]); err == nil && sequence > maxSequence {
				maxSequence = sequence
			}
		}
	}

	return maxSequence
}

// printVersionInfo displays comprehensive version information.
func printVersionInfo() {
	log.Printf("%s v%s", Name(), Version())
	log.Printf("Git Commit: %s", GitCommit())
	log.Printf("Build Time: %s", BuildTime())
	log.Printf("Max Schema Version: v0.0.%d", getMaxSchemaVersion())
	log.Printf("Database Migration Tool for Correlator")
}

// printUsage displays usage information.
func printUsage() {
	log.Printf(`%s v%s - Database Migration Tool for Correlator

USAGE:
    %s [OPTIONS] COMMAND

COMMANDS:
    up      Apply all pending migrations
    down    Rollback the last migration
    status  Show migration status
    version Show current migration version
    drop    Drop all tables (DESTRUCTIVE - requires --force flag)

OPTIONS:
    --help     Show this help message
    --version  Show version information
    --force    Force dangerous operations without confirmation

ENVIRONMENT VARIABLES:
    DATABASE_URL    PostgreSQL connection string (REQUIRED)

    MIGRATION_TABLE Name of migration tracking table
                   (default: schema_migrations)

EXAMPLES:
    %s up                    # Apply all pending migrations
    %s status               # Show current migration status
    %s down                 # Rollback last migration
    %s drop --force         # Drop all tables (DESTRUCTIVE)
    %s --version           # Show version information

For zero-config deployment, run without environment variables to use defaults.
`, Name(), Version(), Name(), Name(), Name(), Name(), Name(), Name())
}
