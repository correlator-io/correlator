package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/correlator-io/correlator/internal/storage"
)

const (
	defaultClientID = "default"
	generateTimeout = 10 * time.Second
)

//nolint:forbidigo
func runGenerateKey(args []string) {
	fs := flag.NewFlagSet("generate-key", flag.ExitOnError)
	name := fs.String("name", "", "human-readable name for the API key (required)")
	clientID := fs.String("client-id", defaultClientID, "client identifier for the key")
	expires := fs.Duration("expires", 0, "key expiration duration (e.g., 720h for 30 days; 0 = no expiry)")

	_ = fs.Parse(args)

	// Validate required flags
	if *name == "" {
		fmt.Fprintln(os.Stderr, "Error: --name is required.")
		fmt.Fprintf(os.Stderr, "\nUsage: correlator generate-key --name <name> [--client-id <id>] [--expires <duration>]\n")
		os.Exit(1)
	}

	// Default empty client-id to "default"
	if *clientID == "" {
		*clientID = defaultClientID
	}

	// Generate the plaintext API key
	plaintextKey, err := storage.GenerateAPIKey()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to generate API key: %v\n", err)
		os.Exit(1)
	}

	storageConfig := storage.LoadConfig()

	dbConn, err := storage.NewConnection(storageConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect to database: %v\n", err)
		fmt.Fprintln(os.Stderr, "Ensure DATABASE_URL is set and the database is running.")
		os.Exit(1)
	}

	defer func() {
		_ = dbConn.Close()
	}()

	keyStore, err := storage.NewPersistentKeyStore(dbConn)
	if err != nil {
		_ = dbConn.Close()

		fmt.Fprintf(os.Stderr, "Error: failed to initialize key store: %v\n", err)
		fmt.Fprintln(os.Stderr, "Ensure database migrations have been applied.")
		os.Exit(1) //nolint:gocritic // Explicit cleanup above, defer won't run with os.Exit
	}

	defer func() {
		_ = keyStore.Close()
	}()

	// Build the API key record
	keyID := uuid.New().String()

	apiKey := &storage.APIKey{
		ID:          keyID,
		Key:         plaintextKey,
		ClientID:    *clientID,
		Name:        *name,
		Permissions: []string{"lineage:write"},
		CreatedAt:   time.Now(),
		Active:      true,
	}

	if *expires > 0 {
		expiresAt := time.Now().Add(*expires)
		apiKey.ExpiresAt = &expiresAt
	}

	// Store the key
	ctx, cancel := context.WithTimeout(context.Background(), generateTimeout)
	defer cancel()

	if err := keyStore.Add(ctx, apiKey); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to store API key: %v\n", err)
		os.Exit(1)
	}

	// Print plaintext key to stdout (pipe-friendly)
	fmt.Println(plaintextKey)

	// Print metadata to stderr (human-friendly)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "API key generated successfully.")
	fmt.Fprintf(os.Stderr, "  Name:      %s\n", *name)
	fmt.Fprintf(os.Stderr, "  Client ID: %s\n", *clientID)
	fmt.Fprintf(os.Stderr, "  Key ID:    %s\n", keyID)

	if apiKey.ExpiresAt != nil {
		fmt.Fprintf(os.Stderr, "  Expires:   %s\n", apiKey.ExpiresAt.Format(time.RFC3339))
	} else {
		fmt.Fprintf(os.Stderr, "  Expires:   never\n")
	}
}
