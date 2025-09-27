// Package main provides the Correlator incident correlation service.
//
// This is the main correlation engine service that processes OpenLineage events
// and correlates test failures with job runs to provide < 5minute incident response.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/correlator-io/correlator/internal/api"
)

// Version information.
const (
	version = "1.0.0-dev"
	name    = "correlator"
)

func main() {
	versionFlag := flag.Bool("version", false, "show version information")
	flag.Parse()

	if *versionFlag {
		log.Printf("%s v%s\n", name, version)
		os.Exit(0)
	}

	serverConfig := api.LoadServerConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: serverConfig.LogLevel,
	}))

	logger.Info("Starting Correlator service",
		slog.String("service", name),
		slog.String("version", version),
	)

	logger.Info("Loaded server configuration",
		slog.String("host", serverConfig.Host),
		slog.Int("port", serverConfig.Port),
		slog.Duration("read_timeout", serverConfig.ReadTimeout),
		slog.Duration("write_timeout", serverConfig.WriteTimeout),
		slog.Duration("shutdown_timeout", serverConfig.ShutdownTimeout),
		slog.String("log_level", serverConfig.LogLevel.String()),
	)

	// Create and start HTTP server
	server := api.NewServer(serverConfig)

	if err := server.Start(); err != nil {
		logger.Error("Server failed to start",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	logger.Info("Correlator service stopped")
}
