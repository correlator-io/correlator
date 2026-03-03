// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"time"

	"github.com/correlator-io/correlator/internal/config"
)

// Config holds rate limiter configuration.
//
// Rate limits specify requests per second (RPS) for three tiers:
//   - Global: Applied to all requests
//   - Per-client: Applied to authenticated requests
//   - Unauthenticated: Applied to requests without client ID
//
// Burst capacity allows temporary bursts above sustained rate.
// If burst fields are 0, they are computed automatically as 2 × rate.
type Config struct {
	// Rate limits (requests per second)
	GlobalRPS int // Default: 100
	ClientRPS int // Default: 50
	UnAuthRPS int // Default: 10

	// Optional burst capacity overrides (0 = compute automatically as 2 × rate) using computeBurstCapacity()
	GlobalBurst int // Default: 0 (computed as 2 × GlobalRPS = 200)
	ClientBurst int // Default: 0 (computed as 2 × ClientRPS = 100)
	UnAuthBurst int // Default: 0 (computed as 2 × UnAuthRPS = 20)

	// Memory cleanup configuration
	CleanupInterval time.Duration // Default: 5 minutes
	IdleTimeout     time.Duration // Default: 1 hour
	MaxClients      int           // Default: 10,000
}

// LoadConfig loads middleware config from environment variables with fallback to defaults.
//
// Default burst capacity: 2 × rate (allows 2-second burst)
// Default cleanup: every 5 minutes, removes clients idle >1 hour
// Default max clients: 10,000 (prevents unbounded memory growth).
func LoadConfig() *Config {
	return &Config{
		// Rate limits
		GlobalRPS: config.GetEnvInt("CORRELATOR_GLOBAL_RPS", defaultGlobalRPS),
		ClientRPS: config.GetEnvInt("CORRELATOR_CLIENT_RPS", defaultClientRPS),
		UnAuthRPS: config.GetEnvInt("CORRELATOR_UNAUTH_RPS", defaultUnAuthRPS),
		// Burst overrides (0 = auto-compute)
		GlobalBurst: config.GetEnvInt("CORRELATOR_GLOBAL_BURST", 0),
		ClientBurst: config.GetEnvInt("CORRELATOR_CLIENT_BURST", 0),
		UnAuthBurst: config.GetEnvInt("CORRELATOR_UNAUTH_BURST", 0),
		// Cleanup configuration
		CleanupInterval: config.GetEnvDuration(
			"CORRELATOR_RATE_LIMIT_CLEANUP_INTERVAL", rateLimiterCleanupInterval,
		),
		IdleTimeout: config.GetEnvDuration("CORRELATOR_RATE_LIMIT_IDLE_TIMEOUT", rateLimiterIdleTimeout),
		MaxClients:  config.GetEnvInt("CORRELATOR_RATE_LIMIT_MAX_CLIENTS", maxClients),
	}
}
