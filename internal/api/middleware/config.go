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
//   - Per-plugin: Applied to authenticated requests
//   - Unauthenticated: Applied to requests without plugin ID
//
// Burst capacity allows temporary bursts above sustained rate.
// If burst fields are 0, they are computed automatically as 2 × rate.
type Config struct {
	// Rate limits (requests per second)
	GlobalRPS int // Default: 100
	PluginRPS int // Default: 50
	UnAuthRPS int // Default: 10

	// Optional burst capacity overrides (0 = compute automatically as 2 × rate) using computeBurstCapacity()
	GlobalBurst int // Default: 0 (computed as 2 × GlobalRPS = 200)
	PluginBurst int // Default: 0 (computed as 2 × PluginRPS = 100)
	UnAuthBurst int // Default: 0 (computed as 2 × UnAuthRPS = 20)

	// Memory cleanup configuration
	CleanupInterval time.Duration // Default: 5 minutes
	IdleTimeout     time.Duration // Default: 1 hour
	MaxPlugins      int           // Default: 10,000
}

// LoadConfig loads middleware config from environment variables with fallback to defaults.
//
// Default burst capacity: 2 × rate (allows 2-second burst)
// Default cleanup: every 5 minutes, removes plugins idle >1 hour
// Default max plugins: 10,000 (prevents unbounded memory growth).
func LoadConfig() *Config {
	return &Config{
		// Rate limits
		GlobalRPS: config.GetEnvInt("CORRELATOR_GLOBAL_RPS", defaultGlobalRPS),
		PluginRPS: config.GetEnvInt("CORRELATOR_PLUGIN_RPS", defaultPluginRPS),
		UnAuthRPS: config.GetEnvInt("CORRELATOR_UNAUTH_RPS", defaultUnAuthRPS),

		// Burst overrides (0 = auto-compute)
		GlobalBurst: config.GetEnvInt("CORRELATOR_GLOBAL_BURST", 0),
		PluginBurst: config.GetEnvInt("CORRELATOR_PLUGIN_BURST", 0),
		UnAuthBurst: config.GetEnvInt("CORRELATOR_UNAUTH_BURST", 0),

		// Cleanup configuration
		CleanupInterval: config.GetEnvDuration(
			"CORRELATOR_RATE_LIMIT_CLEANUP_INTERVAL", rateLimiterCleanupInterval,
		),
		IdleTimeout: config.GetEnvDuration("CORRELATOR_RATE_LIMIT_IDLE_TIMEOUT", rateLimiterIdleTimeout),
		MaxPlugins:  config.GetEnvInt("CORRELATOR_RATE_LIMIT_MAX_PLUGINS", maxPlugins),
	}
}
