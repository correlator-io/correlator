// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	burstCapacityMultiplier    int     = 2
	maxPlugins                 int     = 100
	defaultGlobalRPS           int     = 100
	defaultPluginRPS           int     = 50
	defaultUnAuthRPS           int     = 10
	thresholdMultiplier        float64 = 0.8
	thresholdPercentage        int     = 80
	rateLimiterCleanupInterval         = 5 * time.Minute
	rateLimiterIdleTimeout             = 1 * time.Hour
)

type (
	// RateLimiter provides rate limiting for incoming requests.
	//
	// Implementations may use in-memory token buckets (MVP single-node deployment)
	// or distributed stores like Redis (enterprise multi-node deployment).
	//
	// The interface enables zero-downtime migration from in-memory to Redis-backed
	// rate limiting when scaling beyond single-node deployments.
	RateLimiter interface {
		// Allow checks if a request should be allowed based on rate limits.
		// Returns true if allowed, false if rate limited.
		//
		// For authenticated requests, pluginID identifies the plugin.
		// For unauthenticated requests, pluginID is empty string.
		Allow(pluginID string) bool
	}

	// InMemoryRateLimiter implements RateLimiter using golang.org/x/time/rate.
	//
	// Provides three-tier rate limiting:
	// 1. Global limit (applied to all requests)
	// 2. Per-plugin limit (applied to authenticated requests)
	// 3. Unauthenticated limit (applied to requests without plugin ID)
	//
	// Uses token bucket algorithm with configurable burst capacity.
	// Burst capacity allows temporary bursts above the sustained rate.
	//
	// Memory cleanup runs periodically to prevent unbounded growth.
	// Plugins idle longer than IdleTimeout are removed.
	//
	// Suitable for single-node MVP deployments. For distributed systems,
	// use RedisRateLimiter.
	InMemoryRateLimiter struct {
		global          *rate.Limiter
		perPlugin       map[string]*pluginLimiter
		unauthenticated *rate.Limiter
		mu              sync.RWMutex
		cleanupTicker   *time.Ticker
		done            chan struct{}

		// Configuration (stored for creating new plugin limiters and cleanup)
		pluginRPS       int
		pluginBurst     int
		cleanupInterval time.Duration
		idleTimeout     time.Duration
		maxPlugins      int
	}

	// pluginLimiter tracks rate limit state for a single plugin.
	// Includes last access time for memory cleanup.
	pluginLimiter struct {
		limiter    *rate.Limiter
		lastAccess time.Time
		mu         sync.Mutex
	}
)

// NewInMemoryRateLimiter creates a new in-memory rate limiter with three-tier limits.
//
// Burst capacity is computed automatically as 2 × rate unless overridden in config.
// Cleanup runs periodically to prevent unbounded memory growth.
//
// Parameters:
//   - config: Rate limiter configuration with RPS limits, optional burst overrides,
//     and cleanup settings
//
// Example:
//
//	rl := NewInMemoryRateLimiter(&Config{
//	    GlobalRPS: 100,
//	    PluginRPS: 50,
//	    UnAuthRPS: 10,
//	})
//	defer rl.Close()
func NewInMemoryRateLimiter(config *Config) *InMemoryRateLimiter {
	// Compute burst capacities (use override if provided, otherwise 2 × rate)
	globalBurst := computeBurstCapacity(config.GlobalRPS, config.GlobalBurst)
	pluginBurst := computeBurstCapacity(config.PluginRPS, config.PluginBurst)
	unauthBurst := computeBurstCapacity(config.UnAuthRPS, config.UnAuthBurst)

	// Create rate limiter with three-tier limits
	rl := &InMemoryRateLimiter{
		global:          rate.NewLimiter(rate.Limit(config.GlobalRPS), globalBurst),
		perPlugin:       make(map[string]*pluginLimiter),
		unauthenticated: rate.NewLimiter(rate.Limit(config.UnAuthRPS), unauthBurst),
		done:            make(chan struct{}),
		pluginRPS:       config.PluginRPS,
		pluginBurst:     pluginBurst,
		cleanupInterval: config.CleanupInterval,
		idleTimeout:     config.IdleTimeout,
		maxPlugins:      config.MaxPlugins,
	}

	// Start background cleanup goroutine
	rl.startCleanup()

	return rl
}

// computeBurstCapacity computes the burst capacity based on the rate and optional override.
//
// If burstOverride is 0, computes burst automatically as 2 × rate.
// If burstOverride > 0, uses the override value.
//
// Parameters:
//   - rate: Rate limit in requests per second
//   - burstOverride: Optional burst override (0 = auto-compute)
//
// Returns:
//   - Burst capacity (allows temporary bursts above sustained rate)
//
// Example:
//
//	computeBurstCapacity(100, 0)   // Returns 200 (auto-computed)
//	computeBurstCapacity(100, 500) // Returns 500 (use override)
func computeBurstCapacity(rate, burstOverride int) int {
	if burstOverride > 0 {
		return burstOverride
	}

	return rate * burstCapacityMultiplier
}

// Allow checks if a request should be allowed based on rate limits.
// Implements the RateLimiter interface.
//
// Returns true if the request is allowed, false if rate limited.
//
// Rate limiting is enforced in three tiers:
// 1. Global limit (all requests)
// 2. Per-plugin limit (authenticated) OR unauthenticated limit
//
// Parameters:
//   - pluginID: empty string for unauthenticated requests, plugin ID otherwise
func (rl *InMemoryRateLimiter) Allow(pluginID string) bool {
	// Tier 1: Check global limit first (fail fast)
	if !rl.global.Allow() {
		return false
	}

	// Tier 2: Check plugin-specific or unauthenticated limit
	if pluginID == "" {
		// Unauthenticated request
		return rl.unauthenticated.Allow()
	}

	// Authenticated request - get or create plugin limiter
	rl.mu.RLock()
	pl, ok := rl.perPlugin[pluginID]
	rl.mu.RUnlock()

	if !ok {
		// Lazy initialization: create limiter for this plugin
		rl.mu.Lock()
		// Double-check after acquiring write lock (avoid race)
		if pl, ok = rl.perPlugin[pluginID]; !ok {
			pl = &pluginLimiter{
				limiter:    rate.NewLimiter(rate.Limit(rl.pluginRPS), rl.pluginBurst),
				lastAccess: time.Now(),
			}

			rl.perPlugin[pluginID] = pl

			// Operational monitoring: warn when approaching max plugins limit
			// This helps operators detect plugin ID proliferation before hitting hard limits
			// In later phases, lets add open telemetry metrics to track this
			currentCount := len(rl.perPlugin)
			threshold := int(float64(rl.maxPlugins) * thresholdMultiplier) // 80% threshold

			if currentCount >= threshold {
				slog.Warn("rate limiter approaching max plugins limit",
					"current_plugins", currentCount,
					"max_plugins", rl.maxPlugins,
					"threshold_percent", thresholdPercentage,
					"recommendation", "investigate potential plugin ID proliferation or increase max_plugins limit")
			}
		}

		rl.mu.Unlock()
	}

	// Update last access time (for cleanup)
	pl.mu.Lock()
	pl.lastAccess = time.Now()
	pl.mu.Unlock()

	// Check plugin-specific limit
	return pl.limiter.Allow()
}

// Close stops the cleanup goroutine and releases resources.
// Must be called when the InMemoryRateLimiter is no longer needed.
//
// Note: Close() is not part of the RateLimiter interface to allow
// implementations that don't require cleanup (e.g., RedisRateLimiter
// with connection pooling). Use type assertion if cleanup is needed:
//
//	if closer, ok := limiter.(io.Closer); ok {
//	    closer.Close()
//	}
func (rl *InMemoryRateLimiter) Close() {
	if rl.cleanupTicker != nil {
		rl.cleanupTicker.Stop()
	}

	close(rl.done)
}

// startCleanup starts a background goroutine that periodically removes
// stale plugin limiters to prevent memory leaks.
//
// Cleanup runs every 5 minutes and removes limiters that haven't been
// accessed in the last hour.
func (rl *InMemoryRateLimiter) startCleanup() {
	// Use config values if set, otherwise use defaults
	cleanupInterval := rl.cleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = rateLimiterCleanupInterval
	}

	rl.cleanupTicker = time.NewTicker(cleanupInterval)

	go func() {
		for {
			select {
			case <-rl.cleanupTicker.C:
				rl.cleanup()
			case <-rl.done:
				return
			}
		}
	}()
}

// cleanup removes plugin limiters that haven't been accessed recently.
func (rl *InMemoryRateLimiter) cleanup() {
	// Use config value if set, otherwise use default
	idleTimeout := rl.idleTimeout
	if idleTimeout == 0 {
		idleTimeout = rateLimiterIdleTimeout
	}

	now := time.Now()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	for pluginID, pl := range rl.perPlugin {
		pl.mu.Lock()
		lastAccess := pl.lastAccess
		pl.mu.Unlock()

		if now.Sub(lastAccess) > idleTimeout {
			delete(rl.perPlugin, pluginID)
		}
	}
}

// RateLimit returns a middleware that enforces rate limits on incoming requests.
//
// Rate limiting is applied in three tiers:
//  1. Global limit (all requests)
//  2. Per-plugin limit (authenticated requests with PluginContext)
//  3. Unauthenticated limit (requests without PluginContext)
//
// When a request exceeds the rate limit, the middleware returns a 429 (Too Many Requests)
// response with RFC 7807 error format.
//
// The middleware must be placed after authentication middleware in the chain to access
// PluginContext for per-plugin rate limiting.
//
// Parameters:
//   - limiter: RateLimiter implementation (InMemoryRateLimiter or DistributedRateLimiter)
//
// Example:
//
//	rateLimiter := NewInMemoryRateLimiter(&Config{
//	    GlobalRPS: 100,
//	    PluginRPS: 50,
//	    UnAuthRPS: 10,
//	})
//	defer rateLimiter.Close()
//
//	mux.Use(RateLimit(rateLimiter))
func RateLimit(limiter RateLimiter, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract plugin ID from context (set by authentication middleware)
			// If PluginContext exists, use plugin ID for per-plugin rate limiting
			// If PluginContext is nil, use empty string for unauthenticated rate limiting
			pluginID := ""
			if pluginCtx, ok := GetPluginContext(r.Context()); ok {
				pluginID = pluginCtx.PluginID
			}

			// Check rate limit
			if !limiter.Allow(pluginID) {
				// Get correlation ID for error response
				correlationID := GetCorrelationID(r.Context())

				// Write RFC 7807 compliant error response
				detail := "Rate limit exceeded. Please retry after some time."
				if err := writeRFC7807Error(w, r, http.StatusTooManyRequests, detail, correlationID); err != nil {
					logger.Error("failed to write response with RFC 7807 error format",
						slog.String("correlation_id", correlationID),
						slog.String("path", r.URL.Path),
						slog.String("detail", detail),
						slog.String("error", err.Error()),
					)

					// Fallback to plain text if writeRFC7807Error fails
					http.Error(w, detail, http.StatusTooManyRequests)
				}

				return
			}

			// Rate limit not exceeded, continue to next handler
			next.ServeHTTP(w, r)
		})
	}
}
