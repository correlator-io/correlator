// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const testPlugin = "test-plugin"

// TestRateLimiter_GlobalLimitEnforced verifies that the global rate limit
// is enforced across all requests regardless of plugin ID.
func TestRateLimiter_GlobalLimitEnforced(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: 10 RPS global, 50 RPS plugin (global is more restrictive)
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   10,
		GlobalBurst: 10, // use override value
		PluginRPS:   50,
		UnAuthRPS:   2,
	})
	defer rl.Close()

	// Test: Send 11 requests with pluginID, expect 11th to fail
	// Global limit (10) should be hit before plugin limit (50)
	pluginID := testPlugin
	successCount := 0

	for i := 0; i < 11; i++ {
		if rl.Allow(pluginID) {
			successCount++
		}
	}

	// Expect exactly 10 to succeed (global limit)
	if successCount != 10 {
		t.Errorf("expected 10 successful requests, got %d", successCount)
	}
}

// TestRateLimiter_PluginLimitEnforced verifies that per-plugin rate limits
// are enforced independently from the global limit.
func TestRateLimiter_PluginLimitEnforced(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: 100 RPS global, 5 RPS plugin, 2 RPS unauth
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   5,
		PluginBurst: 5, // use override value
		UnAuthRPS:   2,
	})
	defer rl.Close()

	// Test: Send 6 requests with same pluginID, expect 6th to fail
	pluginID := testPlugin
	successCount := 0

	for i := 0; i < 6; i++ {
		if rl.Allow(pluginID) {
			successCount++
		}
	}

	// Expect exactly 5 to succeed (plugin limit)
	if successCount != 5 {
		t.Errorf("expected 5 successful requests, got %d", successCount)
	}
}

// TestRateLimiter_UnauthenticatedLimitEnforced verifies that requests
// without a plugin ID are rate limited separately.
func TestRateLimiter_UnauthenticatedLimitEnforced(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: 100 RPS global, 50 RPS plugin, 2 RPS unauth
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   50,
		UnAuthRPS:   2,
		UnAuthBurst: 2, // use override value
	})
	defer rl.Close()

	// Test: Send 3 requests with empty pluginID, expect 3rd to fail
	successCount := 0

	for i := 0; i < 3; i++ {
		if rl.Allow("") {
			successCount++
		}
	}

	// Expect exactly 2 to succeed (unauth limit)
	if successCount != 2 {
		t.Errorf("expected 2 successful requests, got %d", successCount)
	}
}

// TestRateLimiter_BurstCapacityWorks verifies that burst capacity allows
// temporary bursts above the sustained rate, then throttles subsequent requests.
func TestRateLimiter_BurstCapacityWorks(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: 10 RPS with 20 burst capacity
	// This means 10 requests can be made instantly (burst),
	// and tokens refill at 10 per second
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   10,
		GlobalBurst: 10, // use override value
		PluginRPS:   5,
		PluginBurst: 5, // use override value
		UnAuthRPS:   2,
	})
	defer rl.Close()

	pluginID := testPlugin
	// Test: Send 10 requests instantly (should all pass due to burst)
	// Note: Global limit is 10, plugin limit is 5, so we'll hit plugin limit first
	successCount := 0

	for i := 0; i < 10; i++ {
		if rl.Allow(pluginID) {
			successCount++
		}
	}

	// Expect 5 to succeed (plugin limit, not global)
	if successCount != 5 {
		t.Errorf("expected 5 successful burst requests, got %d", successCount)
	}

	// Send 1 more immediately (should fail - burst exhausted)
	if rl.Allow(pluginID) {
		t.Error("expected request to be rate limited after burst exhausted")
	}
}

// TestRateLimiter_PluginIsolation verifies that rate limits for different
// plugins are tracked independently.
func TestRateLimiter_PluginIsolation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: 100 RPS global, 5 RPS plugin
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   5,
		PluginBurst: 5, // use override value
		UnAuthRPS:   2,
	})
	defer rl.Close()

	plugin1 := "plugin-1"
	plugin2 := "plugin-2"

	// Plugin 1 uses all 5 requests
	for i := 0; i < 5; i++ {
		if !rl.Allow(plugin1) {
			t.Errorf("plugin1 request %d should succeed", i+1)
		}
	}

	// Plugin 1's 6th request fails
	if rl.Allow(plugin1) {
		t.Error("plugin1 should be rate limited")
	}

	// Plugin 2 should still have 5 requests available
	for i := 0; i < 5; i++ {
		if !rl.Allow(plugin2) {
			t.Errorf("plugin2 request %d should succeed", i+1)
		}
	}
}

// TestRateLimiter_ConcurrentAccess verifies that the rate limiter is safe
// for concurrent use by multiple goroutines.
func TestRateLimiter_ConcurrentAccess(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS: 100,
		PluginRPS: 50,
		UnAuthRPS: 10,
	})
	defer rl.Close()

	// Launch 10 goroutines, each making 10 requests
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(pluginID string) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				_ = rl.Allow(pluginID)
			}
		}(fmt.Sprintf("plugin-%d", i))
	}

	wg.Wait()
	// If we get here without panic/race, concurrent access is safe
}

// TestRateLimiter_MemoryCleanup verifies that stale plugin limiters
// are removed after the idle timeout period.
func TestRateLimiter_MemoryCleanup(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter with short idle timeout for testing
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   50,
		UnAuthRPS:   10,
		IdleTimeout: 100 * time.Millisecond, // Short timeout for test
	})
	defer rl.Close()

	// Create plugin limiter by making a request
	pluginID := "stale-plugin"
	if !rl.Allow(pluginID) {
		t.Fatal("first request should succeed")
	}

	// Verify plugin limiter exists in map
	rl.mu.RLock()
	_, exists := rl.perPlugin[pluginID]
	rl.mu.RUnlock()

	if !exists {
		t.Fatal("plugin limiter should exist after first request")
	}

	// Wait for idle timeout + buffer
	time.Sleep(150 * time.Millisecond)

	// Manually trigger cleanup (don't wait for ticker)
	rl.cleanup()

	// Verify plugin limiter was removed
	rl.mu.RLock()
	_, exists = rl.perPlugin[pluginID]
	rl.mu.RUnlock()

	if exists {
		t.Error("stale plugin limiter should have been removed after cleanup")
	}
}

// TestRateLimiter_CleanupPreservesActivePlugins verifies that cleanup
// only removes idle plugins and preserves recently active ones.
func TestRateLimiter_CleanupPreservesActivePlugins(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter with short idle timeout
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   50,
		UnAuthRPS:   10,
		IdleTimeout: 100 * time.Millisecond,
	})
	defer rl.Close()

	stalePlugin := "stale-plugin"
	activePlugin := "active-plugin"

	// Create both plugin limiters
	if !rl.Allow(stalePlugin) {
		t.Fatal("stale plugin first request should succeed")
	}

	if !rl.Allow(activePlugin) {
		t.Fatal("active plugin first request should succeed")
	}

	// Wait for stale plugin to exceed idle timeout
	time.Sleep(150 * time.Millisecond)

	// Keep active plugin active (update lastAccess)
	if !rl.Allow(activePlugin) {
		t.Fatal("active plugin should still be allowed")
	}

	// Trigger cleanup
	rl.cleanup()

	// Verify stale plugin was removed
	rl.mu.RLock()
	_, staleExists := rl.perPlugin[stalePlugin]
	_, activeExists := rl.perPlugin[activePlugin]
	rl.mu.RUnlock()

	if staleExists {
		t.Error("stale plugin should have been removed")
	}

	if !activeExists {
		t.Error("active plugin should have been preserved")
	}
}

// TestRateLimitMiddleware_RequestAllowed verifies that requests under
// the rate limit are allowed to proceed to the next handler.
func TestRateLimitMiddleware_RequestAllowed(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter with high limits (request will not be blocked)
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS: 100,
		PluginRPS: 50,
		UnAuthRPS: 10,
	})
	defer rl.Close()

	logger := slog.New(slog.DiscardHandler)

	// Create test handler that tracks if it was called
	nextCalled := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalled = true

		w.WriteHeader(http.StatusOK)
	})

	// Wrap with rate limit middleware
	handler := RateLimit(rl, logger)(nextHandler)

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	// Execute request
	handler.ServeHTTP(rec, req)

	// Verify next handler was called
	if !nextCalled {
		t.Error("expected next handler to be called when rate limit not exceeded")
	}

	// Verify response status
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

// TestRateLimitMiddleware_RequestBlocked verifies that requests exceeding
// the rate limit are rejected with 429 status.
func TestRateLimitMiddleware_RequestBlocked(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter with very low limits (requests will be blocked)
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   1,
		GlobalBurst: 1,
		PluginRPS:   1,
		UnAuthRPS:   1,
	})
	defer rl.Close()

	logger := slog.New(slog.DiscardHandler)

	// Create test handler that should NOT be called
	nextCalled := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalled = true

		w.WriteHeader(http.StatusOK)
	})

	// Wrap with rate limit middleware
	handler := RateLimit(rl, logger)(nextHandler)

	// Make first request (should succeed)
	req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("first request should succeed, got status %d", rec1.Code)
	}

	// Make second request immediately (should be rate limited)
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec2 := httptest.NewRecorder()
	nextCalled = false // Reset flag

	handler.ServeHTTP(rec2, req2)

	// Verify next handler was NOT called
	if nextCalled {
		t.Error("expected next handler NOT to be called when rate limit exceeded")
	}

	// Verify 429 status
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", rec2.Code)
	}
}

// TestRateLimitMiddleware_RFC7807ErrorFormat verifies that rate limit
// errors return RFC 7807 compliant responses.
func TestRateLimitMiddleware_RFC7807ErrorFormat(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter with very low limits
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   1,
		GlobalBurst: 1,
		PluginRPS:   1,
		UnAuthRPS:   1,
	})
	defer rl.Close()

	logger := slog.New(slog.DiscardHandler)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := RateLimit(rl, logger)(nextHandler)

	// Exhaust rate limit
	req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Make rate-limited request
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/lineage/events", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	// Verify Content-Type header
	contentType := rec2.Header().Get("Content-Type")
	if contentType != contentTypeProblemJSON {
		t.Errorf("expected Content-Type %s, got %s", contentTypeProblemJSON, contentType)
	}

	// Parse response body
	var problem map[string]interface{}
	if err := json.Unmarshal(rec2.Body.Bytes(), &problem); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}

	// Verify RFC 7807 fields
	if problem["type"] != "https://getcorrelator.io/problems/429" {
		t.Errorf("expected type https://getcorrelator.io/problems/429, got %v", problem["type"])
	}

	if problem["title"] != "Too Many Requests" {
		t.Errorf("expected title 'Too Many Requests', got %v", problem["title"])
	}

	if problem["status"] != float64(429) {
		t.Errorf("expected status 429, got %v", problem["status"])
	}

	if problem["instance"] != "/api/v1/lineage/events" {
		t.Errorf("expected instance /api/v1/lineage/events, got %v", problem["instance"])
	}
}

// TestRateLimitMiddleware_AuthenticatedVsUnauthenticated verifies that
// authenticated and unauthenticated requests use different rate limits.
func TestRateLimitMiddleware_AuthenticatedVsUnauthenticated(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create limiter: high global, low unauth, medium plugin
	rl := NewInMemoryRateLimiter(&Config{
		GlobalRPS:   100,
		PluginRPS:   10,
		PluginBurst: 10,
		UnAuthRPS:   2,
		UnAuthBurst: 2,
	})
	defer rl.Close()

	logger := slog.New(slog.DiscardHandler)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := RateLimit(rl, logger)(nextHandler)

	// Test unauthenticated requests (limit: 2)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("unauthenticated request %d should succeed, got status %d", i+1, rec.Code)
		}
	}

	// 3rd unauthenticated request should fail
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("3rd unauthenticated request should be rate limited, got status %d", rec.Code)
	}

	// Test authenticated requests (limit: 10, separate from unauth)
	pluginCtx := PluginContext{
		PluginID: "test-plugin",
		Name:     "Test Plugin",
	}

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		ctx := SetPluginContext(req.Context(), pluginCtx)
		req = req.WithContext(ctx)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("authenticated request %d should succeed, got status %d", i+1, rec.Code)
		}
	}

	// 11th authenticated request should fail
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	ctx := SetPluginContext(req.Context(), pluginCtx)
	req = req.WithContext(ctx)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("11th authenticated request should be rate limited, got status %d", rec.Code)
	}
}
