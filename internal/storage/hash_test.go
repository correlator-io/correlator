package storage

import (
	"strings"
	"testing"
	"time"
)

const testAPIKey = "sk-test-12345678901234567890123456789012" // pragma: allowlist secret

func TestHashAPIKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		apiKey      string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid API key",
			apiKey:  testAPIKey,
			wantErr: false,
		},
		{
			name:    "short API key",
			apiKey:  "sk-test-123",
			wantErr: false,
		},
		{
			name:    "long API key",
			apiKey:  strings.Repeat("a", 100),
			wantErr: false,
		},
		{
			name:        "empty API key",
			apiKey:      "",
			wantErr:     true,
			errContains: "API key cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := HashAPIKey(tt.apiKey)

			if tt.wantErr {
				if err == nil {
					t.Errorf("HashAPIKey() expected error, got nil")
				}

				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("HashAPIKey() error = %v, want error containing %q", err, tt.errContains)
				}

				if hash != "" {
					t.Errorf("HashAPIKey() hash = %q, want empty string on error", hash)
				}

				return
			}

			if err != nil {
				t.Errorf("HashAPIKey() unexpected error = %v", err)

				return
			}

			// Verify hash properties
			if hash == "" {
				t.Error("HashAPIKey() returned empty hash")
			}

			// Bcrypt hashes should start with $2a$, $2b$, or $2y$
			if !strings.HasPrefix(hash, "$2") {
				t.Errorf("HashAPIKey() hash = %q, want bcrypt format starting with $2", hash)
			}

			// Bcrypt hashes should be 60 characters
			if len(hash) != 60 {
				t.Errorf("HashAPIKey() hash length = %d, want 60", len(hash))
			}

			// Hash should be different each time (bcrypt includes salt)
			hash2, err := HashAPIKey(tt.apiKey)
			if err != nil {
				t.Errorf("HashAPIKey() second call error = %v", err)
			}

			if hash == hash2 {
				t.Error("HashAPIKey() produced identical hashes, should include random salt")
			}
		})
	}
}

func TestCompareAPIKeyHash(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Generate a test hash for comparison tests
	testKey := testAPIKey

	testHash, err := HashAPIKey(testKey)
	if err != nil {
		t.Fatalf("Failed to generate test hash: %v", err)
	}

	tests := []struct {
		name   string
		hash   string
		apiKey string
		want   bool
	}{
		{
			name:   "correct key matches hash",
			hash:   testHash,
			apiKey: testKey,
			want:   true,
		},
		{
			name:   "incorrect key does not match hash",
			hash:   testHash,
			apiKey: "sk-test-wrong-key-here",
			want:   false,
		},
		{
			name:   "empty hash",
			hash:   "",
			apiKey: testKey,
			want:   false,
		},
		{
			name:   "empty api key",
			hash:   testHash,
			apiKey: "",
			want:   false,
		},
		{
			name:   "both empty",
			hash:   "",
			apiKey: "",
			want:   false,
		},
		{
			name:   "invalid hash format",
			hash:   "invalid-hash-format",
			apiKey: testKey,
			want:   false,
		},
		{
			name:   "case sensitive comparison",
			hash:   testHash,
			apiKey: strings.ToUpper(testKey),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareAPIKeyHash(tt.hash, tt.apiKey)

			if got != tt.want {
				t.Errorf("CompareAPIKeyHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHashAPIKey_Performance(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Measure hashing time (should be ~60ms for cost 10)
	start := time.Now()
	hash, err := HashAPIKey(testAPIKey)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("HashAPIKey() error = %v", err)
	}

	if hash == "" {
		t.Fatal("HashAPIKey() returned empty hash")
	}

	t.Logf("Hashing took %v", duration)

	// For cost 10, expect 20-100ms (varies by hardware)
	if duration > 200*time.Millisecond {
		t.Errorf("HashAPIKey() took %v, expected < 200ms for cost 10", duration)
	}

	if duration < 10*time.Millisecond {
		t.Errorf("HashAPIKey() took %v, suspiciously fast for bcrypt cost 10", duration)
	}
}

func TestCompareAPIKeyHash_Performance(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	hash, err := HashAPIKey(testAPIKey)
	if err != nil {
		t.Fatalf("Failed to generate test hash: %v", err)
	}

	// Measure comparison time (should be ~60ms for cost 10)
	start := time.Now()
	result := CompareAPIKeyHash(hash, testAPIKey)
	duration := time.Since(start)

	if !result {
		t.Fatal("CompareAPIKeyHash() returned false for correct key")
	}

	t.Logf("Comparison took %v", duration)

	// For cost 10, expect 20-100ms (varies by hardware)
	if duration > 200*time.Millisecond {
		t.Errorf("CompareAPIKeyHash() took %v, expected < 200ms for cost 10", duration)
	}

	if duration < 10*time.Millisecond {
		t.Errorf("CompareAPIKeyHash() took %v, suspiciously fast for bcrypt cost 10", duration)
	}
}

func TestBenchmarkHashAPIKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Run a small benchmark (not using testing.B for unit test)
	const iterations = 5

	var totalDuration time.Duration

	for i := 0; i < iterations; i++ {
		start := time.Now()

		_, err := HashAPIKey(testAPIKey)
		if err != nil {
			t.Fatalf("HashAPIKey() iteration %d error = %v", i, err)
		}

		totalDuration += time.Since(start)
	}

	avgDuration := totalDuration / iterations
	t.Logf("Average hashing time over %d iterations: %v", iterations, avgDuration)

	// Verify reasonable performance for cost 10
	if avgDuration > 150*time.Millisecond {
		t.Errorf("Average hashing time %v is too slow for cost 10", avgDuration)
	}
}
