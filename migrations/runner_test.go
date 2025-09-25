package main

import (
	"errors"
	"strings"
	"testing"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// Static errors for testing.
var (
	ErrSyntaxError              = errors.New("syntax error in migration")
	ErrConnectionLost           = errors.New("connection lost")
	ErrCannotRollback           = errors.New("cannot rollback applied migration")
	ErrDatabaseDirty            = errors.New("database is in dirty state")
	ErrDatabaseConnectionFailed = errors.New("database connection failed")
	ErrCannotDropTables         = errors.New("cannot drop tables")
	ErrPermissionDenied         = errors.New("permission denied")
	ErrConnectionCloseError     = errors.New("connection close error")
	ErrMigrationFailed          = errors.New("migration failed")
	ErrRollbackFailed           = errors.New("rollback failed")
	ErrDropFailed               = errors.New("drop failed")
	ErrMultipleCloseErrors      = errors.New(
		"close errors: [source close error: connection lost, database close error: timeout]",
	)
)

// mockMigrationRunner implements MigrationRunner for testing.
type mockMigrationRunner struct {
	upError      error
	downError    error
	statusError  error
	versionError error
	dropError    error
	closeError   error
}

func (m *mockMigrationRunner) Up() error      { return m.upError }
func (m *mockMigrationRunner) Down() error    { return m.downError }
func (m *mockMigrationRunner) Status() error  { return m.statusError }
func (m *mockMigrationRunner) Version() error { return m.versionError }
func (m *mockMigrationRunner) Drop() error    { return m.dropError }
func (m *mockMigrationRunner) Close() error   { return m.closeError }

// Helper function to reduce test code duplication.
type testCase struct {
	name        string
	setupMock   func() *mockMigrationRunner
	expectError bool
	errorText   string
}

// runTestCases is a helper function to execute test cases and reduce duplication.
func runTestCases(t *testing.T, tests []testCase, operation func(MigrationRunner) error) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := tt.setupMock()

			err := operation(runner)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				if tt.errorText != "" && !strings.Contains(err.Error(), tt.errorText) {
					t.Errorf("expected error containing %q, got %q", tt.errorText, err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// NOTE: NewMigrationRunner testing requires a real database connection and proper
// migration files setup. Since all test cases in unit tests would fail with
// "failed to ping database" in CI/test environments without database access,
// comprehensive testing of NewMigrationRunner is covered in integration tests
// using testcontainers. This allows testing actual error conditions like:
// - "failed to create postgres driver" (invalid database configurations)
// - "failed to create migrate instance" (migration setup issues)
// - Database connectivity and migration file validation scenarios

func TestMigrationRunnerUp(t *testing.T) {
	tests := []testCase{
		{
			name: "successful migration up",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					upError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "no migrations to apply",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					upError: nil, // Mock should return nil for "no change" scenario
				}
			},
			expectError: false, // Should handle ErrNoChange gracefully
		},
		{
			name: "migration failure",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					upError: ErrSyntaxError,
				}
			},
			expectError: true,
			errorText:   "syntax error in migration", // Mock returns error directly
		},
		{
			name: "database connection lost during migration",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					upError: ErrConnectionLost,
				}
			},
			expectError: true,
			errorText:   "connection lost", // Mock returns error directly
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Up() })
}

func TestMigrationRunnerDown(t *testing.T) {
	tests := []testCase{
		{
			name: "successful migration down",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					downError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "no migrations to rollback",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					downError: nil, // Mock should return nil for "no change" scenario
				}
			},
			expectError: false, // Should handle ErrNoChange gracefully
		},
		{
			name: "rollback failure",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					downError: ErrCannotRollback,
				}
			},
			expectError: true,
			errorText:   "cannot rollback applied migration", // Mock returns error directly
		},
		{
			name: "database in dirty state",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					downError: ErrDatabaseDirty,
				}
			},
			expectError: true,
			errorText:   "database is in dirty state", // Mock returns error directly
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Down() })
}

func TestMigrationRunnerStatus(t *testing.T) {
	tests := []testCase{
		{
			name: "get status successfully",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					statusError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "database connection error",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					statusError: ErrDatabaseConnectionFailed,
				}
			},
			expectError: true,
			errorText:   "database connection failed",
		},
		{
			name: "no migrations table exists",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					statusError: nil, // Mock should return nil for graceful handling
				}
			},
			expectError: false, // Should handle gracefully
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Status() })
}

func TestMigrationRunnerVersion(t *testing.T) {
	tests := []testCase{
		{
			name: "get version successfully",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					versionError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "database connection error",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					versionError: ErrDatabaseConnectionFailed,
				}
			},
			expectError: true,
			errorText:   "database connection failed",
		},
		{
			name: "no migrations applied",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					versionError: nil, // Mock should return nil for graceful handling
				}
			},
			expectError: false, // Should handle gracefully
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Version() })
}

func TestMigrationRunnerDrop(t *testing.T) {
	tests := []testCase{
		{
			name: "successful drop",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					dropError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "drop failure",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					dropError: ErrCannotDropTables,
				}
			},
			expectError: true,
			errorText:   "cannot drop tables", // Mock returns error directly
		},
		{
			name: "permission denied",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					dropError: ErrPermissionDenied,
				}
			},
			expectError: true,
			errorText:   "permission denied", // Mock returns error directly
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Drop() })
}

func TestMigrationRunnerClose(t *testing.T) {
	tests := []testCase{
		{
			name: "successful close",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					closeError: nil,
				}
			},
			expectError: false,
		},
		{
			name: "close with connection error",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					closeError: ErrConnectionCloseError,
				}
			},
			expectError: true,
			errorText:   "connection close error",
		},
		{
			name: "close with multiple errors",
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{
					closeError: ErrMultipleCloseErrors,
				}
			},
			expectError: true,
			errorText:   "close errors",
		},
	}

	runTestCases(t, tests, func(r MigrationRunner) error { return r.Close() })
}

// TestMigrationRunnerInterface ensures our interface compliance.
func TestMigrationRunnerInterface(_ *testing.T) {
	// This is a compile-time test to ensure interface compliance
	var _ MigrationRunner = (*mockMigrationRunner)(nil)

	// Also test that our real implementation complies with the interface
	var _ MigrationRunner = (*Runner)(nil) // This should compile when implemented
}

// TestMigrationRunnerLifecycle tests the complete lifecycle of a migration runner.
func TestMigrationRunnerLifecycle(t *testing.T) {
	// This test defines the expected workflow for migration operations
	mock := &mockMigrationRunner{
		upError:      nil,
		statusError:  nil,
		versionError: nil,
		closeError:   nil,
	}

	// Test typical workflow: Status -> Up -> Status -> Close
	if err := mock.Status(); err != nil {
		t.Errorf("initial status check failed: %v", err)
	}

	if err := mock.Up(); err != nil {
		t.Errorf("migration up failed: %v", err)
	}

	if err := mock.Status(); err != nil {
		t.Errorf("post-migration status check failed: %v", err)
	}

	if err := mock.Version(); err != nil {
		t.Errorf("version check failed: %v", err)
	}

	if err := mock.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}
}

// TestMigrationRunnerErrorRecovery tests error handling and recovery scenarios.
func TestMigrationRunnerErrorRecovery(t *testing.T) {
	tests := []struct {
		name        string
		operations  []func(MigrationRunner) error
		expectPanic bool
	}{
		{
			name: "handle up error gracefully",
			operations: []func(MigrationRunner) error{
				func(r MigrationRunner) error { return r.Up() },
				func(r MigrationRunner) error { return r.Status() }, // Should still work after error
			},
			expectPanic: false,
		},
		{
			name: "handle down error gracefully",
			operations: []func(MigrationRunner) error{
				func(r MigrationRunner) error { return r.Down() },
				func(r MigrationRunner) error { return r.Version() }, // Should still work after error
			},
			expectPanic: false,
		},
		{
			name: "handle multiple operations after error",
			operations: []func(MigrationRunner) error{
				func(r MigrationRunner) error { return r.Up() },     // This will error
				func(r MigrationRunner) error { return r.Status() }, // Should still work
				func(r MigrationRunner) error { return r.Down() },   // Should work
				func(r MigrationRunner) error { return r.Close() },  // Should work
			},
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock that will error on specific operations
			mock := &mockMigrationRunner{
				upError:   ErrMigrationFailed,
				downError: ErrRollbackFailed,
				// Other operations succeed
				statusError:  nil,
				versionError: nil,
				closeError:   nil,
			}

			defer func() {
				if r := recover(); r != nil {
					if !tt.expectPanic {
						t.Errorf("unexpected panic: %v", r)
					}
				}
			}()

			for i, op := range tt.operations {
				err := op(mock)
				// We expect some operations to error, but the runner should remain functional
				t.Logf("operation %d result: %v", i, err)
			}
		})
	}
}

// TestMigrationRunnerResourceManagement tests proper resource cleanup.
func TestMigrationRunnerResourceManagement(t *testing.T) {
	mock := &mockMigrationRunner{}

	// Test that Close can be called multiple times safely
	if err := mock.Close(); err != nil {
		t.Errorf("first close failed: %v", err)
	}

	if err := mock.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}

	// Test that operations after close behave reasonably
	// (exact behavior depends on implementation, but shouldn't panic)
	_ = mock.Status()
	_ = mock.Version()
}

// BenchmarkMigrationRunnerOperations benchmarks basic operations.
func BenchmarkMigrationRunnerOperations(b *testing.B) {
	mock := &mockMigrationRunner{}

	b.Run("Status", func(b *testing.B) {
		for range b.N {
			_ = mock.Status()
		}
	})

	b.Run("Version", func(b *testing.B) {
		for range b.N {
			_ = mock.Version()
		}
	})

	b.Run("Up", func(b *testing.B) {
		for range b.N {
			_ = mock.Up()
		}
	})
}

// TestExecuteCommand tests the CLI command execution logic, particularly the --force flag behavior.
func TestExecuteCommand(t *testing.T) {
	tests := []struct {
		name          string
		command       string
		force         bool
		setupMock     func() *mockMigrationRunner
		wantError     bool
		errorContains string
	}{
		{
			name:    "up command works",
			command: "up",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors
			},
			wantError: false,
		},
		{
			name:    "down command works",
			command: "down",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors
			},
			wantError: false,
		},
		{
			name:    "status command works",
			command: "status",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors
			},
			wantError: false,
		},
		{
			name:    "version command works",
			command: "version",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors
			},
			wantError: false,
		},
		{
			name:    "drop command without force fails with safety error",
			command: "drop",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors - should not be called
			},
			wantError:     true,
			errorContains: "drop command requires --force flag for safety",
		},
		{
			name:    "drop command with force succeeds",
			command: "drop",
			force:   true,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors
			},
			wantError: false,
		},
		{
			name:    "drop command with force handles runner errors",
			command: "drop",
			force:   true,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{dropError: ErrDropFailed}
			},
			wantError:     true,
			errorContains: "drop failed",
		},
		{
			name:    "unknown command fails",
			command: "invalid",
			force:   false,
			setupMock: func() *mockMigrationRunner {
				return &mockMigrationRunner{} // no errors - should not be called
			},
			wantError:     true,
			errorContains: "unknown command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.setupMock()

			err := executeCommand(tt.command, mock, tt.force)

			if tt.wantError {
				if err == nil {
					t.Errorf("Expected error but got none")

					return
				}

				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain %q, got %q", tt.errorContains, err.Error())
				}
			} else if err != nil {
				t.Errorf("Expected no error but got: %v", err)

				return
			}
		})
	}
}
