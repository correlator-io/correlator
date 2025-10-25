// Package canonicalization provides namespace normalization tests.
package canonicalization

import "testing"

func TestNormalizeNamespace(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Scheme normalization - postgres to postgresql
		{
			name:  "postgres to postgresql",
			input: "postgres://prod-db:5432",
			want:  "postgresql://prod-db",
		},
		{
			name:  "postgresql unchanged",
			input: "postgresql://prod-db:5432",
			want:  "postgresql://prod-db",
		},
		{
			name:  "uppercase scheme PostgreSQL",
			input: "PostgreSQL://HOST",
			want:  "postgresql://HOST", // Only scheme lowercased
		},
		{
			name:  "uppercase scheme POSTGRES",
			input: "POSTGRES://HOST:5432",
			want:  "postgresql://HOST", // Normalized + port removed
		},

		// S3 scheme normalization
		{
			name:  "s3a to s3",
			input: "s3a://bucket",
			want:  "s3://bucket",
		},
		{
			name:  "s3n to s3",
			input: "s3n://bucket",
			want:  "s3://bucket",
		},
		{
			name:  "s3 unchanged",
			input: "s3://bucket",
			want:  "s3://bucket",
		},
		{
			name:  "s3 preserves path case",
			input: "s3://MyBucket/Path/To/File",
			want:  "s3://MyBucket/Path/To/File",
		},
		{
			name:  "s3a preserves path case",
			input: "s3a://MyBucket/Path/To/File",
			want:  "s3://MyBucket/Path/To/File",
		},

		// Default port removal - PostgreSQL
		{
			name:  "postgres default port 5432",
			input: "postgres://db:5432",
			want:  "postgresql://db",
		},
		{
			name:  "postgres non-default port 5433",
			input: "postgres://db:5433",
			want:  "postgresql://db:5433",
		},
		{
			name:  "postgresql default port",
			input: "postgresql://db:5432",
			want:  "postgresql://db",
		},
		{
			name:  "postgresql no port",
			input: "postgresql://db",
			want:  "postgresql://db",
		},

		// Default port removal - MySQL
		{
			name:  "mysql default port 3306",
			input: "mysql://db:3306",
			want:  "mysql://db",
		},
		{
			name:  "mysql non-default port 3307",
			input: "mysql://db:3307",
			want:  "mysql://db:3307",
		},

		// Default port removal - MongoDB
		{
			name:  "mongodb default port 27017",
			input: "mongodb://db:27017",
			want:  "mongodb://db",
		},
		{
			name:  "mongodb non-default port 27018",
			input: "mongodb://db:27018",
			want:  "mongodb://db:27018",
		},

		// URL component preservation
		{
			name:  "with username",
			input: "postgres://user@db:5432",
			want:  "postgresql://user@db",
		},
		{
			name:  "with masked password",
			input: "postgres://user:***@db:5432",
			want:  "postgresql://user:***@db",
		},
		{
			name:  "with database path",
			input: "postgres://db:5432/mydb",
			want:  "postgresql://db/mydb",
		},
		{
			name:  "with query params",
			input: "postgres://db:5432?sslmode=require",
			want:  "postgresql://db?sslmode=require",
		},
		{
			name:  "complex URL with all components",
			input: "postgres://user:pass@host:5432/db?param=value", // pragma: allowlist secret
			want:  "postgresql://user:pass@host/db?param=value",    // pragma: allowlist secret
		},
		{
			name:  "complex URL with multiple query params",
			input: "postgres://host:5432/db?sslmode=require&timeout=30",
			want:  "postgresql://host/db?sslmode=require&timeout=30",
		},

		// Non-URL namespaces (passthrough)
		{
			name:  "bigquery no scheme",
			input: "bigquery",
			want:  "bigquery",
		},
		{
			name:  "kafka no scheme",
			input: "kafka",
			want:  "kafka",
		},
		{
			name:  "snowflake no scheme",
			input: "snowflake",
			want:  "snowflake",
		},

		// Schemes that should pass through (no normalization rules)
		{
			name:  "kafka with scheme",
			input: "kafka://broker:9092",
			want:  "kafka://broker:9092",
		},
		{
			name:  "redis with default port",
			input: "redis://localhost:6379",
			want:  "redis://localhost", // Default port removed
		},
		{
			name:  "redis with non-default port",
			input: "redis://localhost:6380",
			want:  "redis://localhost:6380",
		},

		// Edge cases
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "only scheme with slashes",
			input: "postgres://",
			want:  "postgresql://",
		},
		{
			name:  "trailing slash",
			input: "postgres://db:5432/",
			want:  "postgresql://db/",
		},
		{
			name:  "multiple slashes in path",
			input: "s3://bucket//path//file",
			want:  "s3://bucket//path//file",
		},
		{
			name:  "special characters in password",
			input: "postgres://user:p@ss!w0rd@host:5432/db",
			want:  "postgresql://user:p@ss!w0rd@host/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeNamespace(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeNamespace(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
