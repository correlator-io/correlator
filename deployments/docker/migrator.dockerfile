# Migrator Dockerfile
# Production-ready database migration container

# Build stage
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files for dependency caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the migrator binary with embedded migrations and version information
# The go:embed directive will include SQL files at compile time
ARG VERSION=1.0.0-dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -a -installsuffix cgo \
    -ldflags "-X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT} -X 'main.buildTime=${BUILD_TIME}'" \
    -o migrator ./migrations

# Runtime stage
FROM alpine:3.19

# Install ca-certificates for SSL/TLS connections
RUN apk --no-cache add ca-certificates

# Create non-root user for security
RUN addgroup -g 1001 -S correlator && \
    adduser -u 1001 -S correlator -G correlator

# Set working directory
WORKDIR /app

# Copy the migrator binary from builder stage (includes embedded migrations)
COPY --from=builder /app/migrator .

# Change ownership to non-root user
RUN chown -R correlator:correlator /app

# Switch to non-root user
USER correlator

# Set default environment variables
ENV DATABASE_URL=""
ENV MIGRATION_TABLE="schema_migrations"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ./migrator --version || exit 1

# Default command shows help
CMD ["./migrator", "--help"]
