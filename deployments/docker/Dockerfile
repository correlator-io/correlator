# Correlator API Server Dockerfile

# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Copy go mod files for dependency caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build arguments for version injection
ARG VERSION=0.1.0-dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -a -installsuffix cgo \
    -ldflags "-s -w -X main.version=${VERSION} -X main.gitCommit=${GIT_COMMIT} -X 'main.buildTime=${BUILD_TIME}'" \
    -o correlator ./cmd/correlator

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates wget

# Create non-root user for security
RUN addgroup -g 1001 -S correlator && \
    adduser -u 1001 -S correlator -G correlator

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/correlator .

RUN chown -R correlator:correlator /app

USER correlator

# Environment variables (config mounted at runtime, not baked in)
ENV DATABASE_URL=""
ENV LOG_LEVEL="info"
ENV CORRELATOR_CONFIG_PATH=""

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

CMD ["./correlator"]
