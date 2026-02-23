# Correlator Web UI Dockerfile

# Build stage
FROM node:22-alpine AS builder

# Build arguments for configuration
ARG NEXT_PUBLIC_CORRELATOR_URL=http://localhost:8080
ARG VERSION=0.1.0-dev
ARG GIT_COMMIT=unknown

# Set build-time environment variables
ENV NEXT_PUBLIC_CORRELATOR_URL=$NEXT_PUBLIC_CORRELATOR_URL
ENV NEXT_PUBLIC_VERSION=$VERSION
ENV NEXT_PUBLIC_GIT_COMMIT=$GIT_COMMIT

WORKDIR /app

# Copy package files for dependency caching
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build the application
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# Runtime stage
FROM node:22-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Install wget for healthcheck
RUN apk --no-cache add wget

# Create non-root user for security (matches backend pattern)
RUN addgroup -g 1001 -S correlator && \
    adduser -u 1001 -S correlator -G correlator

# Copy built application from standalone output
COPY --from=builder /app/public ./public
COPY --from=builder --chown=correlator:correlator /app/.next/standalone ./
COPY --from=builder --chown=correlator:correlator /app/.next/static ./.next/static

USER correlator

EXPOSE 3000

ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000 || exit 1

CMD ["node", "server.js"]
