# Development Container for Correlator Project
# Matches CI environment: Go 1.25.0, golangci-lint v2.4.0

FROM golang:1.25.0-bookworm AS base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client-15 \
    curl \
    git \
    make \
    ca-certificates \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install pre-commit and common dependencies (override PEP 668 for container environment)
RUN pip3 install --no-cache-dir --break-system-packages pre-commit detect-secrets

# Install golangci-lint v2.4.0 (matching CI version)
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | \
    sh -s -- -b $(go env GOPATH)/bin v2.4.0

# Install Docker CLI for testcontainers
RUN curl -fsSL https://get.docker.com | sh

# Create non-root user for security and add to docker group
RUN groupadd --gid 1000 correlator && \
    useradd --uid 1000 --gid correlator --shell /bin/bash --create-home correlator && \
    usermod -aG docker correlator

# Set up workspace with proper ownership
WORKDIR /workspace
RUN chown correlator:correlator /workspace

# Create simple entrypoint to fix Docker socket permissions
RUN echo '#!/bin/bash\n\
# Fix Docker socket permissions for non-root user\n\
if [ -S /var/run/docker.sock ]; then\n\
    chmod 666 /var/run/docker.sock\n\
fi\n\
\n\
# Execute the original command\n\
exec "$@"' > /usr/local/bin/fix-docker-socket.sh && \
    chmod +x /usr/local/bin/fix-docker-socket.sh

# Set Go environment for development
ENV GO111MODULE=on \
    CGO_ENABLED=1 \
    GOPROXY=https://proxy.golang.org,direct \
    GOSUMDB=sum.golang.org

# Ensure Go tools are in PATH for correlator user
ENV PATH="${PATH}:/home/correlator/go/bin"

# Use entrypoint to fix Docker socket permissions, but start as root initially
USER root
ENTRYPOINT ["/usr/local/bin/fix-docker-socket.sh"]

# Keep container running
CMD ["sleep", "infinity"]
