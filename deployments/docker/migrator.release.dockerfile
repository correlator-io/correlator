# Correlator Migrator — Release Dockerfile
# Used by GoReleaser: copies pre-built binary (with embedded SQL), no compilation.

FROM alpine:3.19

RUN apk --no-cache add ca-certificates

RUN addgroup -g 1001 -S correlator && \
    adduser -u 1001 -S correlator -G correlator

WORKDIR /app

ARG TARGETPLATFORM
COPY ${TARGETPLATFORM}/migrator .

RUN chown -R correlator:correlator /app

USER correlator

ENV DATABASE_URL=""
ENV MIGRATION_TABLE="schema_migrations"

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ./migrator --version || exit 1

CMD ["./migrator", "--help"]
