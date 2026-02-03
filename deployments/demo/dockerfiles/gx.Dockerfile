# Great Expectations Dockerfile for Correlator Demo
# Includes GE 1.3+ with ge-correlator plugin from TestPyPI

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /gx

# Install Great Expectations and ge-correlator from TestPyPI
# --extra-index-url ensures dependencies not on TestPyPI are fetched from PyPI
RUN pip install --no-cache-dir \
    "great_expectations>=1.3.0" \
    psycopg2-binary \
    sqlalchemy \
    && pip install --no-cache-dir \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    ge-correlator

# Default command
ENTRYPOINT ["python"]
CMD ["--help"]
