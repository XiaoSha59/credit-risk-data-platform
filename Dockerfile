# ==========================================
# STAGE 1: Builder
# ==========================================
FROM python:3.10-slim AS builder

WORKDIR /app

# Install build dependencies and clean APT cache immediately
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-gen.txt .

# Build wheels for all dependencies to avoid recompilation in the final stage
RUN pip wheel --no-cache-dir --prefer-binary --wheel-dir /wheels -r requirements-gen.txt

# ==========================================
# STAGE 2: Final Runtime Image
# ==========================================
FROM python:3.10-slim

WORKDIR /app

# Copy compiled wheels from the builder stage
COPY --from=builder /wheels /wheels

# Install from wheels and remove the wheel files to save space
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels

# Copy application source code
COPY . .

# Execute the data generator script
CMD ["python", "generators/online_stream_gen.py"]