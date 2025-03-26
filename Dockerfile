# Build stage
FROM postgres:17 AS builder

# Install build dependencies,  including Postgres server package to get header files in `/usr/include/postgresql`
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    gnupg \
    lsb-release \
    libclang-dev \
    clang \
    postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*

# Install FoundationDB client
RUN curl -LO https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_aarch64.deb && \
    dpkg -i foundationdb-clients_7.3.63-1_aarch64.deb && \
    rm foundationdb-clients_7.3.63-1_aarch64.deb

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-pgrx and initialize it
RUN cargo install --locked cargo-pgrx && \
    mkdir -p /root/.pgrx && \
    cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config

# Copy the extension source code
WORKDIR /usr/src/pgfdb
COPY . .

# Build and package the extension
RUN cargo pgrx package --pg-config /usr/lib/postgresql/17/bin/pg_config

# Final stage
FROM postgres:17

# Install FoundationDB client (needed at runtime)
RUN apt-get update && apt-get install -y \
    curl \
    libssl3 \
    ca-certificates \
    libc6 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LO https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_aarch64.deb && \
    dpkg -i foundationdb-clients_7.3.63-1_aarch64.deb && \
    rm foundationdb-clients_7.3.63-1_aarch64.deb

# Copy only the built extension files from the builder stage
COPY --from=builder /usr/src/pgfdb/target/release/pgfdb-pg17/usr/lib/postgresql/17/lib/* /usr/lib/postgresql/17/lib/
COPY --from=builder /usr/src/pgfdb/target/release/pgfdb-pg17/usr/share/postgresql/17/extension/* /usr/share/postgresql/17/extension/

# Add a script to create and initialise the extension on database startup
RUN echo "CREATE EXTENSION pgfdb; ALTER SYSTEM SET default_table_access_method = pgfdb;" \
    > /docker-entrypoint-initdb.d/create_extension.sql

# Expose the PostgreSQL port
EXPOSE 5432

# The official postgres image already has a proper entrypoint
