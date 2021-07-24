FROM clux/muslrust
# download the index
RUN cargo search lazy_static
ADD Cargo.toml Cargo.lock ./
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    cargo check --release
