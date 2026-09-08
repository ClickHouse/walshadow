FROM rust:1.96-slim AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config && rm -rf /var/lib/apt/lists/*
COPY engine/Cargo.toml engine/Cargo.lock* ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src
COPY engine/src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/market-engine /usr/local/bin/market-engine
COPY config /app/config
COPY sql /app/sql
COPY engine/static /app/engine/static
RUN mkdir -p /app/results
EXPOSE 8080
CMD ["market-engine"]
