# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
#
# docker build -t grainlift-turso .
# docker run --rm -p 8080:8080 -v ./turso.toml:/etc/grainlift-turso/turso.toml:ro \
#   -e TURSO_APP_TOKEN grainlift-turso
#
# The configuration must listen on 0.0.0.0 with allow_insecure_remote = true,
# behind a load balancer or sidecar that terminates TLS.

FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin grainlift-turso

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/grainlift-turso --create-home grainlift
COPY --from=build /src/target/release/grainlift-turso /usr/local/bin/grainlift-turso
USER grainlift
WORKDIR /var/lib/grainlift-turso
EXPOSE 8080
ENTRYPOINT ["grainlift-turso"]
CMD ["serve", "--config", "/etc/grainlift-turso/turso.toml"]
