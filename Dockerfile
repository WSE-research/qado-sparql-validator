FROM rust:latest as builder
WORKDIR /usr/src/qado_sparql_validator
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/qado_sparql_validator /usr/local/bin/qado_sparql_validator
ENTRYPOINT ["qado_sparql_validator"]
