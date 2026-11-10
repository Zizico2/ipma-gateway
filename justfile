set positional-arguments

default:
    @just --list

run *args:
    cargo run --locked -p ipma-gateway -- "$@"

build:
    cargo build --locked --workspace

release:
    cargo build --locked --workspace --release

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

check:
    cargo check --locked --workspace --all-targets

lint:
    cargo clippy --locked --workspace --all-targets -- -D warnings

test:
    cargo test --locked --workspace --all-targets
    cargo test --locked --workspace --doc

# Ping is forwarded to the adapter, so this checks both services.
ping address='127.0.0.1:50052':
    buf curl --protocol grpc --http2-prior-knowledge --data '{}' "http://$1/ipma.proxy.v1.ProxyService/Ping"

# All supported selections; the extra forecast count is explicitly opt-in.
smoke address='http://127.0.0.1:50052' extra_calls='0':
    cargo run --locked -p ipma-gateway --example smoke -- "$1" "$2"

verify: fmt-check check lint test

clean:
    cargo clean
