# kv5

A Redis-like KV database written in Rust.

## Build & Test

```bash
cargo build --release
```

Run the full verification suite (fmt, clippy, test):
```bash
./test.sh
```

Run a single test:
```bash
cargo test <test_name>
```

Run the server:
```bash
cargo run --bin kv5-server
```

Run the CLI:
```bash
cargo run --bin kv5-cli
```

## Binaries

- `kv5-server` - Entry point: `src/main.rs`
- `kv5-cli` - Entry point: `src/cli.rs`

## Verification Order

`cargo fmt --check` -> `cargo clippy -- -D warnings` -> `cargo test`

This project uses clippy with `-D warnings` (treats warnings as errors).

## Architecture

- `src/cmd.rs` - Command parsing and handling
- `src/server.rs` - Server setup
- `src/db.rs` - Core key-value store
- `src/pubsub.rs` - Pub/sub functionality
- `src/resp.rs` - RESP protocol encoding/decoding
- `src/store/` - Storage implementations