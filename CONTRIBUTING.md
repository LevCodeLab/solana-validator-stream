# contributing

Thanks for poking around the code! Keep PRs small, fast, and easy to read at 3am with half a cup of adrak chai left.

## golden rules

- Never block the validator thread inside `src/plugin.rs` or `src/ipc.rs` - no `Mutex::lock` stalls, no blocking I/O, only `try_send`.
- Keep wire changes backward-friendly in `src/events.rs` (`#[serde(default)]` on new optional fields so old readers do not panic).
- Do not touch `_create_plugin` unless you really mean it (when i wrote that `unsafe extern "C"` pointer only me and bhagwan knew how it worked, now only bhagwan knows).
- Always clean up temp `.sock` files in tests so `/tmp` does not look like a junkyard.
- Zero heavy gRPC or protobuf crates in `Cargo.toml` - this repo stays fast and lean.

## local dev commands

```bash
# fast compile check across lib, tests, and examples
cargo check --workspace --all-targets

# run all unit + UDS socket integration tests
cargo test --workspace

# run the live local UDS loopback demo
cargo run --example uds_loopback_stream
```

## sending a PR

- Branch off `main` (`feat/your-thing` or `fix/your-thing`).
- Add a test in `tests/stream_integration.rs` if you touch socket framing, reconnect logic, or ring buffer drops.
- Make sure `cargo check --workspace --all-targets` and `cargo test --workspace` pass with zero warnings before pushing.
