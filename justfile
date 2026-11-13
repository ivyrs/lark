default: check

check:
    cargo clippy --workspace -- -D warnings
    cargo test --workspace

# Run the daemon against a music dir (null output: no device needed)
dev dir="~/Music" output="default":
    cargo build --workspace
    LARK_LOG=larkd=debug,info ./target/debug/larkd --music-dir {{dir}} --output {{output}}

tui:
    cargo run -q -p lark

release:
    cargo build --workspace --release
