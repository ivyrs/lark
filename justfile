default: check

check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo test --workspace --locked
    python3 -m unittest discover -s plugins/navidrome -p 'test_*.py' -v
    python3 -m unittest discover -s plugins/mpd -p 'test_*.py' -v

# Isolated local/Navidrome/MPD integrations (Python stdlib; optional LARK_RMPC_BIN).
smoke:
    cargo build --workspace --locked
    python3 scripts/smoke.py
    python3 scripts/navidrome_smoke.py
    python3 scripts/mpd_smoke.py

# Run the daemon against a music dir (null output: no device needed)
dev dir=(env("HOME") / "Music") output="default":
    cargo build --workspace
    LARK_LOG=larkd=debug,info ./target/debug/larkd --music-dir {{quote(dir)}} --output {{quote(output)}}

tui:
    cargo run -q -p lark

release:
    cargo build --workspace --release
