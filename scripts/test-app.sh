#!/usr/bin/env bash
# Build a test-only carrier against an existing, read-only app_sqlite checkout.
set -euo pipefail
if [[ $# != 1 ]]; then
  echo "usage: bash scripts/test-app.sh /path/to/app_sqlite" >&2
  exit 2
fi
repo=$(cd "$(dirname "$0")/.." && pwd)
app=$(cd "$1" && pwd)
build="$repo/.artifacts/app"
mkdir -p "$build/tmp" "$build/cargo-home"
export CARGO_HOME="$build/cargo-home" TMPDIR="$build/tmp"
cargo build --manifest-path "$app/Cargo.toml" --locked --lib --target-dir "$build/target"
tokio=("$build"/target/debug/deps/libtokio-*.rlib)
[[ ${#tokio[@]} == 1 ]] || { echo "use a clean test target directory" >&2; exit 1; }
rustc --edition=2024 "$repo/tests/app_peer.rs" \
  -L "dependency=$build/target/debug/deps" \
  --extern "app_sqlite=$build/target/debug/libapp_sqlite.rlib" \
  --extern "tokio=${tokio[0]}" -o "$build/app-peer"
cd "$repo"
TIANA_SQLITE_APP_PEER_BINARY="$build/app-peer" go test -race -count=1 -timeout 2m ./...
