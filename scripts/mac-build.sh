#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF' 1>&2
Usage:
  bash scripts/mac-build.sh [--skip-build] [--skip-test] [--max-perf]

Builds and tests Agave on macOS (Apple Silicon / Intel).

Options:
  --skip-build   Skip the release build step.
  --skip-test    Skip running tests.
  --max-perf     Enable fat LTO + codegen-units=1 (slower build, better runtime).

Env vars:
  BUILD_JOBS     Override cargo jobs (default: sysctl hw.ncpu).
  TARGET_CPU     Override target CPU (default: native). Set TARGET_CPU=generic to disable.
  RUSTFLAGS      Extra rustc flags (this script appends -C target-cpu=native by default).
EOF
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }
need_cmd() { have_cmd "$1" || { echo "error: missing required command: $1" >&2; exit 1; }; }

SKIP_BUILD=0
SKIP_TEST=0
MAX_PERF=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-test)  SKIP_TEST=1;  shift ;;
    --max-perf)   MAX_PERF=1;   shift ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$(uname)" != "Darwin" ]]; then
  echo "error: mac-build.sh is intended for macOS hosts" >&2
  exit 2
fi

need_cmd rustup
need_cmd cargo
need_cmd brew

# ── Homebrew paths ──────────────────────────────────────────────────
BREW_PREFIX="$(brew --prefix)"

# LLVM / libclang (needed by rocksdb bindgen)
LLVM_PREFIX="${BREW_PREFIX}/opt/llvm"
if [[ ! -d "$LLVM_PREFIX" ]]; then
  echo "error: llvm not found — run: brew install llvm" >&2
  exit 1
fi
export PATH="${LLVM_PREFIX}/bin:${PATH}"
export LDFLAGS="-L${LLVM_PREFIX}/lib ${LDFLAGS:-}"
export CPPFLAGS="-I${LLVM_PREFIX}/include ${CPPFLAGS:-}"
export LIBCLANG_PATH="${LLVM_PREFIX}/lib"
export DYLD_FALLBACK_LIBRARY_PATH="${LLVM_PREFIX}/lib:${DYLD_FALLBACK_LIBRARY_PATH:-}"

# OpenSSL
OPENSSL_PREFIX="${BREW_PREFIX}/opt/openssl@3"
if [[ ! -d "$OPENSSL_PREFIX" ]]; then
  echo "error: openssl@3 not found — run: brew install openssl@3" >&2
  exit 1
fi
export OPENSSL_DIR="$OPENSSL_PREFIX"
export PKG_CONFIG_PATH="${OPENSSL_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

# Protobuf
if ! have_cmd protoc; then
  PROTOC="${BREW_PREFIX}/opt/protobuf/bin/protoc"
  if [[ ! -x "$PROTOC" ]]; then
    echo "error: protoc not found — run: brew install protobuf" >&2
    exit 1
  fi
  export PROTOC
fi

# ── Repo root ───────────────────────────────────────────────────────
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agave_root="$(cd "$script_dir/.." && pwd)"
cd "$agave_root"

# ── Jobs ────────────────────────────────────────────────────────────
jobs="${BUILD_JOBS:-}"
if [[ -z "$jobs" ]]; then
  jobs="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"
fi

# ── RUSTFLAGS ───────────────────────────────────────────────────────
target_cpu="${TARGET_CPU:-native}"
if [[ "$target_cpu" != "generic" ]]; then
  export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C target-cpu=${target_cpu}"
fi

if [[ "$MAX_PERF" -eq 1 ]]; then
  export CARGO_PROFILE_RELEASE_LTO=fat
  export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
fi

# ── Build ───────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "==> Building agave-validator (release, -j${jobs})..." >&2
  set -x
  ./cargo build --release -j "$jobs" -p agave-validator
  ./cargo build --release -j "$jobs" -p solana-keygen
  set +x

  bin="$agave_root/target/release/agave-validator"
  if [[ ! -x "$bin" ]]; then
    echo "error: build succeeded but $bin is missing" >&2
    exit 1
  fi
  echo "==> Build complete: $bin" >&2
fi

# ── Test ────────────────────────────────────────────────────────────
if [[ "$SKIP_TEST" -eq 0 ]]; then
  echo "==> Running solana-core tests (-j${jobs})..." >&2
  set -x
  ./cargo test -j "$jobs" -p solana-core
  set +x
  echo "==> Tests passed." >&2
fi

echo "==> Done." >&2
