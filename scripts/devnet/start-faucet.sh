#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../agave/scripts/devnet/common.sh
source "$script_dir/common.sh"

usage() {
  cat <<'EOF'
Usage:
  start-faucet.sh [--env-file FILE] [--print-command]

The Solana faucet binary binds to TCP port 9900 by default. This helper only assembles the command
line and reads limits from an env file.

Important env vars:
  SOLANA_FAUCET_BIN
  FAUCET_KEYPAIR
  FAUCET_SLICE_SECS
  FAUCET_PER_TIME_CAP_SOL
  FAUCET_PER_REQUEST_CAP_SOL
  FAUCET_ALLOWED_IPS          (comma or space separated)
EOF
}

env_file=""
print_only="${PRINT_COMMAND:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      [[ $# -ge 2 ]] || die "--env-file requires a value"
      env_file="$2"
      shift 2
      ;;
    --print-command)
      print_only=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

source_env_file_if_present "$env_file"
if [[ -n "$env_file" ]]; then
  source_env_file_if_present "${env_file%.env}.secrets.env"
fi

SOLANA_FAUCET_BIN="${SOLANA_FAUCET_BIN:-/opt/agave/bin/solana-faucet}"
FAUCET_KEYPAIR="${FAUCET_KEYPAIR:-}"

[[ -x "$SOLANA_FAUCET_BIN" ]] || die "faucet binary not found or not executable: $SOLANA_FAUCET_BIN"
[[ -n "$FAUCET_KEYPAIR" ]] || die "FAUCET_KEYPAIR is required"

args=(
  --keypair "$FAUCET_KEYPAIR"
)

append_flag_if_set args --slice "${FAUCET_SLICE_SECS:-}"
append_flag_if_set args --per-time-cap "${FAUCET_PER_TIME_CAP_SOL:-}"
append_flag_if_set args --per-request-cap "${FAUCET_PER_REQUEST_CAP_SOL:-}"
append_repeated_flags args --allow-ip "${FAUCET_ALLOWED_IPS:-}"

command=("$SOLANA_FAUCET_BIN" "${args[@]}")

if is_true "$print_only"; then
  print_command "${command[@]}"
  exit 0
fi

print_command "${command[@]}"
exec "${command[@]}"
