#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../agave/scripts/devnet/common.sh
source "$script_dir/common.sh"

usage() {
  cat <<'EOF'
Usage:
  start-validator.sh [--env-file FILE] [--node-name NAME] [--print-command]

This script starts a validator using environment variables that are easy to use from systemd.
The systemd unit loads /etc/agave/devnet/<name>.env before calling this script, but you can also
run it manually with --env-file for validation or dry runs.

Important env vars:
  AGAVE_VALIDATOR_BIN
  IDENTITY_KEYPAIR
  VOTE_ACCOUNT_KEYPAIR        (required unless NO_VOTING=true)
  AUTHORIZED_VOTER_KEYPAIR    (optional)
  LEDGER_DIR
  ACCOUNTS_DIR
  SNAPSHOTS_DIR
  ENTRYPOINTS                 (comma or space separated)
  KNOWN_VALIDATORS            (comma or space separated)
  EXPECTED_GENESIS_HASH       (optional for a bootstrap validator)
  EXPECTED_SHRED_VERSION      (optional for a bootstrap validator)
  BIND_ADDRESS
  RPC_BIND_ADDRESS
  PUBLIC_RPC_ADDRESS
  RPC_PORT
  GOSSIP_PORT
  DYNAMIC_PORT_RANGE
  PRIVATE_RPC                 (true/false)
  FULL_RPC_API                (true/false)
  NO_VOTING                   (true/false)
  RPC_FAUCET_ADDRESS
  ENABLE_RPC_TRANSACTION_HISTORY (true/false)
  ENABLE_EXTENDED_TX_METADATA_STORAGE (true/false)
  FAIR_ENABLE                 (true/false)
  FAIR_WITNESS_QUORUM
  SOLANACDN_ENABLE            (true/false)
  SOLANACDN_MODE              (hybrid, only, all)
  SOLANACDN_API_BASE
  SOLANACDN_API_TOKEN
  SOLANACDN_POPS              (comma or space separated)
  SOLANACDN_CONTROL           (comma or space separated)
  SOLANACDN_SERVER_NAME
  SOLANACDN_TLS_CA_CERT_PATH
  SOLANACDN_TLS_INSECURE_SKIP_VERIFY
  SOLANACDN_METRICS_ADDR
  LIMIT_LEDGER_SIZE           (true/false)
  EXTRA_FLAGS                 (simple space-delimited flags)
EOF
}

env_file=""
node_name="${AGAVE_NODE_NAME:-}"
print_only="${PRINT_COMMAND:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      [[ $# -ge 2 ]] || die "--env-file requires a value"
      env_file="$2"
      shift 2
      ;;
    --node-name)
      [[ $# -ge 2 ]] || die "--node-name requires a value"
      node_name="$2"
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

role="${AGAVE_ROLE:-validator}"
node_name="${node_name:-$role}"

AGAVE_VALIDATOR_BIN="${AGAVE_VALIDATOR_BIN:-/opt/agave/bin/agave-validator}"
IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-}"
VOTE_ACCOUNT_KEYPAIR="${VOTE_ACCOUNT_KEYPAIR:-}"
AUTHORIZED_VOTER_KEYPAIR="${AUTHORIZED_VOTER_KEYPAIR:-}"
LEDGER_DIR="${LEDGER_DIR:-/var/lib/agave/$node_name/ledger}"
ACCOUNTS_DIR="${ACCOUNTS_DIR:-/var/lib/agave/$node_name/accounts}"
SNAPSHOTS_DIR="${SNAPSHOTS_DIR:-/var/lib/agave/$node_name/snapshots}"
BIND_ADDRESS="${BIND_ADDRESS:-}"
RPC_BIND_ADDRESS="${RPC_BIND_ADDRESS:-127.0.0.1}"
PUBLIC_RPC_ADDRESS="${PUBLIC_RPC_ADDRESS:-}"
RPC_PORT="${RPC_PORT:-8899}"
GOSSIP_PORT="${GOSSIP_PORT:-8001}"
DYNAMIC_PORT_RANGE="${DYNAMIC_PORT_RANGE:-8002-8020}"
PRIVATE_RPC="${PRIVATE_RPC:-true}"
FULL_RPC_API="${FULL_RPC_API:-false}"
NO_VOTING="${NO_VOTING:-false}"
LIMIT_LEDGER_SIZE="${LIMIT_LEDGER_SIZE:-true}"
SOLANACDN_ENABLE="${SOLANACDN_ENABLE:-true}"
SOLANACDN_MODE="${SOLANACDN_MODE:-hybrid}"
FAIR_ENABLE="${FAIR_ENABLE:-true}"
ENABLE_RPC_TRANSACTION_HISTORY="${ENABLE_RPC_TRANSACTION_HISTORY:-false}"
ENABLE_EXTENDED_TX_METADATA_STORAGE="${ENABLE_EXTENDED_TX_METADATA_STORAGE:-false}"

[[ -x "$AGAVE_VALIDATOR_BIN" ]] || die "validator binary not found or not executable: $AGAVE_VALIDATOR_BIN"
[[ -n "$IDENTITY_KEYPAIR" ]] || die "IDENTITY_KEYPAIR is required"
if ! is_true "$NO_VOTING"; then
  [[ -n "$VOTE_ACCOUNT_KEYPAIR" ]] || die "VOTE_ACCOUNT_KEYPAIR is required unless NO_VOTING=true"
fi
if is_true "$FAIR_ENABLE" && ! is_true "$SOLANACDN_ENABLE"; then
  die "FAIR_ENABLE=true requires SOLANACDN_ENABLE=true"
fi
if is_true "$SOLANACDN_ENABLE" \
  && [[ -z "${SOLANACDN_API_TOKEN:-}" ]] \
  && [[ -z "${SOLANACDN_POPS:-}" ]] \
  && [[ -z "${SOLANACDN_CONTROL:-}" ]]; then
  die "SOLANACDN_ENABLE=true requires SOLANACDN_API_TOKEN, SOLANACDN_POPS, or SOLANACDN_CONTROL"
fi

if ! is_true "$print_only"; then
  mkdir -p "$LEDGER_DIR" "$ACCOUNTS_DIR" "$SNAPSHOTS_DIR"
fi

args=(
  --identity "$IDENTITY_KEYPAIR"
  --ledger "$LEDGER_DIR"
  --accounts "$ACCOUNTS_DIR"
  --snapshots "$SNAPSHOTS_DIR"
  --rpc-bind-address "$RPC_BIND_ADDRESS"
  --rpc-port "$RPC_PORT"
  --gossip-port "$GOSSIP_PORT"
  --dynamic-port-range "$DYNAMIC_PORT_RANGE"
)

if ! is_true "$NO_VOTING"; then
  args+=(--vote-account "$VOTE_ACCOUNT_KEYPAIR")
fi
append_flag_if_set args --bind-address "$BIND_ADDRESS"
append_flag_if_set args --authorized-voter "$AUTHORIZED_VOTER_KEYPAIR"
append_repeated_flags args --entrypoint "${ENTRYPOINTS:-}"
append_repeated_flags args --known-validator "${KNOWN_VALIDATORS:-}"
append_flag_if_set args --expected-genesis-hash "${EXPECTED_GENESIS_HASH:-}"
append_flag_if_set args --expected-shred-version "${EXPECTED_SHRED_VERSION:-}"
append_flag_if_set args --public-rpc-address "$PUBLIC_RPC_ADDRESS"
append_flag_if_set args --rpc-faucet-address "${RPC_FAUCET_ADDRESS:-}"
append_bool_flag_if_true args --private-rpc "$PRIVATE_RPC"
append_bool_flag_if_true args --full-rpc-api "$FULL_RPC_API"
append_bool_flag_if_true args --no-voting "$NO_VOTING"
append_bool_flag_if_true args --limit-ledger-size "$LIMIT_LEDGER_SIZE"
append_bool_flag_if_true args --enable-rpc-transaction-history "$ENABLE_RPC_TRANSACTION_HISTORY"
append_bool_flag_if_true args --enable-extended-tx-metadata-storage "$ENABLE_EXTENDED_TX_METADATA_STORAGE"

if is_true "$SOLANACDN_ENABLE"; then
  case "$SOLANACDN_MODE" in
    hybrid)
      args+=(--solanacdn-hybrid)
      ;;
    only)
      args+=(--solanacdn-only)
      ;;
    all)
      ;;
    *)
      die "SOLANACDN_MODE must be one of: hybrid, only, all"
      ;;
  esac

  append_flag_if_set args --solanacdn-api-base "${SOLANACDN_API_BASE:-}"
  append_flag_if_set args --solanacdn-api-token "${SOLANACDN_API_TOKEN:-}"
  append_repeated_flags args --solanacdn-pop "${SOLANACDN_POPS:-}"
  append_repeated_flags args --solanacdn-control "${SOLANACDN_CONTROL:-}"
  append_flag_if_set args --solanacdn-server-name "${SOLANACDN_SERVER_NAME:-}"
  append_flag_if_set args --solanacdn-tls-ca-cert-path "${SOLANACDN_TLS_CA_CERT_PATH:-}"
  append_bool_flag_if_true args --solanacdn-tls-insecure-skip-verify "${SOLANACDN_TLS_INSECURE_SKIP_VERIFY:-false}"
  append_flag_if_set args --solanacdn-metrics-addr "${SOLANACDN_METRICS_ADDR:-}"
fi

if is_true "$FAIR_ENABLE"; then
  args+=(--fair-max-protection)
  append_flag_if_set args --fair-slashing-witness-quorum "${FAIR_WITNESS_QUORUM:-}"
fi

if [[ -n "${EXTRA_FLAGS:-}" ]]; then
  read -r -a extra_flags <<< "${EXTRA_FLAGS}"
  args+=("${extra_flags[@]}")
fi

command=("$AGAVE_VALIDATOR_BIN" "${args[@]}")

if is_true "$print_only"; then
  print_command "${command[@]}"
  exit 0
fi

print_command "${command[@]}"
exec "${command[@]}"
