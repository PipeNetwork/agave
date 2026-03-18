#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../agave/scripts/devnet/common.sh
source "$script_dir/common.sh"

usage() {
  cat <<'EOF'
Usage:
  create-genesis.sh [--env-file FILE] [--print-commands]

Creates a bootstrap validator ledger for a public devnet. Missing keypairs are generated in place.
The script refuses to overwrite an existing genesis archive.

Important env vars:
  SOLANA_KEYGEN_BIN
  SOLANA_GENESIS_BIN
  SOLANA_LEDGER_TOOL_BIN      (optional, used to write cluster-info.env)
  IDENTITY_KEYPAIR
  VOTE_ACCOUNT_KEYPAIR
  STAKE_ACCOUNT_KEYPAIR
  FAUCET_KEYPAIR
  LEDGER_DIR
  CLUSTER_TYPE
  HASHES_PER_TICK
  FAUCET_LAMPORTS
  BOOTSTRAP_VALIDATOR_LAMPORTS
  BOOTSTRAP_VALIDATOR_STAKE_LAMPORTS
  ENABLE_WARMUP_EPOCHS
  TICKS_PER_SLOT
  SLOTS_PER_EPOCH
  TARGET_LAMPORTS_PER_SIGNATURE
  CLUSTER_INFO_FILE
EOF
}

env_file=""
print_only=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      [[ $# -ge 2 ]] || die "--env-file requires a value"
      env_file="$2"
      shift 2
      ;;
    --print-commands)
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

SOLANA_KEYGEN_BIN="${SOLANA_KEYGEN_BIN:-solana-keygen}"
SOLANA_GENESIS_BIN="${SOLANA_GENESIS_BIN:-solana-genesis}"
SOLANA_LEDGER_TOOL_BIN="${SOLANA_LEDGER_TOOL_BIN:-solana-ledger-tool}"
IDENTITY_KEYPAIR="${IDENTITY_KEYPAIR:-}"
VOTE_ACCOUNT_KEYPAIR="${VOTE_ACCOUNT_KEYPAIR:-}"
STAKE_ACCOUNT_KEYPAIR="${STAKE_ACCOUNT_KEYPAIR:-}"
FAUCET_KEYPAIR="${FAUCET_KEYPAIR:-}"
LEDGER_DIR="${LEDGER_DIR:-}"
CLUSTER_TYPE="${CLUSTER_TYPE:-development}"
HASHES_PER_TICK="${HASHES_PER_TICK:-auto}"
FAUCET_LAMPORTS="${FAUCET_LAMPORTS:-500000000000000000}"
BOOTSTRAP_VALIDATOR_LAMPORTS="${BOOTSTRAP_VALIDATOR_LAMPORTS:-500000000000}"
BOOTSTRAP_VALIDATOR_STAKE_LAMPORTS="${BOOTSTRAP_VALIDATOR_STAKE_LAMPORTS:-500000000000}"
ENABLE_WARMUP_EPOCHS="${ENABLE_WARMUP_EPOCHS:-true}"
CLUSTER_INFO_FILE="${CLUSTER_INFO_FILE:-}"

[[ -n "$IDENTITY_KEYPAIR" ]] || die "IDENTITY_KEYPAIR is required"
[[ -n "$VOTE_ACCOUNT_KEYPAIR" ]] || die "VOTE_ACCOUNT_KEYPAIR is required"
[[ -n "$STAKE_ACCOUNT_KEYPAIR" ]] || die "STAKE_ACCOUNT_KEYPAIR is required"
[[ -n "$FAUCET_KEYPAIR" ]] || die "FAUCET_KEYPAIR is required"
[[ -n "$LEDGER_DIR" ]] || die "LEDGER_DIR is required"

if ! is_true "$print_only"; then
  have_cmd "$SOLANA_KEYGEN_BIN" || die "missing keygen binary: $SOLANA_KEYGEN_BIN"
  have_cmd "$SOLANA_GENESIS_BIN" || die "missing genesis binary: $SOLANA_GENESIS_BIN"
fi

mkdir -p \
  "$(dirname "$IDENTITY_KEYPAIR")" \
  "$(dirname "$VOTE_ACCOUNT_KEYPAIR")" \
  "$(dirname "$STAKE_ACCOUNT_KEYPAIR")" \
  "$(dirname "$FAUCET_KEYPAIR")" \
  "$LEDGER_DIR"

run_or_print() {
  if is_true "$print_only"; then
    print_command "$@"
  else
    "$@"
  fi
}

ensure_keypair() {
  local path="$1"

  if [[ -f "$path" ]]; then
    return
  fi

  run_or_print "$SOLANA_KEYGEN_BIN" new --no-passphrase -o "$path"
}

ensure_keypair "$IDENTITY_KEYPAIR"
ensure_keypair "$VOTE_ACCOUNT_KEYPAIR"
ensure_keypair "$STAKE_ACCOUNT_KEYPAIR"
ensure_keypair "$FAUCET_KEYPAIR"

if [[ -e "$LEDGER_DIR/genesis.bin" || -e "$LEDGER_DIR/genesis.tar.bz2" ]]; then
  die "ledger already contains a genesis archive: $LEDGER_DIR"
fi

genesis_args=(
  --ledger "$LEDGER_DIR"
  --cluster-type "$CLUSTER_TYPE"
  --faucet-pubkey "$FAUCET_KEYPAIR"
  --faucet-lamports "$FAUCET_LAMPORTS"
  --bootstrap-validator "$IDENTITY_KEYPAIR" "$VOTE_ACCOUNT_KEYPAIR" "$STAKE_ACCOUNT_KEYPAIR"
  --bootstrap-validator-lamports "$BOOTSTRAP_VALIDATOR_LAMPORTS"
  --bootstrap-validator-stake-lamports "$BOOTSTRAP_VALIDATOR_STAKE_LAMPORTS"
)

append_flag_if_set genesis_args --hashes-per-tick "$HASHES_PER_TICK"
append_flag_if_set genesis_args --ticks-per-slot "${TICKS_PER_SLOT:-}"
append_flag_if_set genesis_args --slots-per-epoch "${SLOTS_PER_EPOCH:-}"
append_flag_if_set genesis_args --target-lamports-per-signature "${TARGET_LAMPORTS_PER_SIGNATURE:-}"
append_bool_flag_if_true genesis_args --enable-warmup-epochs "$ENABLE_WARMUP_EPOCHS"

run_or_print "$SOLANA_GENESIS_BIN" "${genesis_args[@]}"

if is_true "$print_only"; then
  exit 0
fi

if [[ -z "$CLUSTER_INFO_FILE" ]]; then
  CLUSTER_INFO_FILE="$LEDGER_DIR/cluster-info.env"
fi

identity_pubkey="$("$SOLANA_KEYGEN_BIN" pubkey "$IDENTITY_KEYPAIR")"
vote_pubkey="$("$SOLANA_KEYGEN_BIN" pubkey "$VOTE_ACCOUNT_KEYPAIR")"
stake_pubkey="$("$SOLANA_KEYGEN_BIN" pubkey "$STAKE_ACCOUNT_KEYPAIR")"
faucet_pubkey="$("$SOLANA_KEYGEN_BIN" pubkey "$FAUCET_KEYPAIR")"

genesis_hash=""
shred_version=""
if have_cmd "$SOLANA_LEDGER_TOOL_BIN"; then
  genesis_hash="$("$SOLANA_LEDGER_TOOL_BIN" -l "$LEDGER_DIR" genesis-hash 2>/dev/null || true)"
  shred_version="$("$SOLANA_LEDGER_TOOL_BIN" -l "$LEDGER_DIR" shred-version 2>/dev/null || true)"
fi

cat > "$CLUSTER_INFO_FILE" <<EOF
BOOTSTRAP_IDENTITY_PUBKEY=$identity_pubkey
BOOTSTRAP_VOTE_PUBKEY=$vote_pubkey
BOOTSTRAP_STAKE_PUBKEY=$stake_pubkey
FAUCET_PUBKEY=$faucet_pubkey
EXPECTED_GENESIS_HASH=$genesis_hash
EXPECTED_SHRED_VERSION=$shred_version
LEDGER_DIR=$LEDGER_DIR
EOF

echo "wrote cluster info to $CLUSTER_INFO_FILE"
echo "bootstrap identity pubkey: $identity_pubkey"
echo "bootstrap vote pubkey: $vote_pubkey"
echo "faucet pubkey: $faucet_pubkey"
if [[ -n "$genesis_hash" ]]; then
  echo "genesis hash: $genesis_hash"
fi
if [[ -n "$shred_version" ]]; then
  echo "shred version: $shred_version"
fi
