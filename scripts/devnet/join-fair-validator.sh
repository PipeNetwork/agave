#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../agave/scripts/devnet/common.sh
source "$script_dir/common.sh"

usage() {
  cat <<'EOF'
Usage:
  join-fair-validator.sh \
    --identity FILE \
    --vote-account FILE \
    [--network-env-file FILE] \
    [--network-env-url URL] \
    [--write-env-file FILE] \
    [--write-secrets-file FILE] \
    --entrypoint HOST:PORT [--entrypoint HOST:PORT ...] \
    --known-validator PUBKEY [--known-validator PUBKEY ...] \
    --expected-genesis-hash HASH \
    --expected-shred-version N \
    [--solanacdn-api-token TOKEN | --solanacdn-pop IP:PORT ... | --solanacdn-control IP:PORT] \
    [options]

This is the short path for joining an existing public devnet as a voting validator with
fair ordering enabled. It wraps start-validator.sh and turns on:

  --solanacdn-hybrid
  --fair-max-protection

You can provide network settings either by flags, by pointing the script at a shell file, or by
fetching a published shell file from a URL:

  /opt/agave/devnet/join-fair-validator.sh \
    --network-env-file /etc/agave/devnet/public-fair-devnet.env \
    --identity /etc/agave/keys/validator-7-identity.json \
    --vote-account /etc/agave/keys/validator-7-vote-account.json \
    --solanacdn-api-token <TOKEN>

Or:

  SOLANACDN_API_TOKEN=<TOKEN> /opt/agave/devnet/join-fair-validator.sh \
    --network-env-url https://devnet.example.org/public-fair-devnet.env \
    --identity /etc/agave/keys/validator-7-identity.json \
    --vote-account /etc/agave/keys/validator-7-vote-account.json

To generate a `systemd` env file instead of starting immediately:

  SOLANACDN_API_TOKEN=<TOKEN> /opt/agave/devnet/join-fair-validator.sh \
    --network-env-url https://devnet.example.org/public-fair-devnet.env \
    --node-name validator-7 \
    --identity /etc/agave/keys/validator-7-identity.json \
    --vote-account /etc/agave/keys/validator-7-vote-account.json \
    --write-env-file /etc/agave/devnet/validator-7.env

Defaults:
  --node-name               fair-validator
  --ledger-dir              /var/lib/agave/<node>/ledger
  --accounts-dir            /var/lib/agave/<node>/accounts
  --snapshots-dir           /var/lib/agave/<node>/snapshots
  --rpc-bind-address        127.0.0.1
  --rpc-port                8899
  --gossip-port             8001
  --dynamic-port-range      8002-8020
  --solanacdn-mode          hybrid
  --solanacdn-metrics-addr  127.0.0.1:9100
  --fair-witness-quorum     3

Useful options:
  --bind-address HOST       Set this to the validator's public IP on simple public-IP hosts.
  --authorized-voter FILE   Optional authorized voter keypair.
  --write-env-file FILE     Write a validator env file instead of starting immediately.
  --write-secrets-file FILE Write a separate secrets env file. Default: <env>.secrets.env when needed.
  --print-command           Print the final agave-validator command and exit.

If your host is behind NAT or needs more specialized Agave networking flags, use start-validator.sh
or add EXTRA_FLAGS yourself.
EOF
}

network_env_file=""
network_env_url=""
write_env_file=""
write_secrets_file=""
default_network_env_file="/etc/agave/devnet/public-fair-devnet.env"
downloaded_network_env_file=""
original_args=("$@")

cleanup() {
  if [[ -n "$downloaded_network_env_file" && -f "$downloaded_network_env_file" ]]; then
    rm -f "$downloaded_network_env_file"
  fi
}

trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --network-env-file)
      [[ $# -ge 2 ]] || die "--network-env-file requires a value"
      network_env_file="$2"
      shift 2
      ;;
    --network-env-url)
      [[ $# -ge 2 ]] || die "--network-env-url requires a value"
      network_env_url="$2"
      shift 2
      ;;
    --write-env-file)
      [[ $# -ge 2 ]] || die "--write-env-file requires a value"
      write_env_file="$2"
      shift 2
      ;;
    --write-secrets-file)
      [[ $# -ge 2 ]] || die "--write-secrets-file requires a value"
      write_secrets_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [[ -n "$network_env_file" && -n "$network_env_url" ]]; then
  die "--network-env-file and --network-env-url are mutually exclusive"
fi

if [[ -n "$network_env_url" ]]; then
  have_cmd curl || die "curl is required for --network-env-url"
  downloaded_network_env_file="$(mktemp)"
  curl -fsSL "$network_env_url" -o "$downloaded_network_env_file"
  source_env_file_if_present "$downloaded_network_env_file"
elif [[ -n "$network_env_file" ]]; then
  source_env_file_if_present "$network_env_file"
elif [[ -f "$default_network_env_file" ]]; then
  source_env_file_if_present "$default_network_env_file"
fi

set -- "${original_args[@]}"

node_name="${AGAVE_NODE_NAME:-fair-validator}"
agave_validator_bin="${AGAVE_VALIDATOR_BIN:-/opt/agave/bin/agave-validator}"
identity_keypair="${IDENTITY_KEYPAIR:-}"
vote_account_keypair="${VOTE_ACCOUNT_KEYPAIR:-}"
authorized_voter_keypair="${AUTHORIZED_VOTER_KEYPAIR:-}"
ledger_dir="${LEDGER_DIR:-}"
accounts_dir="${ACCOUNTS_DIR:-}"
snapshots_dir="${SNAPSHOTS_DIR:-}"
bind_address="${BIND_ADDRESS:-}"
rpc_bind_address="${RPC_BIND_ADDRESS:-127.0.0.1}"
rpc_port="${RPC_PORT:-8899}"
gossip_port="${GOSSIP_PORT:-8001}"
dynamic_port_range="${DYNAMIC_PORT_RANGE:-8002-8020}"
entrypoints_env="${ENTRYPOINTS:-}"
known_validators_env="${KNOWN_VALIDATORS:-}"
expected_genesis_hash="${EXPECTED_GENESIS_HASH:-}"
expected_shred_version="${EXPECTED_SHRED_VERSION:-}"
solanacdn_mode="${SOLANACDN_MODE:-hybrid}"
solanacdn_api_base="${SOLANACDN_API_BASE:-https://api.pipedev.network}"
solanacdn_api_token="${SOLANACDN_API_TOKEN:-${SOLANACDN_AGENT_API_TOKEN:-${PIPE_API_KEY:-}}}"
solanacdn_control="${SOLANACDN_CONTROL:-}"
solanacdn_server_name="${SOLANACDN_SERVER_NAME:-}"
solanacdn_tls_ca_cert_path="${SOLANACDN_TLS_CA_CERT_PATH:-}"
solanacdn_tls_insecure_skip_verify="${SOLANACDN_TLS_INSECURE_SKIP_VERIFY:-false}"
solanacdn_metrics_addr="${SOLANACDN_METRICS_ADDR:-127.0.0.1:9100}"
fair_witness_quorum="${FAIR_WITNESS_QUORUM:-3}"
private_rpc="${PRIVATE_RPC:-true}"
limit_ledger_size="${LIMIT_LEDGER_SIZE:-true}"
print_only=false

declare -a entrypoints=()
declare -a known_validators=()
declare -a solanacdn_pops=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --network-env-file)
      [[ $# -ge 2 ]] || die "--network-env-file requires a value"
      network_env_file="$2"
      shift 2
      ;;
    --network-env-url)
      [[ $# -ge 2 ]] || die "--network-env-url requires a value"
      network_env_url="$2"
      shift 2
      ;;
    --write-env-file)
      [[ $# -ge 2 ]] || die "--write-env-file requires a value"
      write_env_file="$2"
      shift 2
      ;;
    --write-secrets-file)
      [[ $# -ge 2 ]] || die "--write-secrets-file requires a value"
      write_secrets_file="$2"
      shift 2
      ;;
    --node-name)
      [[ $# -ge 2 ]] || die "--node-name requires a value"
      node_name="$2"
      shift 2
      ;;
    --agave-validator-bin)
      [[ $# -ge 2 ]] || die "--agave-validator-bin requires a value"
      agave_validator_bin="$2"
      shift 2
      ;;
    --identity)
      [[ $# -ge 2 ]] || die "--identity requires a value"
      identity_keypair="$2"
      shift 2
      ;;
    --vote-account)
      [[ $# -ge 2 ]] || die "--vote-account requires a value"
      vote_account_keypair="$2"
      shift 2
      ;;
    --authorized-voter)
      [[ $# -ge 2 ]] || die "--authorized-voter requires a value"
      authorized_voter_keypair="$2"
      shift 2
      ;;
    --ledger-dir)
      [[ $# -ge 2 ]] || die "--ledger-dir requires a value"
      ledger_dir="$2"
      shift 2
      ;;
    --accounts-dir)
      [[ $# -ge 2 ]] || die "--accounts-dir requires a value"
      accounts_dir="$2"
      shift 2
      ;;
    --snapshots-dir)
      [[ $# -ge 2 ]] || die "--snapshots-dir requires a value"
      snapshots_dir="$2"
      shift 2
      ;;
    --bind-address)
      [[ $# -ge 2 ]] || die "--bind-address requires a value"
      bind_address="$2"
      shift 2
      ;;
    --rpc-bind-address)
      [[ $# -ge 2 ]] || die "--rpc-bind-address requires a value"
      rpc_bind_address="$2"
      shift 2
      ;;
    --rpc-port)
      [[ $# -ge 2 ]] || die "--rpc-port requires a value"
      rpc_port="$2"
      shift 2
      ;;
    --gossip-port)
      [[ $# -ge 2 ]] || die "--gossip-port requires a value"
      gossip_port="$2"
      shift 2
      ;;
    --dynamic-port-range)
      [[ $# -ge 2 ]] || die "--dynamic-port-range requires a value"
      dynamic_port_range="$2"
      shift 2
      ;;
    --entrypoint)
      [[ $# -ge 2 ]] || die "--entrypoint requires a value"
      entrypoints+=("$2")
      shift 2
      ;;
    --known-validator)
      [[ $# -ge 2 ]] || die "--known-validator requires a value"
      known_validators+=("$2")
      shift 2
      ;;
    --expected-genesis-hash)
      [[ $# -ge 2 ]] || die "--expected-genesis-hash requires a value"
      expected_genesis_hash="$2"
      shift 2
      ;;
    --expected-shred-version)
      [[ $# -ge 2 ]] || die "--expected-shred-version requires a value"
      expected_shred_version="$2"
      shift 2
      ;;
    --solanacdn-mode)
      [[ $# -ge 2 ]] || die "--solanacdn-mode requires a value"
      solanacdn_mode="$2"
      shift 2
      ;;
    --solanacdn-api-base)
      [[ $# -ge 2 ]] || die "--solanacdn-api-base requires a value"
      solanacdn_api_base="$2"
      shift 2
      ;;
    --solanacdn-api-token)
      [[ $# -ge 2 ]] || die "--solanacdn-api-token requires a value"
      solanacdn_api_token="$2"
      shift 2
      ;;
    --solanacdn-pop)
      [[ $# -ge 2 ]] || die "--solanacdn-pop requires a value"
      solanacdn_pops+=("$2")
      shift 2
      ;;
    --solanacdn-control)
      [[ $# -ge 2 ]] || die "--solanacdn-control requires a value"
      solanacdn_control="$2"
      shift 2
      ;;
    --solanacdn-server-name)
      [[ $# -ge 2 ]] || die "--solanacdn-server-name requires a value"
      solanacdn_server_name="$2"
      shift 2
      ;;
    --solanacdn-tls-ca-cert-path)
      [[ $# -ge 2 ]] || die "--solanacdn-tls-ca-cert-path requires a value"
      solanacdn_tls_ca_cert_path="$2"
      shift 2
      ;;
    --solanacdn-tls-insecure-skip-verify)
      solanacdn_tls_insecure_skip_verify=true
      shift
      ;;
    --solanacdn-metrics-addr)
      [[ $# -ge 2 ]] || die "--solanacdn-metrics-addr requires a value"
      solanacdn_metrics_addr="$2"
      shift 2
      ;;
    --fair-witness-quorum|--fair-slashing-witness-quorum)
      [[ $# -ge 2 ]] || die "$1 requires a value"
      fair_witness_quorum="$2"
      shift 2
      ;;
    --public-rpc)
      private_rpc=false
      shift
      ;;
    --no-limit-ledger-size)
      limit_ledger_size=false
      shift
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

if [[ ${#entrypoints[@]} -gt 0 ]]; then
  entrypoints_env="${entrypoints[*]}"
fi
if [[ ${#known_validators[@]} -gt 0 ]]; then
  known_validators_env="${known_validators[*]}"
fi
if [[ ${#solanacdn_pops[@]} -gt 0 ]]; then
  SOLANACDN_POPS="${solanacdn_pops[*]}"
else
  SOLANACDN_POPS="${SOLANACDN_POPS:-}"
fi

[[ -n "$identity_keypair" ]] || die "--identity or IDENTITY_KEYPAIR is required"
[[ -n "$vote_account_keypair" ]] || die "--vote-account or VOTE_ACCOUNT_KEYPAIR is required"
[[ -n "$entrypoints_env" ]] || die "--entrypoint or ENTRYPOINTS is required"
[[ -n "$known_validators_env" ]] || die "--known-validator or KNOWN_VALIDATORS is required"
[[ -n "$expected_genesis_hash" ]] || die "--expected-genesis-hash or EXPECTED_GENESIS_HASH is required"
[[ -n "$expected_shred_version" ]] || die "--expected-shred-version or EXPECTED_SHRED_VERSION is required"
if [[ -z "$solanacdn_api_token" && -z "$SOLANACDN_POPS" && -z "$solanacdn_control" ]]; then
  die "SolanaCDN discovery is required: set --solanacdn-api-token, --solanacdn-pop, or --solanacdn-control"
fi

ledger_dir="${ledger_dir:-/var/lib/agave/$node_name/ledger}"
accounts_dir="${accounts_dir:-/var/lib/agave/$node_name/accounts}"
snapshots_dir="${snapshots_dir:-/var/lib/agave/$node_name/snapshots}"

export AGAVE_ROLE=validator
export AGAVE_NODE_NAME="$node_name"
export AGAVE_VALIDATOR_BIN="$agave_validator_bin"
export IDENTITY_KEYPAIR="$identity_keypair"
export VOTE_ACCOUNT_KEYPAIR="$vote_account_keypair"
export AUTHORIZED_VOTER_KEYPAIR="$authorized_voter_keypair"
export LEDGER_DIR="$ledger_dir"
export ACCOUNTS_DIR="$accounts_dir"
export SNAPSHOTS_DIR="$snapshots_dir"
export BIND_ADDRESS="$bind_address"
export RPC_BIND_ADDRESS="$rpc_bind_address"
export RPC_PORT="$rpc_port"
export GOSSIP_PORT="$gossip_port"
export DYNAMIC_PORT_RANGE="$dynamic_port_range"
export ENTRYPOINTS="$entrypoints_env"
export KNOWN_VALIDATORS="$known_validators_env"
export EXPECTED_GENESIS_HASH="$expected_genesis_hash"
export EXPECTED_SHRED_VERSION="$expected_shred_version"
export PRIVATE_RPC="$private_rpc"
export LIMIT_LEDGER_SIZE="$limit_ledger_size"
export SOLANACDN_ENABLE=true
export SOLANACDN_MODE="$solanacdn_mode"
export SOLANACDN_API_BASE="$solanacdn_api_base"
export SOLANACDN_API_TOKEN="$solanacdn_api_token"
export SOLANACDN_POPS="$SOLANACDN_POPS"
export SOLANACDN_CONTROL="$solanacdn_control"
export SOLANACDN_SERVER_NAME="$solanacdn_server_name"
export SOLANACDN_TLS_CA_CERT_PATH="$solanacdn_tls_ca_cert_path"
export SOLANACDN_TLS_INSECURE_SKIP_VERIFY="$solanacdn_tls_insecure_skip_verify"
export SOLANACDN_METRICS_ADDR="$solanacdn_metrics_addr"
export FAIR_ENABLE=true
export FAIR_WITNESS_QUORUM="$fair_witness_quorum"

write_env_line() {
  local key="$1"
  local value="$2"
  printf '%s=%q\n' "$key" "$value"
}

write_optional_env_line() {
  local key="$1"
  local value="$2"
  if [[ -n "$value" ]]; then
    write_env_line "$key" "$value"
  fi
}

if [[ -n "$write_env_file" ]]; then
  mkdir -p "$(dirname "$write_env_file")"

  {
    echo "# Generated by join-fair-validator.sh"
    write_env_line "AGAVE_ROLE" "validator"
    write_env_line "AGAVE_VALIDATOR_BIN" "$agave_validator_bin"
    write_env_line "IDENTITY_KEYPAIR" "$identity_keypair"
    write_env_line "VOTE_ACCOUNT_KEYPAIR" "$vote_account_keypair"
    write_optional_env_line "AUTHORIZED_VOTER_KEYPAIR" "$authorized_voter_keypair"
    write_env_line "LEDGER_DIR" "$ledger_dir"
    write_env_line "ACCOUNTS_DIR" "$accounts_dir"
    write_env_line "SNAPSHOTS_DIR" "$snapshots_dir"
    write_optional_env_line "BIND_ADDRESS" "$bind_address"
    write_env_line "RPC_BIND_ADDRESS" "$rpc_bind_address"
    write_env_line "RPC_PORT" "$rpc_port"
    write_env_line "GOSSIP_PORT" "$gossip_port"
    write_env_line "DYNAMIC_PORT_RANGE" "$dynamic_port_range"
    write_env_line "ENTRYPOINTS" "$entrypoints_env"
    write_env_line "KNOWN_VALIDATORS" "$known_validators_env"
    write_env_line "EXPECTED_GENESIS_HASH" "$expected_genesis_hash"
    write_env_line "EXPECTED_SHRED_VERSION" "$expected_shred_version"
    write_env_line "PRIVATE_RPC" "$private_rpc"
    write_env_line "LIMIT_LEDGER_SIZE" "$limit_ledger_size"
    write_env_line "SOLANACDN_ENABLE" "true"
    write_env_line "SOLANACDN_MODE" "$solanacdn_mode"
    write_optional_env_line "SOLANACDN_API_BASE" "$solanacdn_api_base"
    write_optional_env_line "SOLANACDN_POPS" "$SOLANACDN_POPS"
    write_optional_env_line "SOLANACDN_CONTROL" "$solanacdn_control"
    write_optional_env_line "SOLANACDN_SERVER_NAME" "$solanacdn_server_name"
    write_optional_env_line "SOLANACDN_TLS_CA_CERT_PATH" "$solanacdn_tls_ca_cert_path"
    write_env_line "SOLANACDN_TLS_INSECURE_SKIP_VERIFY" "$solanacdn_tls_insecure_skip_verify"
    write_env_line "SOLANACDN_METRICS_ADDR" "$solanacdn_metrics_addr"
    write_env_line "FAIR_ENABLE" "true"
    write_env_line "FAIR_WITNESS_QUORUM" "$fair_witness_quorum"
  } > "$write_env_file"

  if [[ -n "$solanacdn_api_token" ]]; then
    if [[ -z "$write_secrets_file" ]]; then
      if [[ "$write_env_file" == *.env ]]; then
        write_secrets_file="${write_env_file%.env}.secrets.env"
      else
        write_secrets_file="${write_env_file}.secrets"
      fi
    fi
    mkdir -p "$(dirname "$write_secrets_file")"
    {
      echo "# Generated by join-fair-validator.sh"
      write_env_line "SOLANACDN_API_TOKEN" "$solanacdn_api_token"
    } > "$write_secrets_file"
    echo "wrote validator env: $write_env_file"
    echo "wrote validator secrets env: $write_secrets_file"
  else
    echo "wrote validator env: $write_env_file"
  fi

  expected_systemd_env="/etc/agave/devnet/${node_name}.env"
  if [[ "$write_env_file" == "$expected_systemd_env" ]]; then
    echo "start with: systemctl enable --now agave-validator@${node_name}"
  else
    echo "systemd expects env file at: $expected_systemd_env"
    echo "either move the file there or run start-validator.sh manually"
  fi
  exit 0
fi

command=("$script_dir/start-validator.sh" --node-name "$node_name")
if is_true "$print_only"; then
  command+=(--print-command)
fi

exec "${command[@]}"
