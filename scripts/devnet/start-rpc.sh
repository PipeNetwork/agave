#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export AGAVE_ROLE="${AGAVE_ROLE:-rpc}"
export NO_VOTING="${NO_VOTING:-true}"
export FULL_RPC_API="${FULL_RPC_API:-true}"
export PRIVATE_RPC="${PRIVATE_RPC:-false}"
export FAIR_ENABLE="${FAIR_ENABLE:-false}"
export SOLANACDN_ENABLE="${SOLANACDN_ENABLE:-false}"

exec "$script_dir/start-validator.sh" "$@"
