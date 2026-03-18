#!/usr/bin/env bash
set -euo pipefail

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

die() {
  echo "error: $*" >&2
  exit 1
}

is_true() {
  local value="${1:-}"
  value="${value,,}"
  [[ "$value" == "1" || "$value" == "true" || "$value" == "yes" || "$value" == "on" ]]
}

append_flag_if_set() {
  local -n args_ref="$1"
  local flag="$2"
  local value="${3:-}"

  if [[ -n "$value" ]]; then
    args_ref+=("$flag" "$value")
  fi
}

append_bool_flag_if_true() {
  local -n args_ref="$1"
  local flag="$2"
  local value="${3:-}"

  if is_true "$value"; then
    args_ref+=("$flag")
  fi
}

append_repeated_flags() {
  local -n args_ref="$1"
  local flag="$2"
  local values="${3:-}"
  local normalized item
  local -a items=()

  normalized="${values//,/ }"
  read -r -a items <<< "$normalized"
  for item in "${items[@]}"; do
    [[ -n "$item" ]] || continue
    args_ref+=("$flag" "$item")
  done
}

source_env_file_if_present() {
  local env_file="$1"

  if [[ -n "$env_file" && -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    source "$env_file"
  fi
}

print_command() {
  local -a command=("$@")
  local quoted=()
  local item

  for item in "${command[@]}"; do
    quoted+=("$(printf '%q' "$item")")
  done

  printf '%s\n' "${quoted[*]}"
}
