#!/usr/bin/env bash
#
# DPDK host setup helper (SR-IOV + vfio binding + hugepages).
#
# This script can be disruptive when applying changes (VF creation, driver bind/unbind).
# It defaults to printing the actions it would take unless --apply is provided.
#
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/dpdk-setup.sh (--iface <pf_ifname> | --pci <BDF>) [actions...] [--apply]
  scripts/dpdk-setup.sh (--iface <pf_ifname> | --pci <BDF>) [actions...] [--apply] --force-default-route

Examples (safe: prints actions, does not change host):
  scripts/dpdk-setup.sh --iface eno1 --show-vfs
  scripts/dpdk-setup.sh --pci 0000:41:00.0 --bind-pf

Examples (DISRUPTIVE: actually applies changes):
  sudo scripts/dpdk-setup.sh --iface eno1 --sriov-numvfs 1 --show-vfs --apply
  sudo scripts/dpdk-setup.sh --iface eno1 --vf-index 0 --bind-vf --bind-iommu-group --apply
  sudo scripts/dpdk-setup.sh --pci 0000:41:00.1 --bind-pf --bind-iommu-group --apply --force-default-route

Actions:
  --show-vfs                 Print existing SR-IOV VFs (if supported).
  --sriov-numvfs <N>         Set sriov_numvfs on the PF (create/remove VFs).
  --vf-index <N>             VF index (default: 0) for VF actions.
  --bind-vf                  Bind VF at --vf-index to vfio-pci.
  --bind-pf                  Bind the PF device itself to vfio-pci.
  --bind-iommu-group         When binding, also bind all devices in the same IOMMU group to vfio-pci.
  --vf-mac <MAC>             Optional: set VF MAC (requires --iface + SR-IOV).
  --vf-trust <on|off>        Optional: set VF trust (requires --iface + SR-IOV).
  --vf-spoofchk <on|off>     Optional: set VF spoofchk (requires --iface + SR-IOV).
  --hugepages2m <N>          Set 2MiB hugepage count.
  --mount-hugepages          Ensure /dev/hugepages is mounted (hugetlbfs, 2MiB pages).

Safety:
  - By default, this script prints commands (dry-run). Add --apply to execute.
  - Binding a NIC detaches it from the kernel and can drop network/SSH access.
  - Prefer SR-IOV: keep PF in kernel for management, bind only a VF to vfio-pci.
  - If the PF is the default route, binding it requires --force-default-route.
  - For vfio, DPDK generally requires all devices in the same IOMMU group to be bound to vfio-pci too.
EOF
}

if [[ $(uname) != Linux ]]; then
  echo "dpdk-setup: Linux only"
  exit 0
fi

apply=0
force_default_route=0
iface=""
pci=""
show_vfs=0
sriov_numvfs=""
vf_index=0
bind_vf=0
bind_pf=0
bind_iommu_group=0
vf_mac=""
vf_trust=""
vf_spoofchk=""
hugepages2m=""
mount_hugepages=0

is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }

normalize_bdf() {
  local bdf="${1:-}"
  bdf="${bdf//[[:space:]]/}"
  if [[ -z "$bdf" ]]; then
    return 1
  fi
  if [[ "$bdf" != 0000:* ]]; then
    echo "0000:$bdf"
  else
    echo "$bdf"
  fi
}

run() {
  if (( apply )); then
    "$@"
  else
    printf '+ %q' "$1"
    shift || true
    for a in "$@"; do
      printf ' %q' "$a"
    done
    printf '\n'
  fi
}

write_sysfs() {
  local value="$1"
  local path="$2"
  if (( apply )); then
    echo "$value" >"$path"
  else
    printf '+ echo %q > %q\n' "$value" "$path"
  fi
}

maybe_readlink_basename() {
  local path="$1"
  if [[ -L "$path" ]]; then
    basename "$(readlink -f "$path")"
  else
    echo "(none)"
  fi
}

netdevs_for_bdf() {
  local bdf="$1"
  local dev_sysfs="/sys/bus/pci/devices/$bdf"
  if [[ -d "$dev_sysfs/net" ]]; then
    ls -1 "$dev_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true
  fi
}

bdf_default_route_dev() {
  local bdf="$1"
  if ! command -v ip >/dev/null 2>&1; then
    return 1
  fi
  local dev
  for dev in $(netdevs_for_bdf "$bdf"); do
    if ip -4 route show default 2>/dev/null | grep -Eq " dev ${dev}( |$)"; then
      echo "$dev"
      return 0
    fi
    if ip -6 route show default 2>/dev/null | grep -Eq " dev ${dev}( |$)"; then
      echo "$dev"
      return 0
    fi
  done
  return 1
}

iommu_group_path_for_bdf() {
  local bdf="$1"
  local dev="/sys/bus/pci/devices/$bdf"
  if [[ -L "$dev/iommu_group" ]]; then
    readlink -f "$dev/iommu_group"
  else
    echo ""
  fi
}

iommu_group_devices_for_bdf() {
  local bdf="$1"
  local group_path
  group_path="$(iommu_group_path_for_bdf "$bdf")"
  if [[ -z "$group_path" ]]; then
    return 1
  fi
  ls -1 "$group_path/devices" 2>/dev/null | sort
}

print_iommu_group_summary_for_bdf() {
  local bdf="$1"
  local group_path group_id
  group_path="$(iommu_group_path_for_bdf "$bdf")"
  if [[ -z "$group_path" ]]; then
    echo "  iommu_group: (none)"
    return 0
  fi
  group_id="$(basename "$group_path")"
  echo "  iommu_group: $group_id"
  echo "  iommu_group_devices:"
  local dev_bdf dev_sysfs dev_driver dev_netdevs
  while read -r dev_bdf; do
    dev_sysfs="/sys/bus/pci/devices/$dev_bdf"
    dev_driver="$(maybe_readlink_basename "$dev_sysfs/driver")"
    dev_netdevs="(none)"
    if [[ -d "$dev_sysfs/net" ]]; then
      dev_netdevs="$(ls -1 "$dev_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true)"
      [[ -n "$dev_netdevs" ]] || dev_netdevs="(none)"
    fi
    echo "    - $dev_bdf driver=$dev_driver netdev=$dev_netdevs"
  done < <(iommu_group_devices_for_bdf "$bdf" || true)
}

warn_iommu_group_incomplete_vfio() {
  local bdf="$1"
  local group_path group_id
  group_path="$(iommu_group_path_for_bdf "$bdf")"
  if [[ -z "$group_path" ]]; then
    return 0
  fi
  group_id="$(basename "$group_path")"
  local dev_bdf dev_sysfs dev_driver dev_netdevs bad=0
  local -a non_vfio_lines=()
  while read -r dev_bdf; do
    dev_sysfs="/sys/bus/pci/devices/$dev_bdf"
    dev_driver="$(maybe_readlink_basename "$dev_sysfs/driver")"
    if [[ "$dev_driver" != "vfio-pci" ]]; then
      bad=1
      dev_netdevs="$(netdevs_for_bdf "$dev_bdf")"
      [[ -n "$dev_netdevs" ]] || dev_netdevs="(none)"
      non_vfio_lines+=("    - $dev_bdf driver=$dev_driver netdev=$dev_netdevs")
    fi
  done < <(iommu_group_devices_for_bdf "$bdf" || true)
  if (( bad )); then
    echo "WARNING: IOMMU group $group_id is not fully bound to vfio-pci."
    echo "         Devices not bound to vfio-pci:"
    printf '%s\n' "${non_vfio_lines[@]}"
    echo "         For vfio, DPDK generally requires binding the entire IOMMU group."
    echo "         Re-run with --bind-iommu-group (DISRUPTIVE)."
  fi
}

refuse_iommu_group_default_route_without_force() {
  local bdf="$1"
  if (( force_default_route )); then
    return 0
  fi
  local group_path
  group_path="$(iommu_group_path_for_bdf "$bdf")"
  if [[ -z "$group_path" ]]; then
    return 0
  fi
  local dev_bdf route_dev
  while read -r dev_bdf; do
    if route_dev="$(bdf_default_route_dev "$dev_bdf")"; then
      echo "dpdk-setup: refusing to bind IOMMU group; ${route_dev} is the default route." >&2
      echo "dpdk-setup: re-run with --force-default-route if you have out-of-band access." >&2
      exit 2
    fi
  done < <(iommu_group_devices_for_bdf "$bdf" || true)
}

is_mounted() {
  local path="$1"
  if command -v mountpoint >/dev/null 2>&1; then
    mountpoint -q "$path" 2>/dev/null
    return $?
  fi
  mount | grep -Eq " on ${path}( |$)"
}

list_vfs() {
  local pf_sysfs="$1"
  if [[ ! -e "$pf_sysfs/sriov_totalvfs" ]]; then
    echo "SR-IOV: not supported (no sriov_totalvfs)"
    return 0
  fi
  echo "SR-IOV:"
  echo "  totalvfs: $(cat "$pf_sysfs/sriov_totalvfs")"
  echo "  numvfs:   $(cat "$pf_sysfs/sriov_numvfs")"
  if ! compgen -G "$pf_sysfs/virtfn*" >/dev/null; then
    return 0
  fi
  echo "  VFs:"
  for vf in "$pf_sysfs"/virtfn*; do
    local vf_idx vf_pci vf_sysfs vf_driver vf_netdevs
    vf_idx="$(basename "$vf")"
    vf_pci="$(basename "$(readlink -f "$vf")")"
    vf_sysfs="/sys/bus/pci/devices/$vf_pci"
    vf_driver="$(maybe_readlink_basename "$vf_sysfs/driver")"
    vf_netdevs="(none)"
    if [[ -d "$vf_sysfs/net" ]]; then
      vf_netdevs="$(ls -1 "$vf_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true)"
      [[ -n "$vf_netdevs" ]] || vf_netdevs="(none)"
    fi
    echo "    - $vf_idx: $vf_pci driver=$vf_driver netdev=$vf_netdevs"
  done
}

bind_to_vfio() {
  local target_bdf="$1"
  local dev="/sys/bus/pci/devices/$target_bdf"
  if [[ ! -d "$dev" ]]; then
    echo "dpdk-setup: PCI device not found: $target_bdf" >&2
    exit 2
  fi

  local vendor device cur_driver
  vendor="$(cat "$dev/vendor")"
  device="$(cat "$dev/device")"
  cur_driver="$(maybe_readlink_basename "$dev/driver")"

  echo "Bind to vfio-pci:"
  echo "  pci:    $target_bdf"
  echo "  vendor: $vendor"
  echo "  device: $device"
  echo "  driver: $cur_driver"

  if [[ "$cur_driver" == "vfio-pci" ]]; then
    echo "  (already bound to vfio-pci)"
    return 0
  fi

  run modprobe vfio-pci

  # Unbind from current driver, if any.
  if [[ -L "$dev/driver" ]]; then
    write_sysfs "$target_bdf" "$dev/driver/unbind"
  fi

  # Prefer driver_override + drivers_probe to avoid persisting global new_id state.
  if [[ -w "$dev/driver_override" && -w "/sys/bus/pci/drivers_probe" ]]; then
    write_sysfs "vfio-pci" "$dev/driver_override"
    write_sysfs "$target_bdf" "/sys/bus/pci/drivers_probe"
    # Best-effort clear.
    if [[ -w "$dev/driver_override" ]]; then
      write_sysfs "" "$dev/driver_override"
    fi
    return 0
  fi

  # Fallback to new_id + bind.
  if [[ -w "/sys/bus/pci/drivers/vfio-pci/new_id" ]]; then
    write_sysfs "$vendor $device" "/sys/bus/pci/drivers/vfio-pci/new_id"
  fi
  write_sysfs "$target_bdf" "/sys/bus/pci/drivers/vfio-pci/bind"
}

bind_to_vfio_maybe_iommu_group() {
  local target_bdf="$1"
  if (( bind_iommu_group )); then
    local group_path group_id
    group_path="$(iommu_group_path_for_bdf "$target_bdf")"
    if [[ -z "$group_path" ]]; then
      echo "WARNING: no iommu_group for $target_bdf; binding only the target device"
      bind_to_vfio "$target_bdf"
      return 0
    fi
    group_id="$(basename "$group_path")"
    echo "Bind IOMMU group $group_id to vfio-pci:"
    print_iommu_group_summary_for_bdf "$target_bdf"
    if (( apply )); then
      refuse_iommu_group_default_route_without_force "$target_bdf"
    elif (( force_default_route == 0 )); then
      local group_dev_bdf route_dev
      while read -r group_dev_bdf; do
        if route_dev="$(bdf_default_route_dev "$group_dev_bdf")"; then
          echo "WARNING: IOMMU group $group_id contains default-route dev ${route_dev}; --apply will require --force-default-route"
          break
        fi
      done < <(iommu_group_devices_for_bdf "$target_bdf" || true)
    fi
    local dev_bdf
    while read -r dev_bdf; do
      bind_to_vfio "$dev_bdf"
    done < <(iommu_group_devices_for_bdf "$target_bdf")
    return 0
  fi

  bind_to_vfio "$target_bdf"
  warn_iommu_group_incomplete_vfio "$target_bdf"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iface)
      iface="${2:-}"
      shift 2
      ;;
    --pci)
      pci="${2:-}"
      shift 2
      ;;
    --show-vfs)
      show_vfs=1
      shift 1
      ;;
    --sriov-numvfs)
      sriov_numvfs="${2:-}"
      shift 2
      ;;
    --vf-index)
      vf_index="${2:-}"
      shift 2
      ;;
    --bind-vf)
      bind_vf=1
      shift 1
      ;;
    --bind-pf)
      bind_pf=1
      shift 1
      ;;
    --bind-iommu-group)
      bind_iommu_group=1
      shift 1
      ;;
    --vf-mac)
      vf_mac="${2:-}"
      shift 2
      ;;
    --vf-trust)
      vf_trust="${2:-}"
      shift 2
      ;;
    --vf-spoofchk)
      vf_spoofchk="${2:-}"
      shift 2
      ;;
    --hugepages2m)
      hugepages2m="${2:-}"
      shift 2
      ;;
    --mount-hugepages)
      mount_hugepages=1
      shift 1
      ;;
    --apply)
      apply=1
      shift 1
      ;;
    --force-default-route)
      force_default_route=1
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "dpdk-setup: unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -n "$iface" && -n "$pci" ]]; then
  echo "dpdk-setup: provide exactly one of --iface or --pci" >&2
  exit 2
fi
if [[ -z "$iface" && -z "$pci" ]]; then
  usage >&2
  exit 2
fi

if (( apply )) && [[ $EUID -ne 0 ]]; then
  echo "dpdk-setup: --apply requires root (run via sudo)" >&2
  exit 2
fi

pf_bdf=""
pf_ifname="$iface"
if [[ -n "$iface" ]]; then
  if [[ ! -e "/sys/class/net/$iface/device" ]]; then
    echo "dpdk-setup: interface not found or has no PCI device: $iface" >&2
    exit 2
  fi
  pf_bdf="$(basename "$(readlink -f "/sys/class/net/$iface/device")")"
else
  pf_bdf="$(normalize_bdf "$pci")"
fi

pf_sysfs="/sys/bus/pci/devices/$pf_bdf"
if [[ ! -d "$pf_sysfs" ]]; then
  echo "dpdk-setup: PCI device not found in sysfs: $pf_bdf" >&2
  exit 2
fi

echo "== PF device =="
echo "  pci:    $pf_bdf"
echo "  vendor: $(cat "$pf_sysfs/vendor")"
echo "  device: $(cat "$pf_sysfs/device")"
echo "  driver: $(maybe_readlink_basename "$pf_sysfs/driver")"
print_iommu_group_summary_for_bdf "$pf_bdf"

is_default_route=0
pf_default_route_dev=""
if pf_default_route_dev="$(bdf_default_route_dev "$pf_bdf")"; then
  is_default_route=1
  echo "WARNING: ${pf_default_route_dev} is the default route; binding a PF/IOMMU group can drop access"
fi

if (( show_vfs )); then
  echo
  list_vfs "$pf_sysfs"
fi

if [[ -n "$sriov_numvfs" ]]; then
  if ! is_uint "$sriov_numvfs"; then
    echo "dpdk-setup: --sriov-numvfs must be a non-negative integer" >&2
    exit 2
  fi
  if [[ ! -e "$pf_sysfs/sriov_totalvfs" ]]; then
    echo "dpdk-setup: SR-IOV not supported on $pf_bdf (no sriov_totalvfs)" >&2
    exit 2
  fi
  total="$(cat "$pf_sysfs/sriov_totalvfs")"
  if is_uint "$total" && (( sriov_numvfs > total )); then
    echo "dpdk-setup: requested numvfs=$sriov_numvfs exceeds totalvfs=$total" >&2
    exit 2
  fi
  echo
  echo "== SR-IOV configure =="
  write_sysfs "$sriov_numvfs" "$pf_sysfs/sriov_numvfs"
  list_vfs "$pf_sysfs"
fi

if [[ -n "$hugepages2m" ]]; then
  if ! is_uint "$hugepages2m"; then
    echo "dpdk-setup: --hugepages2m must be a non-negative integer" >&2
    exit 2
  fi
  huge_sysfs="/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
  if [[ ! -e "$huge_sysfs" ]]; then
    echo "dpdk-setup: hugepage sysfs not found: $huge_sysfs" >&2
    exit 2
  fi
  echo
  echo "== Hugepages (2M) =="
  write_sysfs "$hugepages2m" "$huge_sysfs"
fi

if (( mount_hugepages )); then
  echo
  echo "== Mount /dev/hugepages =="
  if is_mounted /dev/hugepages; then
    echo "  (already mounted)"
  else
    run mkdir -p /dev/hugepages
    run mount -t hugetlbfs -o pagesize=2M none /dev/hugepages
  fi
fi

if [[ -n "$vf_mac" || -n "$vf_trust" || -n "$vf_spoofchk" ]]; then
  if [[ -z "$pf_ifname" ]]; then
    echo "dpdk-setup: VF netlink settings require --iface <pf_ifname>" >&2
    exit 2
  fi
  if ! is_uint "$vf_index"; then
    echo "dpdk-setup: --vf-index must be a non-negative integer" >&2
    exit 2
  fi
  echo
  echo "== VF netlink settings =="
  if [[ -n "$vf_mac" ]]; then
    run ip link set "$pf_ifname" vf "$vf_index" mac "$vf_mac"
  fi
  if [[ -n "$vf_trust" ]]; then
    if [[ "$vf_trust" != "on" && "$vf_trust" != "off" ]]; then
      echo "dpdk-setup: --vf-trust must be 'on' or 'off'" >&2
      exit 2
    fi
    run ip link set "$pf_ifname" vf "$vf_index" trust "$vf_trust"
  fi
  if [[ -n "$vf_spoofchk" ]]; then
    if [[ "$vf_spoofchk" != "on" && "$vf_spoofchk" != "off" ]]; then
      echo "dpdk-setup: --vf-spoofchk must be 'on' or 'off'" >&2
      exit 2
    fi
    run ip link set "$pf_ifname" vf "$vf_index" spoofchk "$vf_spoofchk"
  fi
fi

if (( bind_pf )) && (( bind_vf )); then
  echo "dpdk-setup: choose exactly one of --bind-pf or --bind-vf" >&2
  exit 2
fi

if (( bind_vf )); then
  if [[ ! -e "$pf_sysfs/sriov_totalvfs" ]]; then
    echo "dpdk-setup: cannot --bind-vf; SR-IOV not supported on $pf_bdf" >&2
    exit 2
  fi
  if ! is_uint "$vf_index"; then
    echo "dpdk-setup: --vf-index must be a non-negative integer" >&2
    exit 2
  fi
  vf_link="$pf_sysfs/virtfn$vf_index"
  if [[ ! -e "$vf_link" ]]; then
    echo "dpdk-setup: VF not found: virtfn$vf_index (create VFs via --sriov-numvfs first)" >&2
    exit 2
  fi
  vf_bdf="$(basename "$(readlink -f "$vf_link")")"
  echo
  echo "== Bind VF to vfio-pci =="
  bind_to_vfio_maybe_iommu_group "$vf_bdf"
  echo
  echo "Next:"
  echo "  - Verify: scripts/dpdk-status.sh --pci $vf_bdf"
  echo "  - Probe:  cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- --devargs $vf_bdf"
fi

if (( bind_pf )); then
  if (( apply )) && (( is_default_route )) && (( force_default_route == 0 )); then
    echo "dpdk-setup: refusing to bind default-route PF (${pf_default_route_dev:-$pf_bdf}) with --apply." >&2
    echo "dpdk-setup: re-run with --force-default-route if you have out-of-band access." >&2
    exit 2
  fi
  echo
  echo "== Bind PF to vfio-pci =="
  bind_to_vfio_maybe_iommu_group "$pf_bdf"
  echo
  echo "Next:"
  echo "  - Verify: scripts/dpdk-status.sh --pci $pf_bdf"
  echo "  - Probe:  cargo run -p agave-dpdk --features dpdk --bin agave-dpdk-probe -- --devargs $pf_bdf"
fi
