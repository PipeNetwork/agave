#!/usr/bin/env bash
#
# Quick DPDK host sanity/status check for a given NIC interface or PCI BDF.
# Read-only: does not bind/unbind devices or change host networking.
#
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/dpdk-status.sh --iface <ifname> [--check]
  scripts/dpdk-status.sh --pci <BDF> [--check]

Examples:
  scripts/dpdk-status.sh --iface eno1
  scripts/dpdk-status.sh --pci 0000:41:00.0
  scripts/dpdk-status.sh --pci 41:00.0

Options:
  --check   Exit non-zero if host/device is not ready for full DPDK (vfio/uio)

Notes:
  - For full DPDK PMD (vfio/uio), the device should be bound to vfio-pci (or a UIO PMD).
  - Binding a production NIC to vfio will detach it from the kernel and can drop SSH/network access.
EOF
}

if [[ $(uname) != Linux ]]; then
  echo "dpdk-status: Linux only"
  exit 0
fi

GREP_BIN="grep"
GREP_ARGS=("-E")
if command -v rg >/dev/null 2>&1; then
  GREP_BIN="rg"
  GREP_ARGS=()
fi

iface=""
pci=""
check=0

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
    --check)
      check=1
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "dpdk-status: unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -n "$iface" && -n "$pci" ]]; then
  echo "dpdk-status: provide exactly one of --iface or --pci" >&2
  exit 2
fi
if [[ -z "$iface" && -z "$pci" ]]; then
  usage >&2
  exit 2
fi

if [[ -n "$iface" ]]; then
  if [[ ! -e "/sys/class/net/$iface/device" ]]; then
    echo "dpdk-status: interface not found or has no PCI device: $iface" >&2
    exit 2
  fi
  pci="$(basename "$(readlink -f "/sys/class/net/$iface/device")")"
fi

if [[ "$pci" != 0000:* ]]; then
  pci="0000:$pci"
fi

sysfs_dev="/sys/bus/pci/devices/$pci"
if [[ ! -d "$sysfs_dev" ]]; then
  echo "dpdk-status: PCI device not found in sysfs: $pci" >&2
  exit 2
fi

short_bdf="${pci#0000:}"
vendor="$(cat "$sysfs_dev/vendor")"
device="$(cat "$sysfs_dev/device")"

driver="(none)"
if [[ -L "$sysfs_dev/driver" ]]; then
  driver="$(basename "$(readlink -f "$sysfs_dev/driver")")"
fi

echo "== Device =="
echo "  pci:    $pci"
echo "  vendor: $vendor"
echo "  device: $device"
echo "  driver: $driver"

if command -v lspci >/dev/null 2>&1; then
  echo
  echo "== lspci =="
  lspci -nn -s "$short_bdf" | sed 's/^/  /'
  lspci -k -s "$short_bdf" | sed 's/^/  /'
fi

echo
echo "== SR-IOV parent PF =="
if [[ -L "$sysfs_dev/physfn" ]]; then
  pf_pci="$(basename "$(readlink -f "$sysfs_dev/physfn")")"
  pf_sysfs="/sys/bus/pci/devices/$pf_pci"
  echo "  pf_pci: $pf_pci"
  if [[ -d "$pf_sysfs/net" ]]; then
    pf_netdevs="$(ls -1 "$pf_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true)"
    if [[ -n "$pf_netdevs" ]]; then
      echo "  pf_netdev: $pf_netdevs"
      if command -v ip >/dev/null 2>&1; then
        for dev in $pf_netdevs; do
          route="$(ip -4 route show default dev "$dev" 2>/dev/null || true)"
          if [[ -n "$route" ]]; then
            echo "  pf_default_route($dev): $route"
          fi
        done
      fi
    fi
  fi
else
  echo "  (not a VF; no physfn link)"
fi

echo
echo "== IOMMU group =="
if [[ -L "$sysfs_dev/iommu_group" ]]; then
  group_path="$(readlink -f "$sysfs_dev/iommu_group")"
  group_id="$(basename "$group_path")"
  echo "  group: $group_id"
  echo "  devices:"
  all_vfio=1
  for dev in "$group_path"/devices/*; do
    dev_bdf="$(basename "$dev")"
    dev_sysfs="/sys/bus/pci/devices/$dev_bdf"
    dev_driver="(none)"
    if [[ -L "$dev_sysfs/driver" ]]; then
      dev_driver="$(basename "$(readlink -f "$dev_sysfs/driver")")"
    fi
    if [[ "$dev_driver" != "vfio-pci" ]]; then
      all_vfio=0
    fi
    dev_netdev="(none)"
    if [[ -d "$dev_sysfs/net" ]]; then
      dev_netdevs="$(ls -1 "$dev_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true)"
      if [[ -n "$dev_netdevs" ]]; then
        dev_netdev="$dev_netdevs"
      fi
    fi
    echo "    - $dev_bdf driver=$dev_driver netdev=$dev_netdev"
  done
  if [[ "$driver" == "vfio-pci" && "$all_vfio" == 0 ]]; then
    echo "  WARNING: vfio-pci is bound, but not all devices in the IOMMU group use vfio-pci"
    echo "  Hint (DISRUPTIVE): sudo scripts/dpdk-setup.sh --pci $pci --bind-pf --bind-iommu-group --apply"
  fi
else
  echo "  (no iommu_group; IOMMU may be disabled)"
fi

echo
echo "== VFIO access =="
if [[ "$driver" == "vfio-pci" ]]; then
  if [[ -n "${group_id:-}" ]]; then
    devnode="/dev/vfio/$group_id"
    if [[ -e "$devnode" ]]; then
      ls -la "$devnode" | sed 's/^/  /'
      if exec 3<>"$devnode" 2>/dev/null; then
        exec 3>&-
        echo "  access: read/write (ok)"
      else
        echo "  WARNING: cannot open $devnode read/write as user $(id -un)."
        echo "           Run as root or grant access (udev rule / group ownership for the VFIO group)."
      fi
    else
      echo "  WARNING: $devnode not found; ensure vfio is loaded and IOMMU is enabled"
    fi
  else
    echo "  (no iommu_group id found)"
  fi
else
  echo "  (driver is not vfio-pci)"
fi

echo
echo "== SR-IOV =="
if [[ -e "$sysfs_dev/sriov_totalvfs" ]]; then
  echo "  totalvfs: $(cat "$sysfs_dev/sriov_totalvfs")"
  echo "  numvfs:   $(cat "$sysfs_dev/sriov_numvfs")"
  if compgen -G "$sysfs_dev/virtfn*" >/dev/null; then
    echo "  VFs:"
    for vf in "$sysfs_dev"/virtfn*; do
      vf_idx="$(basename "$vf")"
      vf_pci="$(basename "$(readlink -f "$vf")")"
      vf_sysfs="/sys/bus/pci/devices/$vf_pci"
      vf_driver="(none)"
      if [[ -L "$vf_sysfs/driver" ]]; then
        vf_driver="$(basename "$(readlink -f "$vf_sysfs/driver")")"
      fi
      vf_netdev="(none)"
      if [[ -d "$vf_sysfs/net" ]]; then
        # Most VF netdevs appear as a single entry under .../net, but handle 0/1/many.
        vf_netdevs="$(ls -1 "$vf_sysfs/net" 2>/dev/null | tr '\n' ' ' | xargs || true)"
        if [[ -n "$vf_netdevs" ]]; then
          vf_netdev="$vf_netdevs"
        fi
      fi
      echo "    - $vf_idx: $vf_pci driver=$vf_driver netdev=$vf_netdev"
    done
    echo "  Notes:"
    echo "    - Typical SR-IOV deployment: keep PF in kernel (management), bind a VF to vfio-pci for DPDK."
    echo "    - VF creation example (DISRUPTIVE): echo 1 | sudo tee $sysfs_dev/sriov_numvfs"
    echo "    - VF removal example (DISRUPTIVE): echo 0 | sudo tee $sysfs_dev/sriov_numvfs"
  fi
else
  echo "  (no sriov_totalvfs; SR-IOV not supported)"
fi

echo
echo "== Hugepages (2M) =="
"$GREP_BIN" "${GREP_ARGS[@]}" "HugePages_Total|HugePages_Free|Hugepagesize" /proc/meminfo | sed 's/^/  /'
if mount | "$GREP_BIN" "${GREP_ARGS[@]}" -q "hugetlbfs on /dev/hugepages"; then
  echo "  mount: /dev/hugepages (hugetlbfs)"
else
  echo "  mount: (no /dev/hugepages hugetlbfs mount found)"
fi

echo
echo "== Memlock =="
echo "  ulimit -l: $(ulimit -l)"
if [[ -r /proc/self/limits ]]; then
  "$GREP_BIN" "${GREP_ARGS[@]}" "Max locked memory" /proc/self/limits | sed 's/^/  /' || true
fi

if [[ -n "$iface" ]]; then
  echo
  echo "== Kernel netdev =="
  ip -br link show "$iface" | sed 's/^/  /' || true
  ip -4 -br addr show "$iface" | sed 's/^/  /' || true
  if ip route show default 2>/dev/null | "$GREP_BIN" "${GREP_ARGS[@]}" -q " dev $iface( |$)"; then
    echo "  WARNING: $iface is the default route; binding it to vfio/uio will likely drop access"
  fi
fi

echo
echo "== Notes =="
if [[ "$driver" != "vfio-pci" ]]; then
  echo "  - Not currently bound to vfio-pci."
  echo "  - To bind (DISRUPTIVE; detaches from kernel), you typically need:"
  echo "      sudo modprobe vfio-pci"
  echo "      echo $vendor $device | sudo tee /sys/bus/pci/drivers/vfio-pci/new_id"
  echo "      echo $pci | sudo tee $sysfs_dev/driver/unbind"
  echo "      echo $pci | sudo tee /sys/bus/pci/drivers/vfio-pci/bind"
  echo "  - Helper (dry-run by default): scripts/dpdk-setup.sh --pci $pci --bind-pf --bind-iommu-group"
  echo "    (Add --apply via sudo to execute.)"
else
  echo "  - Bound to vfio-pci (OK for full DPDK PMD)."
fi

if (( check )); then
  is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }
  is_dpdk_pmd_driver() {
    local d="$1"
    [[ "$d" == "vfio-pci" || "$d" == "uio_pci_generic" || "$d" == "igb_uio" ]]
  }

  failures=()

  if ! is_dpdk_pmd_driver "$driver"; then
    failures+=("device driver is '$driver' (expected vfio-pci or UIO)")
  fi

  if [[ "$driver" == "vfio-pci" ]]; then
    if [[ -z "${group_id:-}" ]]; then
      failures+=("no IOMMU group (IOMMU may be disabled)")
    else
      if [[ "$all_vfio" == 0 ]]; then
        failures+=("IOMMU group $group_id is not fully bound to vfio-pci")
      fi
      devnode="/dev/vfio/$group_id"
      if [[ ! -e "$devnode" ]]; then
        failures+=("$devnode missing (vfio not loaded / IOMMU disabled)")
      else
        if ! exec 3<>"$devnode" 2>/dev/null; then
          failures+=("cannot open $devnode read/write as user $(id -un)")
        else
          exec 3>&-
        fi
      fi
    fi
  fi

  hp_total="$(awk '/HugePages_Total:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
  hp_free="$(awk '/HugePages_Free:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
  if ! is_uint "$hp_total" || (( hp_total == 0 )); then
    failures+=("HugePages_Total is 0 (reserve hugepages)")
  fi
  if is_uint "$hp_free" && (( hp_free == 0 )); then
    failures+=("HugePages_Free is 0 (hugepages exhausted)")
  fi
  if ! mount | "$GREP_BIN" "${GREP_ARGS[@]}" -q "hugetlbfs on /dev/hugepages"; then
    failures+=("/dev/hugepages is not mounted as hugetlbfs")
  fi

  memlock="$(ulimit -l)"
  if [[ "$memlock" != "unlimited" ]] && is_uint "$memlock" && (( memlock < 65536 )); then
    failures+=("ulimit -l is low (${memlock} KB); set LimitMEMLOCK=infinity / ulimit -l unlimited")
  fi

  if (( ${#failures[@]} > 0 )); then
    echo
    echo "DPDK CHECK: FAIL"
    for f in "${failures[@]}"; do
      echo "  - $f"
    done
    exit 1
  fi

  echo
  echo "DPDK CHECK: OK"
fi
