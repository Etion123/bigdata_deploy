#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

[[ "${AUTO_MOUNT_DATA_DISK}" == "yes" ]] || { log "AUTO_MOUNT_DATA_DISK!=yes, skip disk mount"; exit 0; }

pick_disk() {
  if [[ -n "${DATA_DISK_DEVICE}" ]]; then
    echo "${DATA_DISK_DEVICE}"
    return
  fi
  local root_disk
  root_disk="$(findmnt -n -o SOURCE / 2>/dev/null | sed 's/\[.*\]//;s/[0-9]*$//;s/p$//' | xargs basename 2>/dev/null || true)"
  while read -r name type mount; do
    [[ "${type}" == "disk" ]] || continue
    [[ -z "${mount}" ]] || continue
    [[ "${name}" == "${root_disk}" ]] && continue
    echo "/dev/${name}"
    return
  done < <(lsblk -dn -o NAME,TYPE,MOUNTPOINT 2>/dev/null | awk '{print $1,$2,$3}')
  die "No idle disk candidate found. Set DATA_DISK_DEVICE explicitly."
}

dev="$(pick_disk)"
[[ -b "${dev}" ]] || die "Not a block device: ${dev}"
log "Using data disk: ${dev}"

part="${dev}1"
if ! [[ -b "${part}" ]]; then
  log "Partitioning ${dev} (GPT, single partition)"
  wipefs -a "${dev}" 2>/dev/null || true
  parted -s "${dev}" mklabel gpt
  parted -s "${dev}" mkpart primary "${DATA_DISK_FSTYPE}" 0% 100%
  partprobe "${dev}" || true
  sleep 2
fi
[[ -b "${part}" ]] || die "Expected partition ${part} missing"

if ! blkid "${part}" | grep -q TYPE; then
  log "Creating ${DATA_DISK_FSTYPE} on ${part}"
  if [[ "${DATA_DISK_FSTYPE}" == "xfs" ]]; then
    mkfs.xfs -f "${part}"
  else
    mkfs.ext4 -F "${part}"
  fi
fi

mkdir -p "${DATA_MOUNT_POINT}"
if ! mountpoint -q "${DATA_MOUNT_POINT}"; then
  mount "${part}" "${DATA_MOUNT_POINT}"
fi

uuid="$(blkid -s UUID -o value "${part}")"
grep -q "${uuid}" /etc/fstab 2>/dev/null || {
  echo "UUID=${uuid}  ${DATA_MOUNT_POINT}  ${DATA_DISK_FSTYPE}  defaults,noatime  0  0" >>/etc/fstab
}
log "Data disk mounted at ${DATA_MOUNT_POINT}"
