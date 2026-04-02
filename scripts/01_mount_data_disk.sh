#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

[[ "${AUTO_MOUNT_DATA_DISK}" == "yes" ]] || { log "AUTO_MOUNT_DATA_DISK!=yes, skip disk mount"; exit 0; }

command -v parted >/dev/null 2>&1 || die "parted not installed (run phase repo first)"
command -v wipefs >/dev/null 2>&1 || die "wipefs not installed (run phase repo first)"

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
  die "No idle disk candidate found. Set DATA_DISK_DEVICE explicitly or disable AUTO_MOUNT_DATA_DISK."
}

dev="$(pick_disk)"
[[ -b "${dev}" ]] || die "Not a block device: ${dev}"
log "Using data disk: ${dev}"

part="${dev}1"
if ! [[ -b "${part}" ]]; then
  log "Partitioning ${dev} (GPT, single partition) — DATA WILL BE WIPED"
  wipefs -a "${dev}" 2>/dev/null || true
  parted -s "${dev}" mklabel gpt || die "parted mklabel failed on ${dev}"
  parted -s "${dev}" mkpart primary "${DATA_DISK_FSTYPE}" 0% 100% || die "parted mkpart failed on ${dev}"
  partprobe "${dev}" || true
  sleep 2
fi
[[ -b "${part}" ]] || die "Expected partition ${part} missing (partprobe or kernel refresh?)"

if ! blkid "${part}" | grep -q TYPE; then
  log "Creating ${DATA_DISK_FSTYPE} on ${part}"
  if [[ "${DATA_DISK_FSTYPE}" == "xfs" ]]; then
    mkfs.xfs -f "${part}" || die "mkfs.xfs failed"
  else
    mkfs.ext4 -F "${part}" || die "mkfs.ext4 failed"
  fi
fi

mkdir -p "${DATA_MOUNT_POINT}" || die "Cannot mkdir ${DATA_MOUNT_POINT}"
if ! mountpoint -q "${DATA_MOUNT_POINT}"; then
  mount "${part}" "${DATA_MOUNT_POINT}" || die "mount failed for ${part} -> ${DATA_MOUNT_POINT}"
fi

uuid="$(blkid -s UUID -o value "${part}")"
[[ -n "${uuid}" ]] || die "Could not read UUID for ${part}"
if ! grep -Fq "UUID=${uuid}" /etc/fstab 2>/dev/null; then
  echo "UUID=${uuid}  ${DATA_MOUNT_POINT}  ${DATA_DISK_FSTYPE}  defaults,noatime  0  0" >>/etc/fstab
fi
log "Data disk mounted at ${DATA_MOUNT_POINT}"
