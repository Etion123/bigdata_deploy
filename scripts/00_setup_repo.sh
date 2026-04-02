#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

bd_write_dnf_proxy_conf

if [[ -n "${LOCAL_REPO_FILE}" ]]; then
  [[ -f "${LOCAL_REPO_FILE}" ]] || die "LOCAL_REPO_FILE not found: ${LOCAL_REPO_FILE}"
  if [[ "${LOCAL_REPO_ENABLED}" == "yes" ]]; then
    bak="/etc/yum.repos.d/.bigdata_deploy_backup_$(date +%s)"
    mkdir -p "${bak}"
    log "Backing up existing repo files to ${bak}"
    shopt -s nullglob
    for f in /etc/yum.repos.d/*.repo; do
      mv "${f}" "${bak}/"
    done
    shopt -u nullglob
  fi
  cp -f "${LOCAL_REPO_FILE}" /etc/yum.repos.d/bigdata-local.repo
  chmod 644 /etc/yum.repos.d/bigdata-local.repo
  log "Installed repo: /etc/yum.repos.d/bigdata-local.repo"
fi

if ! dnf -y install wget tar gzip which nc openssh-clients util-linux parted curl ca-certificates 2>/dev/null; then
  die "dnf install base tools failed. Fix: network, HTTP_PROXY/PKG_PROXY in deploy.conf, or valid LOCAL_REPO_FILE + LOCAL_REPO_ENABLED."
fi

if dnf -y makecache; then
  log "Repository setup done."
else
  die "dnf makecache failed. Isolated host: configure LOCAL_REPO_FILE; corporate net: set HTTP_PROXY; firewall: allow dnf mirrors."
fi
