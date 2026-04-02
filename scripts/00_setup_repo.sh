#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

if [[ -n "${PKG_PROXY}" ]]; then
  export http_proxy="${PKG_PROXY}"
  export https_proxy="${PKG_PROXY}"
  log "Using proxy for dnf: ${PKG_PROXY}"
fi

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

dnf -y install wget tar gzip which nc openssh-clients util-linux parted || true
dnf -y makecache || die "dnf makecache failed (check repo / network)"
log "Repository setup done."
