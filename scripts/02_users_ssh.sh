#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

if ! getent group "${BD_GROUP}" >/dev/null; then
  groupadd "${BD_GROUP}"
fi
if ! getent passwd "${BD_USER}" >/dev/null; then
  useradd -m -g "${BD_GROUP}" -s /bin/bash "${BD_USER}"
fi

short="$(hostname -s)"
long="$(hostname -f 2>/dev/null || echo "${short}")"
primary_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
if [[ -n "${primary_ip}" ]] && ! grep -Fq "${long}" /etc/hosts 2>/dev/null; then
  echo "${primary_ip} ${long} ${short}" >>/etc/hosts
  log "Appended hosts entry: ${primary_ip} ${long} ${short}"
fi

dnf -y install openssh-server openssh-clients nc || true
systemctl enable sshd 2>/dev/null || systemctl enable sshd.service
systemctl start sshd 2>/dev/null || true

if [[ "${CONFIGURE_SSH_LOCALHOST}" != "yes" ]]; then
  log "CONFIGURE_SSH_LOCALHOST!=yes, skip localhost keys"
  exit 0
fi

sudo -u "${BD_USER}" -H mkdir -p "${BD_HOME}/.ssh"
sudo -u "${BD_USER}" -H chmod 700 "${BD_HOME}/.ssh"
if [[ ! -f "${BD_HOME}/.ssh/id_rsa" ]]; then
  sudo -u "${BD_USER}" -H ssh-keygen -t rsa -N "" -f "${BD_HOME}/.ssh/id_rsa"
fi
if ! grep -q "localhost" "${BD_HOME}/.ssh/authorized_keys" 2>/dev/null; then
  cat "${BD_HOME}/.ssh/id_rsa.pub" >>"${BD_HOME}/.ssh/authorized_keys"
fi
sudo -u "${BD_USER}" -H chmod 600 "${BD_HOME}/.ssh/authorized_keys"

sudo -u "${BD_USER}" -H bash -lc "ssh-keyscan -p ${SSH_PORT} -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null || true"
sudo -u "${BD_USER}" -H bash -lc "ssh-keyscan -p ${SSH_PORT} -H localhost >> ~/.ssh/known_hosts 2>/dev/null || true"

# Non-interactive test
sudo -u "${BD_USER}" -H ssh -p "${SSH_PORT}" -o BatchMode=yes -o StrictHostKeyChecking=no \
  "${BD_USER}@127.0.0.1" true || die "Passwordless SSH to localhost failed for ${BD_USER}"

log "User ${BD_USER} and localhost SSH OK."
