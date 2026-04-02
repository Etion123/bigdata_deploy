#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

if [[ "${JAVA_USE_SYSTEM}" == "yes" ]]; then
  dnf -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel || die "OpenJDK 8 install failed"
  java_home="$(dirname "$(dirname "$(readlink -f /etc/alternatives/java)")")"
  echo "export JAVA_HOME=${java_home}" >/etc/profile.d/bigdata-java.sh
  chmod 644 /etc/profile.d/bigdata-java.sh
  export JAVA_HOME="${java_home}"
  java -version
  log "System JDK 8 at ${JAVA_HOME}"
  exit 0
fi

[[ -n "${JAVA_TARBALL_URL}" ]] || die "JAVA_USE_SYSTEM=no requires JAVA_TARBALL_URL"
ensure_dir "${INSTALL_BASE}/jdk"
archive="${DOWNLOAD_DIR}/$(basename "${JAVA_TARBALL_URL}")"
download_file "${JAVA_TARBALL_URL}" "${archive}"
extract_tgz "${archive}" "${INSTALL_BASE}/jdk"
jh="$(find "${INSTALL_BASE}/jdk" -maxdepth 3 -type f -name java -path '*/bin/java' 2>/dev/null | head -1 | xargs dirname 2>/dev/null | xargs dirname 2>/dev/null || true)"
[[ -n "${jh}" ]] || jh="${INSTALL_BASE}/jdk"
echo "export JAVA_HOME=${jh}" >/etc/profile.d/bigdata-java.sh
export JAVA_HOME="${jh}"
log "Tarball JDK at ${JAVA_HOME}"
