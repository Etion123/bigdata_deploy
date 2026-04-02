#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root
prepare_install_base

ver="${ZOOKEEPER_VERSION}"
name="apache-zookeeper-${ver}-bin"
archive="${DOWNLOAD_DIR}/${name}.tar.gz"
url="$(apache_url "zookeeper/zookeeper-${ver}/${name}.tar.gz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/zookeeper"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/zookeeper"

ensure_dir "${INSTALL_BASE}/zookeeper-data"
render_template "${ROOT_DIR}/templates/zookeeper/zoo.cfg.template" "${INSTALL_BASE}/zookeeper/conf/zoo.cfg"

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/zookeeper" "${INSTALL_BASE}/zookeeper-data"
log "ZooKeeper ${ver} installed under ${INSTALL_BASE}/zookeeper"
