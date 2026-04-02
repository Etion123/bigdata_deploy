#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

ver="${KAFKA_VERSION}"
scala="${KAFKA_SCALA_VERSION}"
name="kafka_${scala}-${ver}"
archive="${DOWNLOAD_DIR}/${name}.tgz"
url="$(apache_url "kafka/${ver}/${name}.tgz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/kafka"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/kafka"

ensure_dir "${INSTALL_BASE}/kafka-logs"
render_template "${ROOT_DIR}/templates/kafka/server.properties.template" "${KAFKA_HOME}/config/server.properties"

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/kafka" "${INSTALL_BASE}/kafka-logs"
log "Kafka ${ver} (${name}) installed."
