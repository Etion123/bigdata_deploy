#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

ver="${FLINK_VERSION}"
scala="${FLINK_SCALA_VERSION}"
name="flink-${ver}-bin-scala_${scala}"
archive="${DOWNLOAD_DIR}/${name}.tgz"
url="$(apache_url "flink/flink-${ver}/${name}.tgz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/flink"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/flink"

conf="${FLINK_HOME}/conf/flink-conf.yaml"
if ! grep -q "bigdata_deploy" "${conf}"; then
  tmp="$(mktemp)"
  render_template "${ROOT_DIR}/templates/flink/flink-conf.yaml.snippet" "${tmp}"
  echo "" >>"${conf}"
  echo "# --- bigdata_deploy ---" >>"${conf}"
  cat "${tmp}" >>"${conf}"
  rm -f "${tmp}"
fi

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/flink"
log "Flink ${ver} installed."
