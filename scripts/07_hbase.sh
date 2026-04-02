#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

ver="${HBASE_VERSION}"
name="hbase-${ver}"
archive="${DOWNLOAD_DIR}/${name}-bin.tar.gz"
url="$(apache_url "hbase/${ver}/${name}-bin.tar.gz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/hbase"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/hbase"

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

render_template "${ROOT_DIR}/templates/hbase/hbase-site.xml.template" "${HBASE_HOME}/conf/hbase-site.xml"

cat >>"${HBASE_HOME}/conf/hbase-env.sh" <<EOF
export JAVA_HOME=${JAVA_HOME}
export HBASE_MANAGES_ZK=false
EOF

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/hbase"
log "HBase ${ver} installed (uses external ZooKeeper on port ${ZK_CLIENT_PORT})."
