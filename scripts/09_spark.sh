#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

ver="${SPARK_VERSION}"
prof="${SPARK_HADOOP_PROFILE}"
name="spark-${ver}-bin-${prof}"
archive="${DOWNLOAD_DIR}/${name}.tgz"
url="$(apache_url "spark/spark-${ver}/${name}.tgz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/spark"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/spark"

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

cat >"${SPARK_HOME}/conf/spark-env.sh" <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export SPARK_DIST_CLASSPATH=\$(${HADOOP_HOME}/bin/hadoop classpath)
EOF
chmod 755 "${SPARK_HOME}/conf/spark-env.sh"

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/spark"
log "Spark ${ver} (${prof}) installed."
