#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

ver="${HADOOP_VERSION}"
name="hadoop-${ver}"
archive="${DOWNLOAD_DIR}/${name}.tar.gz"
url="$(apache_url "hadoop/common/hadoop-${ver}/${name}.tar.gz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/hadoop"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/hadoop"

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

for d in hadoop-data/tmp hadoop-data/namenode hadoop-data/datanode; do
  ensure_dir "${INSTALL_BASE}/${d}"
done

tpl="${ROOT_DIR}/templates/hadoop"
render_template "${tpl}/core-site.xml.template" "${HADOOP_CONF_DIR}/core-site.xml"
render_template "${tpl}/hdfs-site.xml.template" "${HADOOP_CONF_DIR}/hdfs-site.xml"
render_template "${tpl}/mapred-site.xml.template" "${HADOOP_CONF_DIR}/mapred-site.xml"
render_template "${tpl}/yarn-site.xml.template" "${HADOOP_CONF_DIR}/yarn-site.xml"
render_template "${tpl}/workers.template" "${HADOOP_CONF_DIR}/workers"

cat >>"${HADOOP_CONF_DIR}/hadoop-env.sh" <<EOF

# bigdata_deploy
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
EOF

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/hadoop" "${INSTALL_BASE}/hadoop-data"

marker="${INSTALL_BASE}/hadoop-data/.formatted"
if [[ ! -f "${marker}" ]]; then
  log "Formatting HDFS namenode (first run)"
  run_as_bd "source /etc/profile.d/bigdata-java.sh 2>/dev/null; export JAVA_HOME=\${JAVA_HOME:-${JAVA_HOME}}; ${HADOOP_HOME}/bin/hdfs namenode -format -force"
  touch "${marker}"
  chown "${BD_USER}:${BD_GROUP}" "${marker}"
fi

log "Hadoop ${ver} installed. Start with: sudo -u ${BD_USER} ${HADOOP_HOME}/sbin/start-dfs.sh && start-yarn.sh"
