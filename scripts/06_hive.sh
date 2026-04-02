#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

[[ "${HIVE_DB_TYPE}" == "derby" ]] || die "Only derby metastore is automated in this script; set HIVE_DB_TYPE=derby"

ver="${HIVE_VERSION}"
name="apache-hive-${ver}-bin"
archive="${DOWNLOAD_DIR}/${name}.tar.gz"
url="$(apache_url "hive/hive-${ver}/${name}.tar.gz")"
download_file "${url}" "${archive}"

rm -rf "${INSTALL_BASE}/hive"
extract_tgz "${archive}" "${INSTALL_BASE}"
mv "${INSTALL_BASE}/${name}" "${INSTALL_BASE}/hive"

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

# Reduce Guava conflict between Hive and Hadoop 3.x
hive_lib="${HIVE_HOME}/lib"
hadoop_guava="$(ls "${HADOOP_HOME}/share/hadoop/hdfs/lib"/guava-*.jar 2>/dev/null | head -1 || true)"
if [[ -n "${hadoop_guava}" ]]; then
  rm -f "${hive_lib}"/guava-*.jar
  cp -f "${hadoop_guava}" "${hive_lib}/"
fi

ensure_dir "${INSTALL_BASE}/hive-data/metastore_db"
render_template "${ROOT_DIR}/templates/hive/hive-site.xml.template" "${HIVE_CONF_DIR}/hive-site.xml"

cat >>"${HIVE_HOME}/conf/hive-env.sh" <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HIVE_CONF_DIR=${HIVE_CONF_DIR}
export HIVE_HOME=${HIVE_HOME}
EOF

chown -R "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}/hive" "${INSTALL_BASE}/hive-data"

marker="${INSTALL_BASE}/hive-data/.schema_inited"
if [[ ! -f "${marker}" ]]; then
  run_as_bd "source ${HIVE_HOME}/conf/hive-env.sh; ${HIVE_HOME}/bin/schematool -dbType derby -initSchema"
  touch "${marker}"
  chown "${BD_USER}:${BD_GROUP}" "${marker}"
fi

log "Hive ${ver} installed."
