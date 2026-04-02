#!/usr/bin/env bash
# Verify ZooKeeper + HDFS + YARN + Spark only (no Hive/HBase/Kafka/Flink)
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

[[ -d "${ZOOKEEPER_HOME}/bin" ]] || die "ZooKeeper not installed (expected ${ZOOKEEPER_HOME})"
[[ -d "${HADOOP_HOME}/sbin" ]] || die "Hadoop not installed (expected ${HADOOP_HOME})"
[[ -d "${SPARK_HOME}/bin" ]] || die "Spark not installed (expected ${SPARK_HOME})"

fail=0
ok() { log "OK  $*"; }
bad() { log "FAIL $*"; fail=1; }

run_bd() { sudo -u "${BD_USER}" -H bash -lc "$*"; }

profile_sh="/etc/profile.d/bigdata-spark-verify.sh"
cat >"${profile_sh}" <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export SPARK_HOME=${SPARK_HOME}
export PATH=\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin:\${SPARK_HOME}/bin:\${PATH}
EOF
chmod 644 "${profile_sh}"

log "Starting ZooKeeper"
run_bd "source ${profile_sh}; ${ZOOKEEPER_HOME}/bin/zkServer.sh start" || bad "ZooKeeper start"
sleep 2
if echo ruok | nc -w 2 127.0.0.1 "${ZK_CLIENT_PORT}" 2>/dev/null | grep -q imok; then
  ok "ZooKeeper imok"
else
  bad "ZooKeeper ruok"
fi

log "Starting HDFS / YARN"
run_bd "source ${profile_sh}; ${HADOOP_HOME}/sbin/start-dfs.sh" || bad "start-dfs"
run_bd "source ${profile_sh}; ${HADOOP_HOME}/sbin/start-yarn.sh" || bad "start-yarn"
sleep 5

if run_bd "source ${profile_sh}; ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp /tmp/spark-verify"; then
  ok "HDFS mkdir"
else
  bad "HDFS mkdir"
fi

if run_bd "source ${profile_sh}; ${HADOOP_HOME}/bin/hdfs dfs -put -f ${HADOOP_HOME}/LICENSE.txt /tmp/LICENSE.spark-verify 2>/dev/null"; then
  ok "HDFS put"
else
  bad "HDFS put"
fi

log "Spark CLI"
if run_bd "source ${profile_sh}; ${SPARK_HOME}/bin/spark-submit --version 2>&1 | head -5"; then
  ok "spark-submit --version"
else
  bad "spark-submit --version"
fi

log "Spark Pi (local mode)"
if run_bd 'source /etc/profile.d/bigdata-spark-verify.sh; j=$(ls ${SPARK_HOME}/examples/jars/spark-examples*.jar 2>/dev/null | head -1); [[ -n "$j" ]] || exit 1; ${SPARK_HOME}/bin/spark-submit --master local[2] --class org.apache.spark.examples.SparkPi "$j" 20 2>&1 | tail -5'; then
  ok "Spark Pi completed (see log tail above)"
else
  bad "Spark Pi"
fi

if [[ "${fail}" -ne 0 ]]; then
  die "Spark verification had failures."
fi
log "Spark stack verification passed."
