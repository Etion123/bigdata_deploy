#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

# shellcheck source=/dev/null
source /etc/profile.d/bigdata-java.sh 2>/dev/null || true
export JAVA_HOME="${JAVA_HOME:-$(detect_java_home)}"

fail=0
ok() { log "OK  $*"; }
bad() { log "FAIL $*"; fail=1; }

run_bd() { sudo -u "${BD_USER}" -H bash -lc "$*"; }

ensure_stack_profile() {
  local f="/etc/profile.d/bigdata-stack.sh"
  cat >"${f}" <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export HIVE_HOME=${HIVE_HOME}
export HIVE_CONF_DIR=${HIVE_CONF_DIR}
export SPARK_HOME=${SPARK_HOME}
export HBASE_HOME=${HBASE_HOME}
export KAFKA_HOME=${KAFKA_HOME}
export FLINK_HOME=${FLINK_HOME}
export ZOOKEEPER_HOME=${ZOOKEEPER_HOME}
export PATH=\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin:\${HIVE_HOME}/bin:\${SPARK_HOME}/bin:\${HBASE_HOME}/bin:\${KAFKA_HOME}/bin:\${FLINK_HOME}/bin:\${ZOOKEEPER_HOME}/bin:\${PATH}
EOF
  chmod 644 "${f}"
}

ensure_stack_profile

log "Starting ZooKeeper"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${ZOOKEEPER_HOME}/bin/zkServer.sh start" || bad "ZooKeeper start"
sleep 2
if echo ruok | nc -w 2 127.0.0.1 "${ZK_CLIENT_PORT}" 2>/dev/null | grep -q imok; then
  ok "ZooKeeper imok"
else
  bad "ZooKeeper not answering ruok"
fi

log "Starting HDFS / YARN"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${HADOOP_HOME}/sbin/start-dfs.sh"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${HADOOP_HOME}/sbin/start-yarn.sh"
sleep 4
if run_bd "source /etc/profile.d/bigdata-stack.sh; ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /tmp /tmp/hive /user/hive/warehouse /hbase && ${HADOOP_HOME}/bin/hdfs dfs -chmod 777 /tmp/hive"; then
  ok "HDFS mkdir"
else
  bad "HDFS mkdir"
fi
if run_bd "source /etc/profile.d/bigdata-stack.sh; ${HADOOP_HOME}/bin/hdfs dfs -put -f ${HADOOP_HOME}/LICENSE.txt /tmp/LICENSE.verify 2>/dev/null"; then
  ok "HDFS put"
else
  bad "HDFS put"
fi

log "Starting HBase"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${HBASE_HOME}/bin/start-hbase.sh"
sleep 5
if run_bd "source /etc/profile.d/bigdata-stack.sh; echo 'list' | ${HBASE_HOME}/bin/hbase shell 2>/dev/null | head -5"; then
  ok "HBase shell"
else
  bad "HBase shell"
fi

log "Starting Kafka"
run_bd "pkill -f '[k]afka.Kafka' 2>/dev/null || true"
sleep 2
run_bd "source /etc/profile.d/bigdata-stack.sh; nohup ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties >/tmp/kafka-server.log 2>&1 &"
sleep 8
if run_bd "source /etc/profile.d/bigdata-stack.sh; ${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:${KAFKA_PORT} --list"; then
  ok "Kafka topics list"
else
  bad "Kafka topics list"
fi
if run_bd "source /etc/profile.d/bigdata-stack.sh; ${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:${KAFKA_PORT} --create --topic verify-topic --partitions 1 --replication-factor 1 --if-not-exists"; then
  ok "Kafka create topic"
else
  bad "Kafka create topic"
fi

log "Hive"
if run_bd "source /etc/profile.d/bigdata-stack.sh; source ${HIVE_HOME}/conf/hive-env.sh; ${HIVE_HOME}/bin/hive -e 'show databases;' 2>/dev/null"; then
  ok "Hive CLI"
else
  bad "Hive CLI"
fi

log "Spark"
if run_bd "source /etc/profile.d/bigdata-stack.sh; ${SPARK_HOME}/bin/spark-submit --version 2>&1 | head -3"; then
  ok "Spark version"
else
  bad "Spark version"
fi
if run_bd 'source /etc/profile.d/bigdata-stack.sh; j=$(ls ${SPARK_HOME}/examples/jars/spark-examples*.jar 2>/dev/null | head -1); ${SPARK_HOME}/bin/spark-submit --master local[1] --class org.apache.spark.examples.SparkPi "$j" 10 2>&1 | tail -3'; then
  ok "Spark Pi"
else
  bad "Spark Pi"
fi

log "Flink"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${FLINK_HOME}/bin/stop-cluster.sh 2>/dev/null || true"
run_bd "source /etc/profile.d/bigdata-stack.sh; ${FLINK_HOME}/bin/start-cluster.sh" || true
sleep 5
if curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/overview" >/dev/null; then
  ok "Flink REST /overview"
else
  bad "Flink REST /overview"
fi

if [[ "${fail}" -ne 0 ]]; then
  die "One or more checks failed (see FAIL lines above)."
fi
log "All verification checks passed."
