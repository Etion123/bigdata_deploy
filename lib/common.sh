#!/usr/bin/env bash
# shellcheck disable=SC2034
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${CONFIG_FILE:-${ROOT_DIR}/config/deploy.conf}"

if [[ -f "${CONFIG_FILE}" ]]; then
  # shellcheck source=/dev/null
  source "${CONFIG_FILE}"
fi

BD_USER="${BD_USER:-hadoop}"
BD_GROUP="${BD_GROUP:-hadoop}"
NN_RPC_PORT="${NN_RPC_PORT:-9000}"
NN_HTTP_PORT="${NN_HTTP_PORT:-9870}"
RM_WEB_PORT="${RM_WEB_PORT:-8088}"
ZK_CLIENT_PORT="${ZK_CLIENT_PORT:-2181}"
HIVE_SERVER2_PORT="${HIVE_SERVER2_PORT:-10000}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
FLINK_JOBMANAGER_RPC_PORT="${FLINK_JOBMANAGER_RPC_PORT:-6123}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8081}"

BD_HOME="${BD_HOME:-/home/${BD_USER}}"
INSTALL_BASE="${INSTALL_BASE:-/opt/bigdata}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-${INSTALL_BASE}/downloads}"
LOG_DIR="${LOG_DIR:-${INSTALL_BASE}/logs}"

export JAVA_HOME="${JAVA_HOME:-}"
export HADOOP_HOME="${INSTALL_BASE}/hadoop"
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export HIVE_HOME="${INSTALL_BASE}/hive"
export HIVE_CONF_DIR="${HIVE_HOME}/conf"
export SPARK_HOME="${INSTALL_BASE}/spark"
export HBASE_HOME="${INSTALL_BASE}/hbase"
export KAFKA_HOME="${INSTALL_BASE}/kafka"
export FLINK_HOME="${INSTALL_BASE}/flink"
export ZOOKEEPER_HOME="${INSTALL_BASE}/zookeeper"

log() { echo "[$(date '+%F %T')] $*"; }
die() { log "ERROR: $*"; exit 1; }

prepare_install_base() {
  mkdir -p "${INSTALL_BASE}" "${DOWNLOAD_DIR}" "${LOG_DIR}"
  if getent passwd "${BD_USER}" >/dev/null 2>&1; then
    chown "${BD_USER}:${BD_GROUP}" "${INSTALL_BASE}" "${DOWNLOAD_DIR}" "${LOG_DIR}" 2>/dev/null || true
  fi
}

require_root() {
  [[ "$(id -u)" -eq 0 ]] || die "Run as root on the target openEuler host."
}

run_as_bd() {
  local cmd="$*"
  sudo -u "${BD_USER}" -H bash -lc "${cmd}"
}

ensure_dir() {
  mkdir -p "$1"
  if getent passwd "${BD_USER}" >/dev/null 2>&1; then
    chown "${BD_USER}:${BD_GROUP}" "$1" 2>/dev/null || true
  fi
}

download_file() {
  local url="$1"
  local dest="$2"
  ensure_dir "$(dirname "${dest}")"
  if [[ -f "${dest}" ]]; then
    log "Reuse existing archive: ${dest}"
    return 0
  fi
  log "Downloading: ${url}"
  wget -q --show-progress -O "${dest}.part" "${url}" || die "wget failed: ${url}"
  mv "${dest}.part" "${dest}"
}

extract_tgz() {
  local archive="$1"
  local target_parent="$2"
  tar -xzf "${archive}" -C "${target_parent}"
}

apache_url() {
  local path="$1"
  echo "${APACHE_MIRROR}/${path}"
}

detect_java_home() {
  if [[ -n "${JAVA_HOME:-}" && -d "${JAVA_HOME}" ]]; then
    echo "${JAVA_HOME}"
    return
  fi
  local v
  v="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")" 2>/dev/null || true)"
  if [[ -n "${v}" && -d "${v}" ]]; then
    echo "${v}"
    return
  fi
  die "JAVA_HOME not set and java not in PATH"
}

# Replace @PLACEHOLDER@ in template file -> dest (stream)
render_template() {
  local src="$1"
  local dest="$2"
  local host
  host="$(hostname -f 2>/dev/null || hostname)"
  sed \
    -e "s|@HOSTNAME@|${host}|g" \
    -e "s|@INSTALL_BASE@|${INSTALL_BASE}|g" \
    -e "s|@NN_RPC_PORT@|${NN_RPC_PORT}|g" \
    -e "s|@NN_HTTP_PORT@|${NN_HTTP_PORT}|g" \
    -e "s|@RM_WEB_PORT@|${RM_WEB_PORT}|g" \
    -e "s|@ZK_CLIENT_PORT@|${ZK_CLIENT_PORT}|g" \
    -e "s|@HIVE_SERVER2_PORT@|${HIVE_SERVER2_PORT}|g" \
    -e "s|@KAFKA_PORT@|${KAFKA_PORT}|g" \
    -e "s|@FLINK_JOBMANAGER_RPC_PORT@|${FLINK_JOBMANAGER_RPC_PORT}|g" \
    -e "s|@FLINK_WEB_PORT@|${FLINK_WEB_PORT}|g" \
    "${src}" >"${dest}"
}
