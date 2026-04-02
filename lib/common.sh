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
INSTALL_BASE="${INSTALL_BASE:-/usr/local/bigdata}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-${INSTALL_BASE}/downloads}"
LOG_DIR="${LOG_DIR:-${INSTALL_BASE}/logs}"

OFFLINE_MODE="${OFFLINE_MODE:-no}"
SKIP_NETWORK_CHECK="${SKIP_NETWORK_CHECK:-no}"
WGET_TRIES="${WGET_TRIES:-5}"
WGET_TIMEOUT="${WGET_TIMEOUT:-60}"

# --- proxy: PKG_PROXY legacy ---
if [[ -z "${HTTP_PROXY:-}" && -n "${PKG_PROXY:-}" ]]; then
  HTTP_PROXY="${PKG_PROXY}"
fi
if [[ -z "${HTTPS_PROXY:-}" && -n "${HTTP_PROXY:-}" ]]; then
  HTTPS_PROXY="${HTTP_PROXY}"
fi
export http_proxy="${HTTP_PROXY:-}"
export https_proxy="${HTTPS_PROXY:-}"
export HTTP_PROXY="${HTTP_PROXY:-}"
export HTTPS_PROXY="${HTTPS_PROXY:-}"
export ALL_PROXY="${ALL_PROXY:-}"
NO_PROXY="${NO_PROXY:-localhost,127.0.0.1,::1}"
export NO_PROXY
export no_proxy="${NO_PROXY}"

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

: "${_NETWORK_PROBE_DONE:=0}"

log() { echo "[$(date '+%F %T')] $*"; }
warn() { echo "[$(date '+%F %T')] WARN: $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

prepare_install_base() {
  mkdir -p "${INSTALL_BASE}" "${DOWNLOAD_DIR}" "${LOG_DIR}"
  chmod 755 "${INSTALL_BASE}" 2>/dev/null || true
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

bd_probe_outbound() {
  [[ "${SKIP_NETWORK_CHECK}" == "yes" ]] && return 0
  [[ "${OFFLINE_MODE}" == "yes" ]] && return 0
  local url="${NETWORK_CHECK_URL:-}"
  [[ -n "${url}" ]] || url="${APACHE_MIRROR}/"
  if command -v curl >/dev/null 2>&1; then
    curl -sfS --connect-timeout 10 --max-time 25 -I -L "${url}" >/dev/null 2>&1 && return 0
    curl -sfS --connect-timeout 10 --max-time 25 -L "${url}" >/dev/null 2>&1 && return 0
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -q --spider --tries=2 --timeout="${WGET_TIMEOUT}" "${url}" && return 0
  fi
  return 1
}

bd_ensure_outbound_for_download() {
  [[ "${OFFLINE_MODE}" == "yes" ]] && return 0
  [[ "${_NETWORK_PROBE_DONE}" -eq 1 ]] && return 0
  _NETWORK_PROBE_DONE=1
  if bd_probe_outbound; then
    log "Outbound check OK (${NETWORK_CHECK_URL:-${APACHE_MIRROR}})"
    return 0
  fi
  warn "Outbound check failed (no proxy or blocked). Will still try wget; place tarballs under ${DOWNLOAD_DIR} or set HTTP_PROXY / OFFLINE_MODE=yes."
}

download_file() {
  local url="$1"
  local dest="$2"
  ensure_dir "$(dirname "${dest}")"
  local base
  base="$(basename "${dest}")"

  if [[ -f "${dest}" ]]; then
    [[ -s "${dest}" ]] || die "Archive exists but is empty: ${dest}"
    log "Reuse existing archive: ${dest}"
    return 0
  fi

  if [[ "${OFFLINE_MODE}" == "yes" ]]; then
    die "OFFLINE_MODE=yes but missing archive: ${dest} (expected ${base} under ${DOWNLOAD_DIR})"
  fi

  bd_ensure_outbound_for_download

  log "Downloading: ${url}"
  local i=1 tries="${WGET_TRIES}"
  while [[ "${i}" -le "${tries}" ]]; do
    if wget -q --show-progress \
      --tries=1 --timeout="${WGET_TIMEOUT}" \
      -O "${dest}.part" "${url}" 2>/dev/null; then
      mv -f "${dest}.part" "${dest}"
      [[ -s "${dest}" ]] || die "Downloaded file is empty: ${dest}"
      log "Saved: ${dest}"
      return 0
    fi
    warn "wget attempt ${i}/${tries} failed for ${url}"
    rm -f "${dest}.part"
    if [[ "${i}" -lt "${tries}" ]]; then
      sleep $((i * 2))
    fi
    i=$((i + 1))
  done

  die "Download failed after ${tries} attempts: ${url}
Hints: export HTTP_PROXY/HTTPS_PROXY or set PKG_PROXY in deploy.conf; use a reachable APACHE_MIRROR; or copy ${base} to ${DOWNLOAD_DIR} and set OFFLINE_MODE=yes."
}

extract_tgz() {
  local archive="$1"
  local target_parent="$2"
  [[ -f "${archive}" ]] || die "Archive not found: ${archive}"
  [[ -s "${archive}" ]] || die "Archive is empty: ${archive}"
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

# Persist dnf proxy (00_setup_repo calls this)
bd_write_dnf_proxy_conf() {
  local proxy="${HTTP_PROXY:-${PKG_PROXY:-}}"
  local dropin="/etc/dnf/dnf.conf.d/90-bigdata-proxy.conf"
  if [[ -z "${proxy}" ]]; then
    rm -f "${dropin}" 2>/dev/null || true
    return 0
  fi
  mkdir -p /etc/dnf/dnf.conf.d
  printf '%s\n' "[main]" "proxy=${proxy}" >"${dropin}"
  chmod 644 "${dropin}"
  log "Wrote dnf proxy drop-in: ${dropin}"
}
