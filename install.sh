#!/usr/bin/env bash
# Single-node bigdata stack installer for openEuler 22.03 SP4
# Usage: sudo ./install.sh [phase]
#   phase: all | to-spark | verify-spark | repo | disk | ssh | jdk | zk | hadoop | hive | hbase | kafka | spark | flink | verify
#   to-spark: repo..hadoop + spark + 88_verify_spark (good for blocked-Hive or staged rollout)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CONFIG_FILE="${CONFIG_FILE:-${ROOT_DIR}/config/deploy.conf}"

# shellcheck source=/dev/null
source "${ROOT_DIR}/lib/common.sh"
require_root

phase="${1:-all}"

run_step() {
  local name="$1"
  local s="${ROOT_DIR}/scripts/${name}"
  [[ -f "${s}" ]] || die "Missing script ${s}"
  chmod +x "${s}" 2>/dev/null || true
  echo "[install] === ${name} ==="
  bash "${s}"
}

case "${phase}" in
  all)
    run_step 00_setup_repo.sh
    run_step 01_mount_data_disk.sh
    run_step 02_users_ssh.sh
    run_step 03_jdk.sh
    run_step 04_zookeeper.sh
    run_step 05_hadoop.sh
    run_step 06_hive.sh
    run_step 07_hbase.sh
    run_step 08_kafka.sh
    run_step 09_spark.sh
    run_step 10_flink.sh
    run_step 99_verify.sh
    ;;
  to-spark)
    run_step 00_setup_repo.sh
    run_step 01_mount_data_disk.sh
    run_step 02_users_ssh.sh
    run_step 03_jdk.sh
    run_step 04_zookeeper.sh
    run_step 05_hadoop.sh
    run_step 09_spark.sh
    run_step 88_verify_spark.sh
    ;;
  verify-spark)
    run_step 88_verify_spark.sh
    ;;
  repo) run_step 00_setup_repo.sh ;;
  disk) run_step 01_mount_data_disk.sh ;;
  ssh) run_step 02_users_ssh.sh ;;
  jdk) run_step 03_jdk.sh ;;
  zk) run_step 04_zookeeper.sh ;;
  hadoop) run_step 05_hadoop.sh ;;
  hive) run_step 06_hive.sh ;;
  hbase) run_step 07_hbase.sh ;;
  kafka) run_step 08_kafka.sh ;;
  spark) run_step 09_spark.sh ;;
  flink) run_step 10_flink.sh ;;
  verify) run_step 99_verify.sh ;;
  *)
    echo "Unknown phase: ${phase}"
    exit 2
    ;;
esac

echo "[install] Done: ${phase}"
