#!/usr/bin/env python3
"""
Bigdata installer (openEuler / RHEL): single-node or 1+N cluster.
Supports x86_64 and aarch64 (ARM) architectures.

  sudo python3 install.py preflight
  sudo python3 install.py all          # ZK → Hadoop → Hive(+Tez) → Scala → Spark → HBase → Kafka → Flink
  sudo python3 install.py cluster-worker
  python3 install.py list-bundles

SKIP_IF_INSTALLED=yes (default): existing component dirs under INSTALL_BASE are skipped.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bigdata_deploy.config_loader import getenv_overlay, load_deploy_conf
from bigdata_deploy.context import build_context
from bigdata_deploy.preflight import step_preflight
from bigdata_deploy.steps import (
    step_disk,
    step_flink,
    step_hadoop,
    step_hbase,
    step_hive,
    step_jdk,
    step_kafka,
    step_repo,
    step_scala,
    step_spark,
    step_ssh,
    step_tez,
    step_verify_full,
    step_verify_spark,
    step_zookeeper,
)
from bigdata_deploy.util import expected_offline_archives, log


def _load_ctx(config_path: Path):
    data = load_deploy_conf(config_path)
    data.update(getenv_overlay())
    return build_context(ROOT, data)


def _run_steps(ctx, names: list) -> None:
    for fn in names:
        print(f"[install] === {fn.__name__} ===", flush=True)
        fn(ctx)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bigdata deploy (single-node or cluster, x86_64/aarch64)")
    parser.add_argument(
        "-c", "--config", type=Path, default=None,
        help="Path to deploy.conf (default: CONFIG_FILE env or ./config/deploy.conf)",
    )
    parser.add_argument(
        "phase", nargs="?", default="all",
        help="install phase or list-bundles",
    )
    args = parser.parse_args()

    conf = args.config
    if conf is None:
        conf = Path(os.environ.get("CONFIG_FILE", str(ROOT / "config" / "deploy.conf")))

    if args.phase == "list-bundles":
        ctx = _load_ctx(conf)
        log(f"With current config (arch={ctx.arch}), place these files under {ctx.download_dir}:")
        for name in expected_offline_archives(ctx):
            print(f"  {name}")
        if ctx.v("JAVA_USE_SYSTEM", "yes").lower() in ("1", "yes", "true", "on"):
            print("  (JDK: JAVA_USE_SYSTEM=yes — install java-1.8.0-openjdk from local dnf repo, no tarball.)")
        if ctx.is_worker:
            print("  (NODE_ROLE=worker: only Hadoop tarball required on workers.)")
        return 0

    ctx = _load_ctx(conf)

    if ctx.is_worker and args.phase in ("all", "to-spark", "verify", "verify-spark"):
        print("NODE_ROLE=worker: use phase cluster-worker (not all/to-spark/verify).", file=sys.stderr)
        return 2

    if args.phase == "cluster-worker":
        if not ctx.cluster_mode or not ctx.is_worker:
            print("phase cluster-worker requires CLUSTER_MODE=yes and NODE_ROLE=worker.", file=sys.stderr)
            return 2

    # Order: ZK → Hadoop → Hive(+Tez) → Scala → Spark → HBase → Kafka → Flink
    steps_map = {
        "all": [
            step_preflight,
            step_repo,
            step_disk,
            step_ssh,
            step_jdk,
            step_zookeeper,
            step_hadoop,
            step_tez,
            step_hive,
            step_scala,
            step_spark,
            step_hbase,
            step_kafka,
            step_flink,
            step_verify_full,
        ],
        "to-spark": [
            step_preflight,
            step_repo,
            step_disk,
            step_ssh,
            step_jdk,
            step_zookeeper,
            step_hadoop,
            step_tez,
            step_hive,
            step_scala,
            step_spark,
            step_verify_spark,
        ],
        "verify-spark": [step_verify_spark],
        "verify": [step_verify_full],
        "preflight": [step_preflight],
        "repo": [step_repo],
        "disk": [step_disk],
        "ssh": [step_ssh],
        "jdk": [step_jdk],
        "zk": [step_zookeeper],
        "hadoop": [step_hadoop],
        "tez": [step_tez],
        "hive": [step_hive],
        "scala": [step_scala],
        "spark": [step_spark],
        "hbase": [step_hbase],
        "kafka": [step_kafka],
        "flink": [step_flink],
        "cluster-worker": [
            step_preflight,
            step_repo,
            step_disk,
            step_ssh,
            step_jdk,
            step_hadoop,
        ],
    }

    if args.phase not in steps_map:
        print(f"Unknown phase: {args.phase}", file=sys.stderr)
        return 2

    _run_steps(ctx, steps_map[args.phase])
    print(f"[install] Done: {args.phase}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
