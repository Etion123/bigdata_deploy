"""Component install paths and presence detection (skip-if-installed)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from .context import DeployContext


@dataclass(frozen=True)
class ComponentSpec:
    key: str
    label: str
    rel_dir: str
    markers: Tuple[str, ...]


# Order matches default install pipeline: ZK → Hadoop → Hive → Spark → HBase → Kafka → Flink
COMPONENT_ORDER: List[ComponentSpec] = [
    ComponentSpec("zookeeper", "ZooKeeper", "zookeeper", ("bin/zkServer.sh",)),
    ComponentSpec("hadoop", "Hadoop", "hadoop", ("bin/hadoop", "sbin/start-dfs.sh")),
    ComponentSpec("hive", "Hive", "hive", ("bin/hive",)),
    ComponentSpec("spark", "Spark", "spark", ("bin/spark-submit",)),
    ComponentSpec("hbase", "HBase", "hbase", ("bin/hbase",)),
    ComponentSpec("kafka", "Kafka", "kafka", ("bin/kafka-server-start.sh",)),
    ComponentSpec("flink", "Flink", "flink", ("bin/flink",)),
]

_BY_KEY: Dict[str, ComponentSpec] = {c.key: c for c in COMPONENT_ORDER}


def get_component(key: str) -> ComponentSpec:
    return _BY_KEY[key]


def component_installed(ctx: DeployContext, spec: ComponentSpec) -> bool:
    root = ctx.install_base / spec.rel_dir
    if not root.is_dir():
        return False
    for m in spec.markers:
        p = root / m
        if not p.exists():
            return False
    return True


def installed_summary(ctx: DeployContext) -> List[str]:
    """Human-readable lines for preflight."""
    lines: List[str] = []
    for spec in COMPONENT_ORDER:
        st = "present" if component_installed(ctx, spec) else "absent"
        lines.append(f"  [{st}] {spec.label} -> {ctx.install_base / spec.rel_dir}")
    return lines


def should_skip_component_install(ctx: DeployContext, spec: ComponentSpec) -> bool:
    """Return True to skip install for this component (already present)."""
    if not ctx.skip_if_installed:
        return False
    if not component_installed(ctx, spec):
        return False
    return True
