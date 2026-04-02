"""Parse deploy.conf (KEY=value, # comments). Supports ${VAR} expansion."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict


_VAR = re.compile(r"\$\{([^}]+)\}")


def load_deploy_conf(path: Path) -> Dict[str, str]:
    if not path.is_file():
        return {}
    raw: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            raw[k] = v
    return expand_vars(raw)


def expand_vars(d: Dict[str, str]) -> Dict[str, str]:
    out = dict(d)
    for _ in range(64):
        changed = False
        for k, v in list(out.items()):
            def repl(m: re.Match[str]) -> str:
                name = m.group(1)
                return out.get(name, m.group(0))

            nv = _VAR.sub(repl, v)
            if nv != v:
                out[k] = nv
                changed = True
        if not changed:
            break
    return out


def truthy(val: str, default: bool = False) -> bool:
    if val is None or val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def getenv_overlay() -> Dict[str, str]:
    """Environment overrides (same keys as deploy.conf, uppercase)."""
    import os

    keys = (
        "OFFLINE_MODE",
        "SKIP_NETWORK_CHECK",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "PKG_PROXY",
        "NO_PROXY",
        "INSTALL_BASE",
        "APACHE_MIRROR",
        "CONFIG_FILE",
        "JAVA_USE_SYSTEM",
        "CLUSTER_MODE",
        "NODE_ROLE",
        "CLUSTER_MASTER_HOST",
        "WORKER_HOSTS",
        "MASTER_AS_DATANODE",
        "HDFS_REPLICATION",
        "SKIP_IF_INSTALLED",
        "PREFLIGHT_MIN_FREE_DISK_MB",
    )
    o: Dict[str, str] = {}
    for k in keys:
        v = os.environ.get(k)
        if v is not None and v != "":
            o[k] = v
    return o
