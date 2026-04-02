"""Resolved deployment configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

from .config_loader import truthy


@dataclass
class DeployContext:
    root_dir: Path
    values: Dict[str, str]

    def v(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)

    def _p(self, key: str, default: str) -> Path:
        return Path(self.v(key, default))

    @property
    def bd_user(self) -> str:
        return self.v("BD_USER", "hadoop")

    @property
    def bd_group(self) -> str:
        return self.v("BD_GROUP", "hadoop")

    @property
    def install_base(self) -> Path:
        return self._p("INSTALL_BASE", "/usr/local/bigdata")

    @property
    def download_dir(self) -> Path:
        p = self.v("DOWNLOAD_DIR", "")
        if p:
            return Path(p)
        return self.install_base / "downloads"

    @property
    def log_dir(self) -> Path:
        p = self.v("LOG_DIR", "")
        if p:
            return Path(p)
        return self.install_base / "logs"

    @property
    def templates_dir(self) -> Path:
        return self.root_dir / "templates"

    @property
    def offline_mode(self) -> bool:
        return truthy(self.v("OFFLINE_MODE", "no"))

    @property
    def skip_network_check(self) -> bool:
        return truthy(self.v("SKIP_NETWORK_CHECK", "no"))

    @property
    def http_proxy(self) -> str:
        h = self.v("HTTP_PROXY", "").strip()
        if h:
            return h
        return self.v("PKG_PROXY", "").strip()

    @property
    def https_proxy(self) -> str:
        h = self.v("HTTPS_PROXY", "").strip()
        if h:
            return h
        return self.http_proxy

    @property
    def no_proxy(self) -> str:
        return self.v("NO_PROXY", "localhost,127.0.0.1,::1")

    @property
    def apache_mirror(self) -> str:
        return self.v("APACHE_MIRROR", "https://archive.apache.org/dist").rstrip("/")

    @property
    def wget_tries(self) -> int:
        try:
            return max(1, int(self.v("WGET_TRIES", "5")))
        except ValueError:
            return 5

    @property
    def wget_timeout(self) -> int:
        try:
            return max(5, int(self.v("WGET_TIMEOUT", "60")))
        except ValueError:
            return 60

    @property
    def hadoop_home(self) -> Path:
        return self.install_base / "hadoop"

    @property
    def hive_home(self) -> Path:
        return self.install_base / "hive"

    @property
    def spark_home(self) -> Path:
        return self.install_base / "spark"

    @property
    def hbase_home(self) -> Path:
        return self.install_base / "hbase"

    @property
    def kafka_home(self) -> Path:
        return self.install_base / "kafka"

    @property
    def flink_home(self) -> Path:
        return self.install_base / "flink"

    @property
    def zookeeper_home(self) -> Path:
        return self.install_base / "zookeeper"

    def child_env(self) -> Dict[str, str]:
        import os

        e = os.environ.copy()
        if self.http_proxy:
            e["http_proxy"] = self.http_proxy
            e["HTTP_PROXY"] = self.http_proxy
        if self.https_proxy:
            e["https_proxy"] = self.https_proxy
            e["HTTPS_PROXY"] = self.https_proxy
        e["NO_PROXY"] = self.no_proxy
        e["no_proxy"] = self.no_proxy
        ap = self.v("ALL_PROXY", "").strip()
        if ap:
            e["ALL_PROXY"] = ap
        return e


def build_context(root_dir: Path, values: Mapping[str, Any]) -> DeployContext:
    merged: Dict[str, str] = {str(k): str(v) for k, v in values.items()}
    return DeployContext(root_dir=root_dir.resolve(), values=merged)
