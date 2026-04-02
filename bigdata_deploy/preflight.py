"""Pre-install checks (environment, disk, existing stack)."""

from __future__ import annotations

import os
import shutil
import sys

from .components import COMPONENT_ORDER, component_installed, installed_summary
from .context import DeployContext
from .util import die, log, warn, which


def _min_python() -> None:
    if sys.version_info < (3, 8):
        die("Python 3.8 or newer is required.")


def _disk_space(ctx: DeployContext) -> None:
    mb = ctx.v("PREFLIGHT_MIN_FREE_DISK_MB", "").strip()
    if not mb:
        return
    try:
        need = int(mb) * 1024 * 1024
    except ValueError:
        return
    parent = ctx.install_base.parent
    try:
        usage = shutil.disk_usage(parent)
    except OSError:
        warn(f"Could not read disk usage for {parent}")
        return
    if usage.free < need:
        die(
            f"Insufficient free disk under {parent}: need ≥{mb} MB "
            f"(set PREFLIGHT_MIN_FREE_DISK_MB= to disable)."
        )


def step_preflight(ctx: DeployContext) -> None:
    """Run before component installs when phase includes it."""
    log("Preflight: environment and existing install scan")
    _min_python()
    if os.geteuid() != 0:
        die("Must run as root (sudo) for install phases.")

    parent = ctx.install_base.parent
    if not parent.is_dir():
        die(f"INSTALL_BASE parent does not exist: {parent}")
    try:
        os.makedirs(ctx.install_base, exist_ok=True)
    except OSError as e:
        die(f"Cannot create INSTALL_BASE {ctx.install_base}: {e}")

    _disk_space(ctx)

    if not which("tar") or not which("gzip"):
        warn("tar/gzip not found in PATH — repo step will install packages.")

    log("Component paths under INSTALL_BASE (SKIP_IF_INSTALLED=yes skips reinstall if markers exist):")
    for line in installed_summary(ctx):
        print(line, flush=True)

    present = [c.label for c in COMPONENT_ORDER if component_installed(ctx, c)]
    if present:
        warn(f"Already present (will skip per-component if SKIP_IF_INSTALLED=yes): {', '.join(present)}")
    log("Preflight OK.")
