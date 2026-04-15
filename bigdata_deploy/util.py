"""Shell helpers, download (online) / local archive (offline), templates."""

from __future__ import annotations

import datetime as _dt
import os
import sys
import re
import shutil
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import List, Optional

from .context import DeployContext

_log_probe_done = False


def log(msg: str) -> None:
    ts = _dt.datetime.now().strftime("%F %T")
    print(f"[{ts}] {msg}", flush=True)


def warn(msg: str) -> None:
    ts = _dt.datetime.now().strftime("%F %T")
    print(f"[{ts}] WARN: {msg}", file=sys.stderr, flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"ERROR: {msg}")
    raise SystemExit(code)


def require_root() -> None:
    if os.geteuid() != 0:
        die("Run as root on the target Linux host (sudo).")


def run(
    cmd: List[str],
    *,
    ctx: Optional[DeployContext] = None,
    check: bool = True,
    shell: bool = False,
    input: Optional[bytes] = None,
) -> subprocess.CompletedProcess:
    env = ctx.child_env() if ctx else None
    return subprocess.run(
        cmd,
        check=check,
        shell=shell,
        env=env,
        input=input,
        capture_output=False,
    )


def run_capture(
    cmd: List[str],
    *,
    ctx: Optional[DeployContext] = None,
    text: bool = True,
) -> subprocess.CompletedProcess:
    env = ctx.child_env() if ctx else None
    return subprocess.run(cmd, capture_output=True, text=text, env=env)


def run_as_bd(ctx: DeployContext, bash_cmd: str, check: bool = True) -> None:
    subprocess.run(
        ["sudo", "-u", ctx.bd_user, "-H", "bash", "-lc", bash_cmd],
        check=check,
        env=ctx.child_env(),
    )


def which(name: str) -> Optional[str]:
    return shutil.which(name)


def prepare_install_base(ctx: DeployContext) -> None:
    for p in (ctx.install_base, ctx.download_dir, ctx.log_dir):
        p.mkdir(parents=True, exist_ok=True)
        try:
            import pwd

            u = pwd.getpwnam(ctx.bd_user)
            os.chown(p, u.pw_uid, u.pw_gid)
        except (KeyError, ImportError, PermissionError):
            pass
    os.chmod(ctx.install_base, 0o755)


def ensure_dir(path: Path, ctx: DeployContext) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        import pwd

        u = pwd.getpwnam(ctx.bd_user)
        os.chown(path, u.pw_uid, u.pw_gid)
    except (KeyError, ImportError, PermissionError):
        pass


def chown_tree(path: Path, user: str, group: str) -> None:
    run(["chown", "-R", f"{user}:{group}", str(path)], check=False)


def write_dnf_proxy(ctx: DeployContext) -> None:
    proxy = ctx.http_proxy
    dropin = Path("/etc/dnf/dnf.conf.d/90-bigdata-proxy.conf")
    if not proxy:
        dropin.unlink(missing_ok=True)
        return
    dropin.parent.mkdir(parents=True, exist_ok=True)
    dropin.write_text(f"[main]\nproxy={proxy}\n", encoding="utf-8")
    dropin.chmod(0o644)
    log(f"Wrote dnf proxy drop-in: {dropin}")


def hostname_fqdn() -> str:
    try:
        return socket.getfqdn()
    except OSError:
        return socket.gethostname()


def render_template(src: Path, dest: Path, ctx: DeployContext) -> None:
    text = src.read_text(encoding="utf-8")
    rep = {
        "@HOSTNAME@": hostname_fqdn(),
        "@MASTER_HOST@": ctx.master_host(),
        "@DFS_REPLICATION@": str(ctx.dfs_replication()),
        "@HBASE_DISTRIBUTED@": "true" if ctx.hbase_distributed() else "false",
        "@INSTALL_BASE@": str(ctx.install_base),
        "@NN_RPC_PORT@": ctx.v("NN_RPC_PORT", "9000"),
        "@NN_HTTP_PORT@": ctx.v("NN_HTTP_PORT", "9870"),
        "@RM_WEB_PORT@": ctx.v("RM_WEB_PORT", "8088"),
        "@ZK_CLIENT_PORT@": ctx.v("ZK_CLIENT_PORT", "2181"),
        "@HIVE_SERVER2_PORT@": ctx.v("HIVE_SERVER2_PORT", "10000"),
        "@KAFKA_PORT@": ctx.v("KAFKA_PORT", "9092"),
        "@FLINK_JOBMANAGER_RPC_PORT@": ctx.v("FLINK_JOBMANAGER_RPC_PORT", "6123"),
        "@FLINK_WEB_PORT@": ctx.v("FLINK_WEB_PORT", "8081"),
        "@TEZ_VERSION@": ctx.v("TEZ_VERSION", "0.10.0"),
    }
    for k, v in rep.items():
        text = text.replace(k, v)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")


def apache_url(ctx: DeployContext, path: str) -> str:
    path = path.lstrip("/")
    return f"{ctx.apache_mirror}/{path}"


def probe_outbound(ctx: DeployContext) -> bool:
    if ctx.skip_network_check or ctx.offline_mode:
        return True
    url = ctx.v("NETWORK_CHECK_URL", "").strip() or f"{ctx.apache_mirror}/"
    cap = run_capture(["curl", "-sfS", "--connect-timeout", "10", "--max-time", "25", "-I", "-L", url], ctx=ctx)
    if cap.returncode == 0:
        return True
    cap = run_capture(
        ["curl", "-sfS", "--connect-timeout", "10", "--max-time", "25", "-L", url],
        ctx=ctx,
    )
    if cap.returncode == 0:
        return True
    if which("wget"):
        r = run_capture(
            ["wget", "-q", "--spider", "--tries=2", f"--timeout={ctx.wget_timeout}", url],
            ctx=ctx,
        )
        if r.returncode == 0:
            return True
    return False


def ensure_outbound_hint(ctx: DeployContext) -> None:
    global _log_probe_done
    if ctx.offline_mode or _log_probe_done:
        return
    _log_probe_done = True
    if probe_outbound(ctx):
        log(f"Outbound check OK ({ctx.v('NETWORK_CHECK_URL', '') or ctx.apache_mirror})")
    else:
        warn(
            "Outbound check failed. Use HTTP_PROXY, place tarballs under "
            f"{ctx.download_dir}, or set OFFLINE_MODE=yes."
        )


def download_file(ctx: DeployContext, url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 0:
        log(f"Reuse existing archive: {dest}")
        return
    if dest.is_file() and dest.stat().st_size == 0:
        die(f"Archive exists but is empty: {dest}")

    if ctx.offline_mode:
        die(
            f"OFFLINE_MODE=yes but missing archive: {dest}\n"
            f"Place the correct file as: {dest.name} under {ctx.download_dir}"
        )

    ensure_outbound_hint(ctx)
    log(f"Downloading: {url}")

    proxy = ctx.http_proxy
    ctx_ssl = ssl.create_default_context()
    handlers: List[urllib.request.BaseHandler] = [
        urllib.request.HTTPSHandler(context=ctx_ssl),
    ]
    if proxy:
        handlers.insert(
            0,
            urllib.request.ProxyHandler({"http": proxy, "https": ctx.https_proxy or proxy}),
        )
    opener = urllib.request.build_opener(*handlers)
    part = dest.with_suffix(dest.suffix + ".part")
    tries = ctx.wget_tries
    timeout = ctx.wget_timeout
    last_err: Optional[BaseException] = None
    for attempt in range(1, tries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "bigdata-deploy/1.0"})
            with opener.open(req, timeout=timeout) as resp:
                data = resp.read()
            if not data:
                raise RuntimeError("empty response")
            part.write_bytes(data)
            shutil.move(str(part), str(dest))
            log(f"Saved: {dest}")
            return
        except (urllib.error.URLError, OSError, ssl.SSLError, RuntimeError, TimeoutError) as e:
            last_err = e
            warn(f"Download attempt {attempt}/{tries} failed: {e}")
            part.unlink(missing_ok=True)
            if attempt < tries:
                time.sleep(attempt * 2)
    die(
        f"Download failed after {tries} attempts: {url}\nLast error: {last_err}\n"
        "Hints: HTTP_PROXY/PKG_PROXY, reachable APACHE_MIRROR, or copy the file to "
        f"{ctx.download_dir} and set OFFLINE_MODE=yes."
    )


def extract_tgz(archive: Path, target_parent: Path) -> None:
    if not archive.is_file():
        die(f"Archive not found: {archive}")
    if archive.stat().st_size == 0:
        die(f"Archive is empty: {archive}")
    target_parent.mkdir(parents=True, exist_ok=True)
    run(["tar", "-xzf", str(archive), "-C", str(target_parent)], check=True)


def detect_java_home() -> str:
    if os.environ.get("JAVA_HOME") and Path(os.environ["JAVA_HOME"]).is_dir():
        return os.environ["JAVA_HOME"]
    alt = Path("/etc/alternatives/java")
    if alt.is_symlink():
        resolved = alt.resolve()
        # .../bin/java -> java home is parent of bin
        if resolved.name == "java" and resolved.parent.name == "bin":
            return str(resolved.parent.parent)
    java = which("java")
    if java:
        resolved = Path(java).resolve()
        if resolved.parent.name == "bin":
            return str(resolved.parent.parent)
    die("JAVA_HOME not set and java not in PATH")


def bd_home(ctx: DeployContext) -> Path:
    h = ctx.v("BD_HOME", "").strip()
    if h:
        return Path(h)
    try:
        import pwd

        return Path(pwd.getpwnam(ctx.bd_user).pw_dir)
    except KeyError:
        return Path("/home") / ctx.bd_user


def expected_offline_archives(ctx: DeployContext) -> List[str]:
    """Filenames that must exist in download_dir when using offline full install."""
    if ctx.is_worker:
        hv = ctx.v("HADOOP_VERSION", "3.2.0")
        return [f"hadoop-{hv}.tar.gz"]
    zv = ctx.v("ZOOKEEPER_VERSION", "3.6.2")
    hv = ctx.v("HADOOP_VERSION", "3.2.0")
    yv = ctx.v("HIVE_VERSION", "3.1.0")
    tv = ctx.v("TEZ_VERSION", "0.10.0")
    scv = ctx.v("SCALA_VERSION", "2.12.13")
    bv = ctx.v("HBASE_VERSION", "2.2.3")
    kv = ctx.v("KAFKA_VERSION", "2.8.1")
    kscala = ctx.v("KAFKA_SCALA_VERSION", "2.13")
    sv = ctx.v("SPARK_VERSION", "3.3.1")
    prof = ctx.v("SPARK_HADOOP_PROFILE", "hadoop3")
    fv = ctx.v("FLINK_VERSION", "1.15.0")
    fscala = ctx.v("FLINK_SCALA_VERSION", "2.12")
    names = [
        f"apache-zookeeper-{zv}-bin.tar.gz",
        f"hadoop-{hv}.tar.gz",
        f"apache-hive-{yv}-bin.tar.gz",
        f"apache-tez-{tv}-bin.tar.gz",
        f"scala-{scv}.tgz",
        f"spark-{sv}-bin-{prof}.tgz",
        f"hbase-{bv}-bin.tar.gz",
        f"kafka_{kscala}-{kv}.tgz",
        f"flink-{fv}-bin-scala_{fscala}.tgz",
    ]
    if ctx.v("JAVA_USE_SYSTEM", "yes").strip().lower() not in ("1", "yes", "true", "on"):
        ju = ctx.v("JAVA_TARBALL_URL", "")
        if ju:
            names.append(Path(ju).name)
    return names
