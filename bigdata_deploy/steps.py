"""Install steps."""

from __future__ import annotations

import glob
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from .components import get_component, should_skip_component_install
from .context import DeployContext
from .util import (
    apache_url,
    bd_home,
    chown_tree,
    detect_java_home,
    die,
    download_file,
    ensure_dir,
    extract_tgz,
    log,
    prepare_install_base,
    render_template,
    require_root,
    run,
    run_as_bd,
    run_capture,
    warn,
    which,
    write_dnf_proxy,
)


# ---------------------------------------------------------------------------
# repo / disk / ssh / jdk — unchanged logic, minor polish
# ---------------------------------------------------------------------------

def step_repo(ctx: DeployContext) -> None:
    require_root()
    write_dnf_proxy(ctx)
    lf = ctx.v("LOCAL_REPO_FILE", "").strip()
    if lf:
        p = Path(lf)
        if not p.is_file():
            die(f"LOCAL_REPO_FILE not found: {lf}")
        if ctx.v("LOCAL_REPO_ENABLED", "yes").lower() == "yes":
            bak = Path(f"/etc/yum.repos.d/.bigdata_deploy_backup_{int(time.time())}")
            bak.mkdir(parents=True, exist_ok=True)
            log(f"Backing up existing repo files to {bak}")
            for f in Path("/etc/yum.repos.d").glob("*.repo"):
                shutil.move(str(f), str(bak / f.name))
        shutil.copyfile(p, "/etc/yum.repos.d/bigdata-local.repo")
        Path("/etc/yum.repos.d/bigdata-local.repo").chmod(0o644)
        log("Installed repo: /etc/yum.repos.d/bigdata-local.repo")

    r = subprocess.run(
        ["dnf", "-y", "install", "wget", "tar", "gzip", "which", "nc",
         "openssh-clients", "util-linux", "parted", "curl", "ca-certificates"],
        env=ctx.child_env(),
    )
    if r.returncode != 0:
        die("dnf install base tools failed (network, proxy, or repos).")
    r = subprocess.run(["dnf", "-y", "makecache"], env=ctx.child_env())
    if r.returncode != 0:
        die("dnf makecache failed. Use LOCAL_REPO_FILE or HTTP_PROXY.")
    log("Repository setup done.")


def _pick_data_disk(ctx: DeployContext) -> Path:
    dev = ctx.v("DATA_DISK_DEVICE", "").strip()
    if dev:
        return Path(dev)
    root_src = run_capture(["findmnt", "-n", "-o", "SOURCE", "/"], ctx=ctx)
    root_line = (root_src.stdout or "").strip()
    root_disk = ""
    if root_line:
        import re
        s = re.sub(r"\[.*\]", "", root_line)
        s = re.sub(r"\d+$", "", s)
        s = re.sub(r"p$", "", s)
        root_disk = Path(s).name
    out = run_capture(["lsblk", "-dn", "-o", "NAME,TYPE,MOUNTPOINT"], ctx=ctx)
    for line in (out.stdout or "").splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        name, typ, mnt = parts[0], parts[1], parts[2]
        if typ != "disk" or mnt.strip() or name == root_disk:
            continue
        return Path("/dev") / name
    die("No idle disk candidate. Set DATA_DISK_DEVICE or disable AUTO_MOUNT_DATA_DISK.")


def step_disk(ctx: DeployContext) -> None:
    require_root()
    if ctx.v("AUTO_MOUNT_DATA_DISK", "no").lower() != "yes":
        log("AUTO_MOUNT_DATA_DISK!=yes, skip disk mount")
        return
    if not which("parted") or not which("wipefs"):
        die("parted/wipefs missing (run repo step first).")
    dev = _pick_data_disk(ctx)
    if not dev.is_block_device():
        die(f"Not a block device: {dev}")
    log(f"Using data disk: {dev}")
    part = Path(str(dev) + "1")
    if not part.is_block_device():
        log(f"Partitioning {dev} — DATA WILL BE WIPED")
        run(["wipefs", "-a", str(dev)], check=False)
        run(["parted", "-s", str(dev), "mklabel", "gpt"], check=True)
        fst = ctx.v("DATA_DISK_FSTYPE", "xfs")
        run(["parted", "-s", str(dev), "mkpart", "primary", fst, "0%", "100%"], check=True)
        run(["partprobe", str(dev)], check=False)
        time.sleep(2)
    if not part.is_block_device():
        die(f"Expected partition {part} missing.")
    bid = run_capture(["blkid", str(part)], ctx=ctx)
    has_fs = "TYPE" in (bid.stdout or "")
    if not has_fs:
        fst = ctx.v("DATA_DISK_FSTYPE", "xfs")
        log(f"Creating {fst} on {part}")
        if fst == "xfs":
            run(["mkfs.xfs", "-f", str(part)], check=True)
        else:
            run(["mkfs.ext4", "-F", str(part)], check=True)
    mnt = Path(ctx.v("DATA_MOUNT_POINT", "/data"))
    mnt.mkdir(parents=True, exist_ok=True)
    chk = run_capture(["mountpoint", "-q", str(mnt)], ctx=ctx)
    if chk.returncode != 0:
        run(["mount", str(part), str(mnt)], check=True)
    uid = run_capture(["blkid", "-s", "UUID", "-o", "value", str(part)], ctx=ctx)
    uuid = (uid.stdout or "").strip()
    if not uuid:
        die(f"Could not read UUID for {part}")
    fst = ctx.v("DATA_DISK_FSTYPE", "xfs")
    fstab = Path("/etc/fstab")
    line = f"UUID={uuid}  {mnt}  {fst}  defaults,noatime  0  0\n"
    if f"UUID={uuid}" not in fstab.read_text(encoding="utf-8", errors="replace"):
        with fstab.open("a", encoding="utf-8") as f:
            f.write(line)
    log(f"Data disk mounted at {mnt}")


def step_ssh(ctx: DeployContext) -> None:
    require_root()
    run(["groupadd", ctx.bd_group], check=False)
    run(["useradd", "-m", "-g", ctx.bd_group, "-s", "/bin/bash", ctx.bd_user], check=False)

    short = run_capture(["hostname", "-s"], ctx=ctx).stdout.strip()
    long_h = run_capture(["hostname", "-f"], ctx=ctx).stdout.strip() or short
    ip_out = run_capture(["hostname", "-I"], ctx=ctx).stdout.strip().split()
    primary_ip = ip_out[0] if ip_out else ""
    hosts = Path("/etc/hosts")
    if primary_ip and long_h and long_h not in hosts.read_text(encoding="utf-8", errors="replace"):
        with hosts.open("a", encoding="utf-8") as f:
            f.write(f"{primary_ip} {long_h} {short}\n")
        log(f"Appended hosts entry: {primary_ip} {long_h} {short}")

    subprocess.run(
        ["dnf", "-y", "install", "openssh-server", "openssh-clients", "nc"],
        env=ctx.child_env(), check=False,
    )
    run(["systemctl", "enable", "sshd"], check=False)
    run(["systemctl", "start", "sshd"], check=False)

    if ctx.v("CONFIGURE_SSH_LOCALHOST", "yes").lower() != "yes":
        log("CONFIGURE_SSH_LOCALHOST!=yes, skip localhost keys")
        return

    home = bd_home(ctx)
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True, exist_ok=True)
    run(["chown", f"{ctx.bd_user}:{ctx.bd_group}", str(ssh_dir)])
    run(["chmod", "700", str(ssh_dir)])
    key = ssh_dir / "id_rsa"
    if not key.is_file():
        run_as_bd(ctx, f'ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')
    auth = ssh_dir / "authorized_keys"
    pub = ssh_dir / "id_rsa.pub"
    if pub.is_file():
        text = auth.read_text(encoding="utf-8", errors="replace") if auth.is_file() else ""
        if "localhost" not in text:
            with auth.open("a", encoding="utf-8") as f:
                f.write(pub.read_text(encoding="utf-8", errors="replace"))
    run(["chmod", "600", str(auth)])
    port = ctx.v("SSH_PORT", "22")
    run_as_bd(ctx, f"ssh-keyscan -p {port} -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null || true")
    run_as_bd(ctx, f"ssh-keyscan -p {port} -H localhost >> ~/.ssh/known_hosts 2>/dev/null || true")
    run_as_bd(
        ctx,
        f"ssh -p {port} -o BatchMode=yes -o StrictHostKeyChecking=no {ctx.bd_user}@127.0.0.1 true",
    )
    if ctx.cluster_mode and not ctx.is_worker and ctx.v("SSH_KEYSCAN_WORKERS", "yes").lower() in (
        "1", "yes", "true", "on",
    ):
        for wh in ctx.worker_hosts_list():
            run_as_bd(
                ctx,
                f"ssh-keyscan -p {port} -H {wh} >> ~/.ssh/known_hosts 2>/dev/null || true",
                check=False,
            )
        if ctx.worker_hosts_list():
            log("ssh-keyscan: worker host keys appended.")
    log(f"User {ctx.bd_user} and localhost SSH OK.")


def step_jdk(ctx: DeployContext) -> None:
    require_root()
    prepare_install_base(ctx)
    ju = ctx.v("JAVA_USE_SYSTEM", "yes").lower() in ("1", "yes", "true", "on")
    if ju:
        r = subprocess.run(
            ["dnf", "-y", "install", "java-1.8.0-openjdk", "java-1.8.0-openjdk-devel"],
            env=ctx.child_env(),
        )
        if r.returncode != 0:
            die("OpenJDK 8 install failed.")
        java_alt = Path("/etc/alternatives/java").resolve()
        jh = str(java_alt.parent.parent) if java_alt.parent.name == "bin" else detect_java_home()
        Path("/etc/profile.d/bigdata-java.sh").write_text(f"export JAVA_HOME={jh}\n", encoding="utf-8")
        Path("/etc/profile.d/bigdata-java.sh").chmod(0o644)
        log(f"System JDK 8 at {jh}")
        return
    url = ctx.v("JAVA_TARBALL_URL", "").strip()
    if not url:
        die("JAVA_USE_SYSTEM=no requires JAVA_TARBALL_URL")
    ensure_dir(ctx.install_base / "jdk", ctx)
    archive = ctx.download_dir / Path(url).name
    download_file(ctx, url, archive)
    jdk_root = ctx.install_base / "jdk"
    if jdk_root.exists():
        shutil.rmtree(jdk_root)
    jdk_root.mkdir(parents=True)
    extract_tgz(archive, jdk_root)
    jh = str(jdk_root)
    for p in jdk_root.rglob("bin/java"):
        if p.is_file():
            jh = str(p.parent.parent)
            break
    Path("/etc/profile.d/bigdata-java.sh").write_text(f"export JAVA_HOME={jh}\n", encoding="utf-8")
    log(f"Tarball JDK at {jh}")


def _java_home(ctx: DeployContext) -> str:
    p = Path("/etc/profile.d/bigdata-java.sh")
    if p.is_file():
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("export JAVA_HOME="):
                return line.split("=", 1)[1].strip().strip('"')
    return detect_java_home()


# ---------------------------------------------------------------------------
# ZooKeeper
# ---------------------------------------------------------------------------

def step_zookeeper(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip ZooKeeper.")
        return
    if should_skip_component_install(ctx, get_component("zookeeper")):
        warn("Skip ZooKeeper install — already present (SKIP_IF_INSTALLED=yes).")
        return
    prepare_install_base(ctx)
    ver = ctx.v("ZOOKEEPER_VERSION", "3.6.2")
    name = f"apache-zookeeper-{ver}-bin"
    archive = ctx.download_dir / f"{name}.tar.gz"
    download_file(ctx, apache_url(ctx, f"zookeeper/zookeeper-{ver}/{name}.tar.gz"), archive)
    zh = ctx.install_base / "zookeeper"
    if zh.exists():
        shutil.rmtree(zh)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(zh))
    ensure_dir(ctx.install_base / "zookeeper-data", ctx)
    render_template(ctx.templates_dir / "zookeeper" / "zoo.cfg.template", zh / "conf" / "zoo.cfg", ctx)
    chown_tree(zh, ctx.bd_user, ctx.bd_group)
    chown_tree(ctx.install_base / "zookeeper-data", ctx.bd_user, ctx.bd_group)
    log(f"ZooKeeper {ver} installed under {zh}")


# ---------------------------------------------------------------------------
# Hadoop  (arch-aware: aarch64 tarball URL is the same, but we log arch)
# ---------------------------------------------------------------------------

def _hadoop_unpack(ctx: DeployContext) -> None:
    ver = ctx.v("HADOOP_VERSION", "3.2.0")
    name = f"hadoop-{ver}"
    # Hadoop tarballs for 3.3+ include native libs for both architectures.
    # For older versions with arch-specific builds, override HADOOP_TARBALL_URL.
    url_override = ctx.v("HADOOP_TARBALL_URL", "").strip()
    archive = ctx.download_dir / f"{name}.tar.gz"
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(ctx, apache_url(ctx, f"hadoop/common/hadoop-{ver}/{name}.tar.gz"), archive)
    hh = ctx.hadoop_home
    if hh.exists():
        shutil.rmtree(hh)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(hh))


def _hadoop_render_and_env(ctx: DeployContext, jh: str) -> None:
    hh = ctx.hadoop_home
    hconf = hh / "etc" / "hadoop"
    tpl = ctx.templates_dir / "hadoop"
    render_template(tpl / "core-site.xml.template", hconf / "core-site.xml", ctx)
    render_template(tpl / "hdfs-site.xml.template", hconf / "hdfs-site.xml", ctx)
    render_template(tpl / "mapred-site.xml.template", hconf / "mapred-site.xml", ctx)
    render_template(tpl / "yarn-site.xml.template", hconf / "yarn-site.xml", ctx)
    (hconf / "workers").write_text("\n".join(ctx.hadoop_workers_lines()) + "\n", encoding="utf-8")
    with (hconf / "hadoop-env.sh").open("a", encoding="utf-8") as f:
        f.write("\n# bigdata_deploy\n")
        f.write(f"export JAVA_HOME={jh}\n")
        f.write(f"export HADOOP_HOME={hh}\n")
        f.write(f"export HADOOP_CONF_DIR={hconf}\n")


def step_hadoop(ctx: DeployContext) -> None:
    require_root()
    log(f"Detected architecture: {ctx.arch}")
    if ctx.is_worker:
        if not ctx.cluster_mode:
            die("NODE_ROLE=worker requires CLUSTER_MODE=yes")
        if not ctx.v("CLUSTER_MASTER_HOST", "").strip():
            die("Worker: set CLUSTER_MASTER_HOST.")
        if should_skip_component_install(ctx, get_component("hadoop")):
            warn("Skip Hadoop install — already present.")
            return
        _hadoop_unpack(ctx)
        jh = _java_home(ctx)
        for sub in ("hadoop-data/tmp", "hadoop-data/datanode"):
            ensure_dir(ctx.install_base / sub, ctx)
        _hadoop_render_and_env(ctx, jh)
        chown_tree(ctx.hadoop_home, ctx.bd_user, ctx.bd_group)
        chown_tree(ctx.install_base / "hadoop-data", ctx.bd_user, ctx.bd_group)
        log("Hadoop worker (DN/NM) installed.")
        return

    if should_skip_component_install(ctx, get_component("hadoop")):
        warn("Skip Hadoop install — already present.")
        return

    _hadoop_unpack(ctx)
    jh = _java_home(ctx)
    for sub in ("hadoop-data/tmp", "hadoop-data/namenode", "hadoop-data/datanode"):
        ensure_dir(ctx.install_base / sub, ctx)
    _hadoop_render_and_env(ctx, jh)
    hh = ctx.hadoop_home
    chown_tree(hh, ctx.bd_user, ctx.bd_group)
    chown_tree(ctx.install_base / "hadoop-data", ctx.bd_user, ctx.bd_group)
    marker = ctx.install_base / "hadoop-data" / ".formatted"
    if not marker.is_file():
        log("Formatting HDFS namenode (first run)")
        run_as_bd(
            ctx,
            f"source /etc/profile.d/bigdata-java.sh 2>/dev/null; export JAVA_HOME=${{JAVA_HOME:-{jh}}}; "
            f"{hh}/bin/hdfs namenode -format -force",
        )
        marker.touch()
        run(["chown", f"{ctx.bd_user}:{ctx.bd_group}", str(marker)])
    log(f"Hadoop {ctx.v('HADOOP_VERSION', '3.3.6')} installed (arch={ctx.arch}).")


# ---------------------------------------------------------------------------
# Tez  (for Hive execution engine)
# ---------------------------------------------------------------------------

def step_tez(ctx: DeployContext) -> None:
    """Download + install Tez; configure Hive to use tez execution engine."""
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Tez.")
        return
    if should_skip_component_install(ctx, get_component("tez")):
        warn("Skip Tez install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("TEZ_VERSION", "0.10.0")
    # Tez minimal tarball (no hadoop deps embedded)
    name_minimal = f"apache-tez-{ver}-bin"
    archive_name = f"{name_minimal}.tar.gz"
    url_override = ctx.v("TEZ_TARBALL_URL", "").strip()
    archive = ctx.download_dir / archive_name
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(ctx, apache_url(ctx, f"tez/{ver}/{archive_name}"), archive)

    th = ctx.tez_home
    if th.exists():
        shutil.rmtree(th)
    extract_tgz(archive, ctx.install_base)
    # Archive may extract to apache-tez-VER-bin or tez-VER
    for candidate in (ctx.install_base / name_minimal, ctx.install_base / f"tez-{ver}"):
        if candidate.is_dir():
            shutil.move(str(candidate), str(th))
            break
    else:
        # Fallback: pick the first new tez dir
        for d in ctx.install_base.iterdir():
            if d.is_dir() and "tez" in d.name.lower() and d.name != "tez":
                shutil.move(str(d), str(th))
                break

    if not th.is_dir():
        die(f"Tez extraction failed — {th} not found after unpack.")

    # Upload tez tarball to HDFS so YARN containers can use it
    jh = _java_home(ctx)
    tez_hdfs_dir = "/apps/tez"
    try:
        run_as_bd(
            ctx,
            f"source /etc/profile.d/bigdata-java.sh 2>/dev/null; "
            f"export JAVA_HOME=${{JAVA_HOME:-{jh}}}; "
            f"{ctx.hadoop_home}/bin/hdfs dfs -mkdir -p {tez_hdfs_dir}",
        )
        run_as_bd(
            ctx,
            f"source /etc/profile.d/bigdata-java.sh 2>/dev/null; "
            f"export JAVA_HOME=${{JAVA_HOME:-{jh}}}; "
            f"{ctx.hadoop_home}/bin/hdfs dfs -put -f {archive} {tez_hdfs_dir}/",
        )
        log(f"Uploaded {archive.name} to HDFS {tez_hdfs_dir}/")
    except subprocess.CalledProcessError:
        warn("Could not upload Tez tarball to HDFS — HDFS may not be running yet. "
             "Upload manually: hdfs dfs -put <tez.tar.gz> /apps/tez/")

    # tez-site.xml
    tez_conf = th / "conf"
    tez_conf.mkdir(parents=True, exist_ok=True)
    tez_site = tez_conf / "tez-site.xml"
    tez_site.write_text(f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>tez.lib.uris</name>
    <value>${{fs.defaultFS}}{tez_hdfs_dir}/{archive.name}</value>
  </property>
  <property>
    <name>tez.use.cluster.hadoop-libs</name>
    <value>true</value>
  </property>
</configuration>
""", encoding="utf-8")

    chown_tree(th, ctx.bd_user, ctx.bd_group)
    log(f"Tez {ver} installed under {th}")


# ---------------------------------------------------------------------------
# Hive  (now installs Tez too, switches execution engine to tez)
# ---------------------------------------------------------------------------

def step_hive(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Hive.")
        return
    if should_skip_component_install(ctx, get_component("hive")):
        warn("Skip Hive install — already present.")
        return
    if ctx.v("HIVE_DB_TYPE", "derby").lower() != "derby":
        die("Only derby metastore is automated; set HIVE_DB_TYPE=derby")
    ver = ctx.v("HIVE_VERSION", "3.1.0")
    name = f"apache-hive-{ver}-bin"
    archive = ctx.download_dir / f"{name}.tar.gz"
    download_file(ctx, apache_url(ctx, f"hive/hive-{ver}/{name}.tar.gz"), archive)
    hv = ctx.hive_home
    if hv.exists():
        shutil.rmtree(hv)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(hv))
    jh = _java_home(ctx)

    # Fix guava version conflict with Hadoop
    hive_lib = hv / "lib"
    hadoop_guavas = list((ctx.hadoop_home / "share/hadoop/hdfs/lib").glob("guava-*.jar"))
    if hadoop_guavas:
        for g in hive_lib.glob("guava-*.jar"):
            g.unlink(missing_ok=True)
        shutil.copy2(hadoop_guavas[0], hive_lib / hadoop_guavas[0].name)

    ensure_dir(ctx.install_base / "hive-data" / "metastore_db", ctx)
    render_template(ctx.templates_dir / "hive" / "hive-site.xml.template", hv / "conf" / "hive-site.xml", ctx)

    # hive-env.sh — include Tez classpath
    tez_env = ""
    th = ctx.tez_home
    if th.is_dir():
        tez_env = (
            f"export TEZ_HOME={th}\n"
            f"export TEZ_CONF_DIR={th}/conf\n"
            f"export HADOOP_CLASSPATH=${{TEZ_HOME}}/*:${{TEZ_HOME}}/lib/*:${{TEZ_CONF_DIR}}:${{HADOOP_CLASSPATH}}\n"
        )
    with (hv / "conf" / "hive-env.sh").open("a", encoding="utf-8") as f:
        f.write(f"export JAVA_HOME={jh}\n")
        f.write(f"export HADOOP_HOME={ctx.hadoop_home}\n")
        f.write(f"export HIVE_CONF_DIR={hv}/conf\n")
        f.write(f"export HIVE_HOME={hv}\n")
        if tez_env:
            f.write(tez_env)

    chown_tree(hv, ctx.bd_user, ctx.bd_group)
    chown_tree(ctx.install_base / "hive-data", ctx.bd_user, ctx.bd_group)
    marker = ctx.install_base / "hive-data" / ".schema_inited"
    if not marker.is_file():
        run_as_bd(ctx, f"source {hv}/conf/hive-env.sh; {hv}/bin/schematool -dbType derby -initSchema")
        marker.touch()
        run(["chown", f"{ctx.bd_user}:{ctx.bd_group}", str(marker)])
    log(f"Hive {ver} installed (execution.engine=tez if Tez present).")


# ---------------------------------------------------------------------------
# Scala (prerequisite for Spark development / shell)
# ---------------------------------------------------------------------------

def step_scala(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Scala.")
        return
    if should_skip_component_install(ctx, get_component("scala")):
        warn("Skip Scala install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("SCALA_VERSION", "2.12.13")
    name = f"scala-{ver}"
    archive = ctx.download_dir / f"{name}.tgz"
    url_override = ctx.v("SCALA_TARBALL_URL", "").strip()
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(
            ctx,
            f"https://downloads.lightbend.com/scala/{ver}/{name}.tgz",
            archive,
        )
    sh = ctx.scala_home
    if sh.exists():
        shutil.rmtree(sh)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(sh))
    chown_tree(sh, ctx.bd_user, ctx.bd_group)

    # /etc/profile.d
    prof = Path("/etc/profile.d/bigdata-scala.sh")
    prof.write_text(
        f"export SCALA_HOME={sh}\nexport PATH=${{SCALA_HOME}}/bin:${{PATH}}\n",
        encoding="utf-8",
    )
    prof.chmod(0o644)
    log(f"Scala {ver} installed under {sh}")


# ---------------------------------------------------------------------------
# Spark  (arch-aware for -without-hadoop builds on aarch64)
# ---------------------------------------------------------------------------

def step_spark(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Spark.")
        return
    if should_skip_component_install(ctx, get_component("spark")):
        warn("Skip Spark install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("SPARK_VERSION", "3.3.1")
    prof = ctx.v("SPARK_HADOOP_PROFILE", "hadoop3")
    name = f"spark-{ver}-bin-{prof}"
    archive = ctx.download_dir / f"{name}.tgz"
    url_override = ctx.v("SPARK_TARBALL_URL", "").strip()
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(ctx, apache_url(ctx, f"spark/spark-{ver}/{name}.tgz"), archive)
    sp = ctx.spark_home
    if sp.exists():
        shutil.rmtree(sp)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(sp))
    jh = _java_home(ctx)
    hconf = ctx.hadoop_home / "etc" / "hadoop"
    scala_env = ""
    if ctx.scala_home.is_dir():
        scala_env = f"export SCALA_HOME={ctx.scala_home}\n"
    lines = (
        f"export JAVA_HOME={jh}\n"
        f"export HADOOP_CONF_DIR={hconf}\n"
        f"export SPARK_DIST_CLASSPATH=$({ctx.hadoop_home}/bin/hadoop classpath)\n"
        f"{scala_env}"
    )
    (sp / "conf" / "spark-env.sh").write_text(lines, encoding="utf-8")
    (sp / "conf" / "spark-env.sh").chmod(0o755)
    chown_tree(sp, ctx.bd_user, ctx.bd_group)
    log(f"Spark {ver} installed (arch={ctx.arch}).")


# ---------------------------------------------------------------------------
# HBase
# ---------------------------------------------------------------------------

def step_hbase(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip HBase.")
        return
    if should_skip_component_install(ctx, get_component("hbase")):
        warn("Skip HBase install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("HBASE_VERSION", "2.2.3")
    name = f"hbase-{ver}"
    archive_name = f"{name}-bin.tar.gz"
    archive = ctx.download_dir / archive_name
    url_override = ctx.v("HBASE_TARBALL_URL", "").strip()
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(ctx, apache_url(ctx, f"hbase/{ver}/{archive_name}"), archive)
    hb = ctx.hbase_home
    if hb.exists():
        shutil.rmtree(hb)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(hb))
    jh = _java_home(ctx)
    render_template(ctx.templates_dir / "hbase" / "hbase-site.xml.template", hb / "conf" / "hbase-site.xml", ctx)
    (hb / "conf" / "regionservers").write_text(
        "\n".join(ctx.region_server_hosts()) + "\n", encoding="utf-8",
    )
    with (hb / "conf" / "hbase-env.sh").open("a", encoding="utf-8") as f:
        f.write(f"export JAVA_HOME={jh}\nexport HBASE_MANAGES_ZK=false\n")
    chown_tree(hb, ctx.bd_user, ctx.bd_group)
    log(f"HBase {ver} installed (arch={ctx.arch}).")


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def step_kafka(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Kafka.")
        return
    if should_skip_component_install(ctx, get_component("kafka")):
        warn("Skip Kafka install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("KAFKA_VERSION", "2.8.1")
    scala = ctx.v("KAFKA_SCALA_VERSION", "2.13")
    name = f"kafka_{scala}-{ver}"
    archive = ctx.download_dir / f"{name}.tgz"
    download_file(ctx, apache_url(ctx, f"kafka/{ver}/{name}.tgz"), archive)
    kh = ctx.kafka_home
    if kh.exists():
        shutil.rmtree(kh)
    extract_tgz(archive, ctx.install_base)
    shutil.move(str(ctx.install_base / name), str(kh))
    ensure_dir(ctx.install_base / "kafka-logs", ctx)
    render_template(ctx.templates_dir / "kafka" / "server.properties.template", kh / "config" / "server.properties", ctx)
    chown_tree(kh, ctx.bd_user, ctx.bd_group)
    chown_tree(ctx.install_base / "kafka-logs", ctx.bd_user, ctx.bd_group)
    log(f"Kafka {ver} installed.")


# ---------------------------------------------------------------------------
# Flink  (arch-aware: aarch64 builds available for Flink 1.17+)
# ---------------------------------------------------------------------------

def step_flink(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        log("NODE_ROLE=worker: skip Flink.")
        return
    if should_skip_component_install(ctx, get_component("flink")):
        warn("Skip Flink install — already present.")
        return
    prepare_install_base(ctx)
    ver = ctx.v("FLINK_VERSION", "1.15.0")
    scala = ctx.v("FLINK_SCALA_VERSION", "2.12")
    name = f"flink-{ver}-bin-scala_{scala}"
    archive = ctx.download_dir / f"{name}.tgz"
    url_override = ctx.v("FLINK_TARBALL_URL", "").strip()
    if url_override:
        download_file(ctx, url_override, archive)
    else:
        download_file(ctx, apache_url(ctx, f"flink/flink-{ver}/{name}.tgz"), archive)
    fh = ctx.flink_home
    if fh.exists():
        shutil.rmtree(fh)
    extract_tgz(archive, ctx.install_base)
    # Flink may extract as flink-VER or flink-VER-bin-scala_X
    for candidate in (ctx.install_base / f"flink-{ver}", ctx.install_base / name):
        if candidate.is_dir():
            shutil.move(str(candidate), str(fh))
            break
    conf = fh / "conf" / "flink-conf.yaml"
    if conf.is_file():
        text = conf.read_text(encoding="utf-8", errors="replace")
        if "bigdata_deploy" not in text:
            tmp = Path("/tmp/bigdata-flink-snippet.yaml")
            render_template(ctx.templates_dir / "flink" / "flink-conf.yaml.snippet", tmp, ctx)
            snippet = tmp.read_text(encoding="utf-8")
            tmp.unlink(missing_ok=True)
            with conf.open("a", encoding="utf-8") as f:
                f.write("\n# --- bigdata_deploy ---\n")
                f.write(snippet)
    chown_tree(fh, ctx.bd_user, ctx.bd_group)
    log(f"Flink {ver} installed (arch={ctx.arch}).")


# ---------------------------------------------------------------------------
# Profile & Verify
# ---------------------------------------------------------------------------

def _write_stack_profile(ctx: DeployContext, jh: str) -> None:
    f = Path("/etc/profile.d/bigdata-stack.sh")
    scala_line = ""
    if ctx.scala_home.is_dir():
        scala_line = f"export SCALA_HOME={ctx.scala_home}\n"
    tez_line = ""
    if ctx.tez_home.is_dir():
        tez_line = (
            f"export TEZ_HOME={ctx.tez_home}\n"
            f"export TEZ_CONF_DIR={ctx.tez_home}/conf\n"
        )
    body = f"""export JAVA_HOME={jh}
export HADOOP_HOME={ctx.hadoop_home}
export HADOOP_CONF_DIR={ctx.hadoop_home}/etc/hadoop
export HIVE_HOME={ctx.hive_home}
export HIVE_CONF_DIR={ctx.hive_home}/conf
{tez_line}export SPARK_HOME={ctx.spark_home}
{scala_line}export HBASE_HOME={ctx.hbase_home}
export KAFKA_HOME={ctx.kafka_home}
export FLINK_HOME={ctx.flink_home}
export ZOOKEEPER_HOME={ctx.zookeeper_home}
export PATH=${{HADOOP_HOME}}/bin:${{HADOOP_HOME}}/sbin:${{HIVE_HOME}}/bin:${{SPARK_HOME}}/bin:${{HBASE_HOME}}/bin:${{KAFKA_HOME}}/bin:${{FLINK_HOME}}/bin:${{ZOOKEEPER_HOME}}/bin:${{PATH}}
"""
    f.write_text(body, encoding="utf-8")
    f.chmod(0o644)


def step_verify_spark(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        die("verify-spark must run on the master node.")
    jh = _java_home(ctx)
    prof = Path("/etc/profile.d/bigdata-spark-verify.sh")
    prof.write_text(
        f"export JAVA_HOME={jh}\n"
        f"export HADOOP_HOME={ctx.hadoop_home}\n"
        f"export HADOOP_CONF_DIR={ctx.hadoop_home}/etc/hadoop\n"
        f"export SPARK_HOME={ctx.spark_home}\n"
        f"export PATH=${{HADOOP_HOME}}/bin:${{HADOOP_HOME}}/sbin:${{SPARK_HOME}}/bin:${{PATH}}\n",
        encoding="utf-8",
    )
    prof.chmod(0o644)
    fail = False

    def ok(m: str) -> None:
        log(f"OK  {m}")

    def bad(m: str) -> None:
        nonlocal fail
        log(f"FAIL {m}")
        fail = True

    ps = f"source {prof}"
    try:
        run_as_bd(ctx, f"{ps}; {ctx.zookeeper_home}/bin/zkServer.sh start")
    except subprocess.CalledProcessError:
        bad("ZooKeeper start")
    time.sleep(2)
    p = subprocess.run(
        ["bash", "-lc", f"echo ruok | nc -w 2 127.0.0.1 {ctx.v('ZK_CLIENT_PORT', '2181')}"],
        capture_output=True, text=True, env=ctx.child_env(),
    )
    if "imok" in (p.stdout or ""):
        ok("ZooKeeper imok")
    else:
        bad("ZooKeeper ruok")
    try:
        run_as_bd(ctx, f"{ps}; {ctx.hadoop_home}/sbin/start-dfs.sh")
        run_as_bd(ctx, f"{ps}; {ctx.hadoop_home}/sbin/start-yarn.sh")
    except subprocess.CalledProcessError:
        bad("HDFS/YARN start")
    time.sleep(5)
    try:
        run_as_bd(ctx, f"{ps}; {ctx.hadoop_home}/bin/hdfs dfs -mkdir -p /tmp /tmp/spark-verify")
        ok("HDFS mkdir")
    except subprocess.CalledProcessError:
        bad("HDFS mkdir")
    try:
        run_as_bd(
            ctx,
            f"{ps}; {ctx.hadoop_home}/bin/hdfs dfs -put -f {ctx.hadoop_home}/LICENSE.txt /tmp/LICENSE.spark-verify",
        )
        ok("HDFS put")
    except subprocess.CalledProcessError:
        bad("HDFS put")
    try:
        run_as_bd(ctx, f"{ps}; {ctx.spark_home}/bin/spark-submit --version")
        ok("spark-submit --version")
    except subprocess.CalledProcessError:
        bad("spark-submit --version")
    jars = glob.glob(str(ctx.spark_home / "examples" / "jars" / "spark-examples*.jar"))
    if not jars:
        bad("Spark examples jar missing")
    else:
        try:
            run_as_bd(
                ctx,
                f'{ps}; {ctx.spark_home}/bin/spark-submit --master local[2] --class org.apache.spark.examples.SparkPi '
                f'"{jars[0]}" 20',
            )
            ok("Spark Pi")
        except subprocess.CalledProcessError:
            bad("Spark Pi")
    if fail:
        die("Spark verification had failures.")
    log("Spark stack verification passed.")


def step_verify_full(ctx: DeployContext) -> None:
    require_root()
    if ctx.is_worker:
        die("verify must run on the master node.")
    jh = _java_home(ctx)
    _write_stack_profile(ctx, jh)
    ps = "source /etc/profile.d/bigdata-stack.sh"
    fail = False

    def ok(m: str) -> None:
        log(f"OK  {m}")

    def bad(m: str) -> None:
        nonlocal fail
        log(f"FAIL {m}")
        fail = True

    try:
        run_as_bd(ctx, f"{ps}; {ctx.zookeeper_home}/bin/zkServer.sh start")
    except subprocess.CalledProcessError:
        bad("ZooKeeper start")
    time.sleep(2)
    p = subprocess.run(
        ["bash", "-lc", f"echo ruok | nc -w 2 127.0.0.1 {ctx.v('ZK_CLIENT_PORT', '2181')}"],
        capture_output=True, text=True,
    )
    if "imok" in (p.stdout or ""):
        ok("ZooKeeper imok")
    else:
        bad("ZooKeeper ruok")
    run_as_bd(ctx, f"{ps}; {ctx.hadoop_home}/sbin/start-dfs.sh", check=False)
    run_as_bd(ctx, f"{ps}; {ctx.hadoop_home}/sbin/start-yarn.sh", check=False)
    time.sleep(4)
    try:
        run_as_bd(
            ctx,
            f"{ps}; {ctx.hadoop_home}/bin/hdfs dfs -mkdir -p /tmp /tmp/hive /user/hive/warehouse /hbase && "
            f"{ctx.hadoop_home}/bin/hdfs dfs -chmod 777 /tmp/hive",
        )
        ok("HDFS mkdir")
    except subprocess.CalledProcessError:
        bad("HDFS mkdir")
    try:
        run_as_bd(
            ctx,
            f"{ps}; {ctx.hadoop_home}/bin/hdfs dfs -put -f {ctx.hadoop_home}/LICENSE.txt /tmp/LICENSE.verify",
        )
        ok("HDFS put")
    except subprocess.CalledProcessError:
        bad("HDFS put")

    # Verify: Hive → Spark → HBase → Kafka → Flink
    try:
        run_as_bd(
            ctx,
            f"{ps}; source {ctx.hive_home}/conf/hive-env.sh; {ctx.hive_home}/bin/hive -e 'show databases;' 2>/dev/null",
        )
        ok("Hive CLI")
    except subprocess.CalledProcessError:
        bad("Hive CLI")

    try:
        run_as_bd(ctx, f"{ps}; {ctx.spark_home}/bin/spark-submit --version")
        ok("Spark version")
    except subprocess.CalledProcessError:
        bad("Spark version")
    jars = glob.glob(str(ctx.spark_home / "examples" / "jars" / "spark-examples*.jar"))
    if jars:
        try:
            run_as_bd(
                ctx,
                f'{ps}; {ctx.spark_home}/bin/spark-submit --master local[1] --class org.apache.spark.examples.SparkPi '
                f'"{jars[0]}" 10',
            )
            ok("Spark Pi")
        except subprocess.CalledProcessError:
            bad("Spark Pi")
    else:
        bad("Spark examples jar missing")

    try:
        run_as_bd(ctx, f"{ps}; {ctx.hbase_home}/bin/start-hbase.sh")
    except subprocess.CalledProcessError:
        bad("HBase start")
    time.sleep(5)
    try:
        run_as_bd(ctx, f"{ps}; echo 'list' | {ctx.hbase_home}/bin/hbase shell 2>/dev/null | head -5")
        ok("HBase shell")
    except subprocess.CalledProcessError:
        bad("HBase shell")

    run_as_bd(ctx, "pkill -f '[k]afka.Kafka' 2>/dev/null || true", check=False)
    time.sleep(2)
    run_as_bd(
        ctx,
        f"nohup {ctx.kafka_home}/bin/kafka-server-start.sh {ctx.kafka_home}/config/server.properties "
        f">/tmp/kafka-server.log 2>&1 &",
        check=False,
    )
    time.sleep(8)
    kp = ctx.v("KAFKA_PORT", "9092")
    khost = ctx.master_host() if ctx.cluster_mode else "127.0.0.1"
    try:
        run_as_bd(ctx, f"{ps}; {ctx.kafka_home}/bin/kafka-topics.sh --bootstrap-server {khost}:{kp} --list")
        ok("Kafka topics list")
    except subprocess.CalledProcessError:
        bad("Kafka topics list")
    try:
        run_as_bd(
            ctx,
            f"{ps}; {ctx.kafka_home}/bin/kafka-topics.sh --bootstrap-server {khost}:{kp} "
            f"--create --topic verify-topic --partitions 1 --replication-factor 1 --if-not-exists",
        )
        ok("Kafka create topic")
    except subprocess.CalledProcessError:
        bad("Kafka create topic")

    run_as_bd(ctx, f"{ps}; {ctx.flink_home}/bin/stop-cluster.sh 2>/dev/null || true", check=False)
    run_as_bd(ctx, f"{ps}; {ctx.flink_home}/bin/start-cluster.sh", check=False)
    time.sleep(5)
    fw = ctx.v("FLINK_WEB_PORT", "8081")
    curl = subprocess.run(
        ["curl", "-sf", f"http://127.0.0.1:{fw}/overview"],
        capture_output=True, env=ctx.child_env(),
    )
    if curl.returncode == 0:
        ok("Flink REST /overview")
    else:
        bad("Flink REST /overview")

    if fail:
        die("One or more checks failed (see FAIL lines above).")
    log("All verification checks passed.")
