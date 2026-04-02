# bigdata_deploy

在 **openEuler / RHEL 系**上自动安装 **ZooKeeper、Hadoop、Hive、Spark、HBase、Kafka、Flink**。支持 **单机伪分布式** 与 **1 主 + N 从** HDFS/YARN 集群。安装器为 **Python 3.8+**，默认 **仅标准库**（`urllib` 下载）；`requirements.txt` 中无强制 pip 依赖。

## 环境要求

- **操作系统**：openEuler 22.03 SP4 或兼容 RHEL 系（`dnf`、`systemd`）。
- **Python**：3.8+（`install.py` 会预检）。
- **权限**：安装与校验需 **root**（`sudo`）。
- **资源**：内存建议 ≥8GB（YARN 默认 NodeManager 偏大，可在 `templates/hadoop/yarn-site.xml.template` 调小）。

## 默认安装顺序

1. **preflight**（预检：Python 版本、磁盘、`INSTALL_BASE`、已有组件扫描）  
2. **repo** → **disk**（可选）→ **ssh** → **jdk**  
3. **ZooKeeper** → **Hadoop** → **Hive** → **Spark** → **HBase** → **Kafka** → **Flink**  
4. **verify**（全量校验；顺序与上面对齐：先 Hive / Spark，再 HBase、Kafka、Flink）

## 如何调用

在仓库根目录（与 `install.py` 同级）：

```bash
sudo python3 install.py [选项] [阶段]
```

| 场景 | 命令 |
|------|------|
| 全流程 + 校验 | `sudo python3 install.py all` |
| 仅预检 | `sudo python3 install.py preflight` |
| 装到 Spark + 校验 | `sudo python3 install.py to-spark` |
| 仅完整校验 / Spark 校验 | `sudo python3 install.py verify` / `verify-spark` |
| 指定配置 | `sudo python3 install.py -c /path/to/deploy.conf all` |
| 离线包清单 | `python3 install.py list-bundles` |
| 集群从节点 | `sudo python3 install.py cluster-worker` |

```bash
export CONFIG_FILE=/path/to/deploy.conf
sudo -E python3 install.py all
```

### 阶段（phase）一览

| 阶段 | 说明 |
|------|------|
| `all` | preflight + 基础步骤 + **ZK → Hadoop → Hive → Spark → HBase → Kafka → Flink** + verify（仅主节点） |
| `preflight` | 仅预检 |
| `to-spark` | preflight + … + ZK + Hadoop + Spark + verify-spark |
| `cluster-worker` | preflight + repo + disk + ssh + jdk + Hadoop（仅 DN/NM；需 `CLUSTER_MODE=yes` 且 `NODE_ROLE=worker`） |
| `verify` / `verify-spark` | 仅校验（主节点） |
| `repo` / `disk` / `ssh` / `jdk` / `zk` / `hadoop` / … | 单步 |

### 安装前预检与「已存在则跳过」

- **`preflight`**：检查 root、Python 版本、磁盘（可选）、`INSTALL_BASE` 可写，并列出各组件目录是否已存在。
- **`SKIP_IF_INSTALLED=yes`**（默认）：若某组件在 `INSTALL_BASE` 下已存在约定标记文件（如 `zookeeper/bin/zkServer.sh`），则**跳过该组件安装**并输出 WARN，避免覆盖已有环境。

若需强制重装，可先删除对应目录或设 `SKIP_IF_INSTALLED=no`。

### 依赖文件 `requirements.txt`

生产安装 **不需要** `pip install`。文件内仅说明可选开发依赖（如 `pytest`）；保持与仓库同步即可。

---

## 集群模式（1 主 + N 从）

适用于 **一个 NameNode / ResourceManager** + **多个 DataNode / NodeManager**。ZooKeeper、Hive、HBase Master、Kafka、Spark、Flink **默认只装在主节点**。HBase 在 `CLUSTER_MODE=yes` 时可分布式，并生成 `conf/regionservers`。

### 配置要点（`deploy.conf`）

| 变量 | 说明 |
|------|------|
| `CLUSTER_MODE=yes` | 开启集群逻辑 |
| `CLUSTER_MASTER_HOST` | 主 FQDN；主节点可留空；**从节点必填** |
| `WORKER_HOSTS` | 从节点 FQDN（与 Hadoop `workers` 一致） |
| `MASTER_AS_DATANODE` | 主是否跑 DN/NM |
| `HDFS_REPLICATION` | 留空则自动 `min(3, DataNode 数)` |
| `NODE_ROLE` | `master` / `worker` |
| `SSH_KEYSCAN_WORKERS` | 主节点对 worker 做 `ssh-keyscan` |

### 步骤

1. **主节点**：`NODE_ROLE=master`，`sudo python3 install.py all`  
2. **从节点**：同一份配置改为 `NODE_ROLE=worker` 且 **`CLUSTER_MASTER_HOST` 已设**，`sudo python3 install.py cluster-worker`  
3. **SSH**：将主节点 `hadoop` 用户 `id_rsa.pub` 追加到各从节点 `authorized_keys`（脚本不代发私钥）。

### 限制

- ZK 为单节点（跑在主节点）；Flink 为单机 JobManager 模板；多 TM 需自行扩展。
- 从节点与主节点之间需放行相关端口。

---

## 配置文件 `config/deploy.conf`

- **格式**：`KEY=value`，支持 `#` 与 `${VAR}` 展开。
- **路径**：默认 `INSTALL_BASE=/usr/local/bigdata`。
- **版本**：与 Apache 下载文件名一致。

### 代理与离线

- `HTTP_PROXY` / `HTTPS_PROXY` / `PKG_PROXY`：作用于 `dnf` 与下载。
- `OFFLINE_MODE=yes`：不下载；包放到 `INSTALL_BASE/downloads`，名称与 `list-bundles` 一致。从节点在 `NODE_ROLE=worker` 时清单**仅含 Hadoop** tarball。
- 离线仍需 **dnf 本地源**（`LOCAL_REPO_FILE` 等）以安装 `openjdk` 与基础工具。

### 可选数据盘

`AUTO_MOUNT_DATA_DISK=yes` 会格式化并挂载空闲盘，**数据将清空**。

---

## 离线部署简要流程

1. 准备与 `deploy.conf` 版本一致的各组件包。  
2. `OFFLINE_MODE=yes`，配置本地 `dnf` 源。  
3. 包放入 `downloads`，`python3 install.py list-bundles` 核对。  
4. `sudo python3 install.py all`（主）或 `cluster-worker`（从）。

---

## 仓库结构

```
install.py
requirements.txt
bigdata_deploy/         # 包：preflight、components、steps、util、…
config/deploy.conf
config/examples/
templates/
```

## 说明与免责

- 生产与集群场景请在测试环境验证；HDFS 格式化与分区盘操作有风险。
- 组件版权归各开源项目；请遵守许可与安全规范。
