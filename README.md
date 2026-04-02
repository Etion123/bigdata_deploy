# bigdata_deploy

在 **openEuler / RHEL 系**上自动安装 **ZooKeeper、Hadoop、Hive、HBase、Kafka、Spark、Flink**。支持 **单机伪分布式** 与 **1 主 + N 从** 的 HDFS/YARN 集群。实现为 **Python 3 标准库**（`urllib` 下载，无需 `pip`）。

## 环境要求

- **操作系统**：openEuler 22.03 SP4 或兼容的 RHEL 系（`dnf`、`systemd`）。
- **Python**：`python3`（建议 3.8+）。
- **权限**：安装与校验需 **root**（`sudo`）。
- **资源**：内存建议 ≥8GB（YARN 模板默认 NodeManager 8GB，可按节点在 `templates/hadoop/yarn-site.xml.template` 中调小）。

## 如何调用

在克隆后的**仓库根目录**执行（与 `install.py` 同级）：

```bash
sudo python3 install.py [选项] [阶段]
```

### 常用示例

| 场景 | 命令 |
|------|------|
| 单机全流程 + 校验 | `sudo python3 install.py all` |
| 只装到 Spark 并校验 | `sudo python3 install.py to-spark` |
| 仅完整校验 | `sudo python3 install.py verify` |
| 仅 Spark 栈校验 | `sudo python3 install.py verify-spark` |
| 指定配置文件 | `sudo python3 install.py -c /path/to/deploy.conf all` |
| 离线包清单 | `python3 install.py list-bundles` |
| **集群从节点**（见下文） | `sudo python3 install.py cluster-worker` |

配置文件默认：`./config/deploy.conf`。也可用环境变量：

```bash
export CONFIG_FILE=/path/to/deploy.conf
sudo -E python3 install.py all
```

### 安装阶段（phase）

| 阶段 | 说明 |
|------|------|
| `all` | repo → 可选数据盘 → SSH → JDK → ZK → Hadoop → Hive → HBase → Kafka → Spark → Flink → 全量校验（**仅主节点**） |
| `to-spark` | 到 Spark + Spark 校验（**仅主节点**） |
| `cluster-worker` | repo → disk → SSH → JDK → Hadoop（**仅 DataNode/NodeManager**；需 `CLUSTER_MODE=yes` 且 `NODE_ROLE=worker`） |
| `verify` / `verify-spark` | 仅校验（**仅主节点**） |
| `repo` / `disk` / `ssh` / `jdk` / `zk` / `hadoop` / … | 单步执行 |

```bash
python3 install.py -h
```

---

## 集群模式（1 主 + N 从）

适用于 **一个 NameNode / ResourceManager 节点** + **多个 DataNode / NodeManager 节点**。ZooKeeper、Hive、HBase Master、Kafka、Spark、Flink **仍只在主节点安装**（与常见小规模集群一致）。HBase 在 `CLUSTER_MODE=yes` 时默认 **分布式**，并生成 `conf/regionservers`。

### 1. 配置 `deploy.conf`（主、从共用一份，仅 `NODE_ROLE` 不同）

| 变量 | 说明 |
|------|------|
| `CLUSTER_MODE=yes` | 开启集群逻辑 |
| `CLUSTER_MASTER_HOST` | 主节点 FQDN（与 NameNode 一致）。主上可留空，脚本会用本机 `hostname -f` |
| `WORKER_HOSTS` | 从节点 FQDN，空格或逗号分隔；**每台机器上的值应一致**（与 Hadoop `workers` 文件一致） |
| `MASTER_AS_DATANODE` | `yes`：主节点也跑 DN/NM；`no`：仅 `WORKER_HOSTS` 上的节点跑 DN/NM |
| `HDFS_REPLICATION` | 留空则自动 `min(3, DataNode 数)` |
| `NODE_ROLE` | 主节点：`master`；从节点：`worker` |
| `SSH_KEYSCAN_WORKERS` | 主节点上对 `WORKER_HOSTS` 做 `ssh-keyscan`，写入 `hadoop` 用户的 `known_hosts` |

### 2. 安装顺序

1. **主节点**：`NODE_ROLE=master`，执行  
   `sudo python3 install.py all`（或分步装到 Hadoop 后再装其余组件）。
2. **从节点**：将**同一份** `deploy.conf` 改为 `NODE_ROLE=worker`，并**必须**设置 `CLUSTER_MASTER_HOST=<主节点 FQDN>`，执行  
   `sudo python3 install.py cluster-worker`。

### 3. SSH（必做）

Hadoop 的 `start-dfs.sh` / `start-yarn.sh` 会通过 SSH 到 `workers` 里的主机启动 DN/NM。需要：

- 各节点 `hadoop` 用户已创建（脚本会建）；
- **把主节点**上 `hadoop` 用户的 `~/.ssh/id_rsa.pub` **追加到每个从节点**的 `~/.ssh/authorized_keys`（权限 `600` / `700`）。

脚本在主节点可选执行 `ssh-keyscan` 写入 `known_hosts`，**不会**自动分发私钥。

### 4. 启动与验证

在主节点（`hadoop` 用户）：

```bash
source /etc/profile.d/bigdata-stack.sh   # 或 bigdata-java + 自行 export
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
```

Web：NameNode `http://<master>:9870`，YARN `http://<master>:8088`。

### 5. 限制说明

- **ZooKeeper**：当前模板为**单节点**（跑在主节点）；从节点组件通过 `CLUSTER_MASTER_HOST` 访问 ZK。
- **Flink**：仍按单机 JobManager 模板装主节点；多 TaskManager 需自行扩展。
- **防火墙**：主从之间需放行 HDFS/YARN 及组件端口（如 9000、9870、8088、2181 等）。

---

## 配置文件 `config/deploy.conf`

- **键值格式**：`KEY=value`，`#` 注释；支持 `${VAR}` 展开。
- **安装根目录**：默认 `INSTALL_BASE=/usr/local/bigdata`。
- **版本号**：与 Apache 下载文件名一致；改版本后需更新 `downloads` 下包或重新在线下载。

### 代理与离线

- `HTTP_PROXY` / `HTTPS_PROXY` / `PKG_PROXY`：作用于 `dnf` 与下载。
- `OFFLINE_MODE=yes`：不下载；包放到 `INSTALL_BASE/downloads`，名称与 `python3 install.py list-bundles` 一致。从节点在 `NODE_ROLE=worker` 时清单**仅含 Hadoop** tarball。
- 离线仍需要可访问的 **dnf 本地源**（`LOCAL_REPO_FILE` 等）以安装 `openjdk` 与基础工具。

### 可选数据盘

`AUTO_MOUNT_DATA_DISK=yes` 会格式化并挂载空闲盘到 `DATA_MOUNT_POINT`，**数据将清空**。

---

## 离线部署简要流程

1. 准备与 `deploy.conf` 版本一致的各组件包（或内网镜像）。
2. `OFFLINE_MODE=yes`，配置 `LOCAL_REPO_FILE`（及 JDK）。
3. 包放入 `INSTALL_BASE/downloads`，执行 `python3 install.py list-bundles` 核对。
4. 主：`sudo python3 install.py all`；从：`NODE_ROLE=worker` + `sudo python3 install.py cluster-worker`。

---

## 仓库目录

```
install.py
bigdata_deploy/       # Python 包
config/deploy.conf
config/examples/
templates/            # XML / properties / yaml 片段
```

## 说明与免责

- 集群场景请在测试环境验证后再用于生产；注意备份与 HDFS 格式化风险。
- 组件版权归各开源项目；请遵守许可与安全规范。
