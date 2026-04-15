# bigdata_deploy

在 **openEuler / RHEL 系**上自动安装大数据组件栈，支持 **x86_64** 和 **aarch64 (ARM)** 两种架构。

**组件**：ZooKeeper、Hadoop、Tez、Hive、Scala、Spark、HBase、Kafka、Flink  
**模式**：单机伪分布式 / 1 主 + N 从集群  
**运行时依赖**：Python 3.8+ 标准库（无需 pip）

## 环境要求

| 项目 | 要求 |
|------|------|
| 操作系统 | openEuler 22.03 SP4 或兼容 RHEL 系（`dnf`、`systemd`） |
| 架构 | **x86_64** 或 **aarch64**（自动检测，可用 `ARCH=` 覆盖） |
| Python | 3.8+（`install.py` 预检） |
| 权限 | 安装与校验需 **root**（`sudo`） |
| 内存 | 建议 ≥ 8 GB |

## 默认安装顺序

1. **preflight**（预检：架构、Python 版本、磁盘、已有组件扫描）
2. **repo** → **disk**（可选）→ **ssh** → **jdk**
3. **ZooKeeper** → **Hadoop** → **Tez** → **Hive** → **Scala** → **Spark** → **HBase** → **Kafka** → **Flink**
4. **verify**（全量校验）

## 如何调用

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

### 阶段（phase）一览

| 阶段 | 说明 |
|------|------|
| `all` | 全流程 + verify（仅主节点） |
| `preflight` | 仅预检 |
| `to-spark` | 到 Spark + verify-spark |
| `cluster-worker` | repo + disk + ssh + jdk + Hadoop（仅 DN/NM） |
| `verify` / `verify-spark` | 仅校验 |
| `repo` / `disk` / `ssh` / `jdk` / `zk` / `hadoop` / `tez` / `hive` / `scala` / `spark` / `hbase` / `kafka` / `flink` | 单步 |

## 架构支持（x86_64 / aarch64）

脚本启动时自动通过 `platform.machine()` 检测 CPU 架构，并在 preflight 输出。

- **纯 Java 组件**（ZooKeeper、Hive、Tez、HBase、Kafka）的 Apache 官方 tarball 与架构无关，同一个包即可。
- **含 native 代码的组件**（Hadoop、Spark、Flink）：
  - Hadoop / Spark / Flink 若你使用的是带 native 依赖的特定构建（尤其 ARM 场景），建议在 `deploy.conf` 设置 `*_TARBALL_URL` 指向该构建包；脚本会优先下载该 URL。
  - **Spark / Flink** 官方预编译包为纯 Java，跨架构可用。
  - 若使用第三方或自编译的特定架构包，在 `deploy.conf` 中设置对应的 `*_TARBALL_URL`（如 `HADOOP_TARBALL_URL`、`SPARK_TARBALL_URL`、`FLINK_TARBALL_URL`），脚本会优先下载该 URL。

在 `deploy.conf` 中可强制指定：

```
ARCH="aarch64"
```

## 新增：Tez（Hive 执行引擎）

- `step_tez` 下载 Tez tarball，解压到 `INSTALL_BASE/tez`，生成 `tez-site.xml`，并上传 tarball 至 HDFS `/apps/tez/`。
- Hive 模板中 `hive.execution.engine` 已改为 **`tez`**（替代原先的 `mr`），`tez.lib.uris` 指向 HDFS 上的 Tez 包。
- Hive 的 `hive-env.sh` 自动追加 `TEZ_HOME` 和 `HADOOP_CLASSPATH`。
- 版本通过 `TEZ_VERSION` 控制（默认 `0.10.0`）。

## 新增：Scala（Spark 前置）

- `step_scala` 下载并解压 Scala 到 `INSTALL_BASE/scala`，写入 `/etc/profile.d/bigdata-scala.sh`。
- Spark 的 `spark-env.sh` 自动设置 `SCALA_HOME`。
- 版本通过 `SCALA_VERSION` 控制（默认 `2.12.13`）。

## 安装前预检与「已存在则跳过」

- **`preflight`**：检查 root、架构、Python 版本、磁盘、各组件目录。
- **`SKIP_IF_INSTALLED=yes`**（默认）：组件标记文件存在则跳过，避免覆盖。
- 强制重装：删除对应目录或设 `SKIP_IF_INSTALLED=no`。

---

## 集群模式（1 主 + N 从）

| 变量 | 说明 |
|------|------|
| `CLUSTER_MODE=yes` | 开启集群 |
| `CLUSTER_MASTER_HOST` | 主 FQDN；从节点必填 |
| `WORKER_HOSTS` | 从节点 FQDN |
| `NODE_ROLE` | `master` / `worker` |

1. **主节点**：`sudo python3 install.py all`
2. **从节点**：`NODE_ROLE=worker` + `CLUSTER_MASTER_HOST` 已设 → `sudo python3 install.py cluster-worker`
3. **SSH**：将主节点 `hadoop` 用户公钥追加到各从节点。

---

## 配置文件 `config/deploy.conf`

- 格式：`KEY=value`，支持 `#` 注释与 `${VAR}` 展开。
- 所有 `*_TARBALL_URL` 变量为空时使用 Apache 官方下载地址；填写则优先用自定义 URL（适用于内网/ARM 特定构建）。

### 关键版本配置（保持原组件版本不变；仅新增 Tez/Scala）

```
ZOOKEEPER_VERSION="3.6.2"
HADOOP_VERSION="3.2.0"
HIVE_VERSION="3.1.0"
SPARK_VERSION="3.3.1"
HBASE_VERSION="2.2.3"
KAFKA_VERSION="2.8.1"
FLINK_VERSION="1.15.0"
TEZ_VERSION="0.10.0"
SCALA_VERSION="2.12.13"
```

### 离线 / 代理

- `OFFLINE_MODE=yes`：不下载，包放 `downloads` 目录。
- `HTTP_PROXY` / `HTTPS_PROXY`：作用于 `dnf` 和下载。

---

## 仓库结构

```
install.py
requirements.txt
bigdata_deploy/         # preflight、components、steps、util、context、…
config/deploy.conf
config/examples/
templates/              # XML / properties / yaml 模板
```

## 说明与免责

- 生产与集群场景请在测试环境验证；HDFS 格式化与分区盘操作有风险。
- 组件版权归各开源项目；请遵守许可与安全规范。
