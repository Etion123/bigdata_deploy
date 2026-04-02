# bigdata_deploy

单机（openEuler / RHEL 系）自动安装 **ZooKeeper、Hadoop、Hive、HBase、Kafka、Spark、Flink** 等组件。实现为 **Python 3 标准库**（下载走 `urllib`，无需 `pip` 依赖）。

## 环境要求

- **操作系统**：openEuler 22.03 SP4 或兼容的 RHEL 系（`dnf`、`systemd`）。
- **Python**：`python3`（建议 3.8+）。
- **权限**：安装与校验需 **root**（`sudo`）。
- **资源**：内存建议 ≥8GB（YARN 默认配置对 NodeManager 有内存要求，可按机器在模板里调小）。

## 如何调用

在克隆后的**仓库根目录**执行（与 `install.py` 同级）：

```bash
sudo python3 install.py [选项] [阶段]
```

### 常用示例

| 场景 | 命令 |
|------|------|
| 全流程安装并做完整校验 | `sudo python3 install.py all` |
| 只装到 Spark 并做 Spark 校验 | `sudo python3 install.py to-spark` |
| 仅重跑完整校验（组件已装好） | `sudo python3 install.py verify` |
| 仅重跑 Spark 栈校验 | `sudo python3 install.py verify-spark` |
| 指定配置文件 | `sudo python3 install.py -c /path/to/deploy.conf all` |
| 查看离线所需压缩包文件名 | `python3 install.py list-bundles` |

配置文件默认路径：`./config/deploy.conf`。也可通过环境变量指定：

```bash
export CONFIG_FILE=/path/to/deploy.conf
sudo -E python3 install.py all
```

### 安装阶段（phase）

除组合阶段外，均可单独执行，便于排错或断点续装（需已满足前置步骤，例如装 Hadoop 前应先装 JDK、ZK 等）。

| 阶段 | 说明 |
|------|------|
| `all` | repo → 可选数据盘 → SSH → JDK → ZK → Hadoop → Hive → HBase → Kafka → Spark → Flink → 全量校验 |
| `to-spark` | 到 Spark 为止，并执行 Spark 校验（不含 Hive/HBase/Kafka/Flink） |
| `verify` / `verify-spark` | 仅校验 |
| `repo` | 基础包、`dnf`、可选本地源与代理 |
| `disk` | 仅当 `AUTO_MOUNT_DATA_DISK=yes` 时格式化并挂载数据盘 |
| `ssh` | 创建 `hadoop` 用户与本机免密 |
| `jdk` | 系统 OpenJDK 8 或 tarball JDK |
| `zk` / `hadoop` / `hive` / `hbase` / `kafka` / `spark` / `flink` | 各组件安装 |

查看帮助：

```bash
python3 install.py -h
```

## 配置文件 `config/deploy.conf`

- **键值格式**：`KEY=value`，支持 `#` 注释；支持 `${VAR}` 引用（如 `DOWNLOAD_DIR` 引用 `INSTALL_BASE`）。
- **安装根目录**：默认 `INSTALL_BASE=/usr/local/bigdata`，组件与数据子目录均在其下。
- **版本**：`ZOOKEEPER_VERSION`、`HADOOP_VERSION` 等与 Apache 发行包文件名一致，修改后需重新下载或替换 `downloads` 下对应文件。

### 代理与网络

- 在 `deploy.conf` 中设置 `HTTP_PROXY` / `HTTPS_PROXY`（或仅设 `PKG_PROXY`，效果同 `HTTP_PROXY`），会作用于 `dnf` 与本程序下载。
- `OFFLINE_MODE=yes`：**不**从外网拉取组件包；必须把各 tarball 放到 `INSTALL_BASE/downloads`（文件名与 `list-bundles` 输出一致）。
- 离线时 **`repo` 仍会执行 `dnf`**：需配置内网源，例如 `LOCAL_REPO_FILE` 指向本地 `.repo`；`JAVA_USE_SYSTEM=yes` 时 JDK 也来自 `dnf`，无需 JDK 的 tar 包。

### 可选：独立数据盘

- `AUTO_MOUNT_DATA_DISK=yes` 时才会执行 `disk` 阶段逻辑；会**清空**指定或自动识别的空闲盘并挂载到 `DATA_MOUNT_POINT`（默认 `/data`）。生产环境请谨慎。

## 离线部署简要流程

1. 在一台可上网机器上按当前 `deploy.conf` 版本下载好各组件包，或从已有镜像拷贝。
2. 在目标机编辑 `deploy.conf`：`OFFLINE_MODE=yes`，并配置好 `LOCAL_REPO_FILE`（及本地 JDK 的 dnf 源）。
3. 将压缩包放入 `INSTALL_BASE/downloads`（默认即 `/usr/local/bigdata/downloads`）。
4. 执行：`python3 install.py list-bundles` 核对文件是否齐全。
5. 执行：`sudo python3 install.py all`。

## 仓库目录说明

```
install.py              # 入口
bigdata_deploy/         # Python 包：配置解析、步骤实现、下载与模板渲染
config/deploy.conf      # 主配置
config/examples/        # 示例（如本地 dnf 源 repo）
templates/              # Hadoop / Hive 等配置模板
```

## 说明与免责

- 脚本面向**单机伪分布式**场景；生产多节点需自行改模板与拓扑。
- 组件版权归各自开源项目；请遵守其许可证与贵司安全规范。
