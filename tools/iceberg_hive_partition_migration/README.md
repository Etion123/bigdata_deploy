# Hive → Iceberg 分区迁移脚本

将 **Spark SQL / Hive** 中的表迁移到 **Iceberg** 时，若仅用无分区语法的 `CREATE TABLE … USING iceberg AS SELECT *`，Iceberg 元数据里可能**没有分区字段**。本脚本会先解析源表的分区列，再生成带 **`PARTITIONED BY (...)`** 的建表语句，使分区与 `spark_catalog` 中一致。

## 前置条件

- **Spark 3.5.x**（与集群一致），已配置 **Iceberg**（如 `spark.sql.catalog.iceberg` 使用 Hive metastore 等）。
- 驱动与集群能访问 **Hive Metastore** 及源表 HDFS/ORC 数据。
- 源表分区列必须在 **表 schema** 中能通过 `SELECT *` 读出；若分区仅在路径上，需先 **MSCK REPAIR TABLE** 或补列。

## 依赖

- 运行环境需能 `import pyspark`（一般与 `spark-submit` 同环境）。
- Iceberg JAR 通过 **`--packages`** 引入（版本与 Spark/Scala 对齐）。

## 本地自检（无需 Spark）

在项目根目录或本目录执行：

```bash
python3 migrate_hive_to_iceberg.py --self-test
```

应输出 `[self-test] ok`（用于校验 `PARTITIONED BY` 括号平衡与字段解析）。

## 提交运行示例

将脚本路径换成你机器上的绝对路径或从仓库根目录的相对路径。

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  tools/iceberg_hive_partition_migration/migrate_hive_to_iceberg.py \
  --source-catalog spark_catalog \
  --source-schema tpcds_bin_partitioned_varchar_orc_2 \
  --target-catalog iceberg \
  --target-schema tpcds_bin_partitioned_varchar_orc_2
```

Iceberg 与 Hive 常用 Spark 配置（按实际 metastore、warehouse 修改）可通过 `spark-submit --conf` 传入，例如：

```bash
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hive \
  --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.iceberg.warehouse=hdfs:///warehouse/iceberg \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  tools/iceberg_hive_partition_migration/migrate_hive_to_iceberg.py \
  --source-catalog spark_catalog \
  --source-schema tpcds_bin_partitioned_varchar_orc_2 \
  --target-catalog iceberg
```

（若已在 `spark-defaults.conf` 中配置上述项，可省略对应 `--conf`。）

## 参数说明

| 参数 | 说明 |
|------|------|
| `--source-catalog` | 源 catalog，默认 `spark_catalog` |
| `--source-schema` | 源库名，默认 `tpcds_bin_partitioned_varchar_orc_2` |
| `--target-catalog` | Iceberg catalog 名，默认 `iceberg` |
| `--target-schema` | 目标库名；默认与 `--source-schema` 相同 |
| `--tables` | 仅迁移指定表，逗号分隔；默认该 schema 下全部表 |
| `--mode overwrite` | 目标表存在则 `DROP` 后重建（默认） |
| `--mode fail` | 目标表已存在则跳过 |
| `--dry-run` | 只打印每张表解析到的分区列，不执行 DDL |
| `--verbose` / `-v` | 打印生成的 `CREATE TABLE` SQL，并在解析失败时输出异常栈（便于排查） |
| `--continue-on-error` | 某张表 CTAS 失败时只记错误并继续迁移其余表；结束时有 `failures` 计数，有失败则进程退出码为 1 |

## 建议流程

1. **`--dry-run`**：确认每张表的 `partitions=[...]` 是否合理。  
2. 小表试跑单表：`--tables store_sales`。  
3. 全量：`--mode overwrite`（会删除目标同名 Iceberg 表，请谨慎）。

## 分区列解析顺序

脚本按顺序尝试：

1. `USE CATALOG` 后 `catalog.listColumns(表, 库)` 的 `isPartition`  
2. `SHOW PARTITIONS` 首条分区串  
3. `SHOW CREATE TABLE` 中 **`PARTITIONED BY`**（支持括号嵌套）  
4. `DESCRIBE TABLE EXTENDED` 中 Partition Information  

若四类都得不到分区列，则按**非分区表**建 Iceberg（`partitions=(none)`）。

## 常见问题

- **`AnalysisException` / 找不到 catalog**：检查 `spark.sql.catalog.*` 与 `--packages` 是否与 Spark 3.5、Scala 2.12 一致。  
- **CTAS 报列不匹配**：源表分区列未出现在数据中；先修复 Hive 元数据或改 `SELECT` 列列表（需自行改脚本）。  
- **`CREATE NAMESPACE` 失败**：确认对 Iceberg catalog 有建库权限，或先在 Hive/Spark 中手动创建同名 database/namespace。  
- **schema 名拼写**：若实际库名为 `tpcds_bin_partitioned_vachar_orc_2` 等，请用 `--source-schema` 指定真实名称。

## 文件说明

| 文件 | 说明 |
|------|------|
| `migrate_hive_to_iceberg.py` | 主脚本 |
