#!/usr/bin/env python3
"""
Migrate Hive / Spark SQL catalog tables to Iceberg with explicit PARTITIONED BY,
so partition columns are registered in Iceberg metadata (fixes missing partition fields after CTAS).

Typical Spark 3.5.2 + Iceberg (HiveCatalog) submit:

  spark-submit \\
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \\
    migrate_hive_to_iceberg.py \\
    --source-catalog spark_catalog \\
    --source-schema tpcds_bin_partitioned_varchar_orc_2 \\
    --target-catalog iceberg

Requires: PySpark 3.5.x on driver; cluster must have Iceberg extensions / Hive metastore configured.

Note: PARTITIONED BY requires those columns to exist in the source table row (metastore schema). If a
Hive table only has partition values on the path and not in the schema, repair the table (e.g. MSCK)
or add columns before CTAS.
"""

from __future__ import annotations

import argparse
import re
import sys
import traceback
from typing import Any, List, Sequence


def _safe_sql_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def _fqn(catalog: str, schema: str, table: str) -> str:
    a, b, c = _safe_sql_ident(catalog), _safe_sql_ident(schema), _safe_sql_ident(table)
    return f"{a}.{b}.{c}"


def _table_name_from_list_entry(r: Any) -> str:
    return getattr(r, "name", None) or getattr(r, "tableName", None) or str(r)


def _row_get(row: Any, *keys: str) -> Any:
    """Fetch first present key from Row / dict-like (Spark column names vary by version)."""
    if hasattr(row, "asDict"):
        d = row.asDict()
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
    for k in keys:
        try:
            v = row[k]
            if v is not None:
                return v
        except Exception:
            pass
    for k in keys:
        if hasattr(row, k):
            return getattr(row, k)
    return None


def _extract_partitioned_by_paren(ddl: str) -> str | None:
    """Return text inside PARTITIONED BY ( ... ) with balanced parentheses."""
    m = re.search(r"PARTITIONED BY\s*\(", ddl, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    open_pos = m.end() - 1
    depth = 0
    for i in range(open_pos, len(ddl)):
        c = ddl[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return ddl[open_pos + 1 : i]
    return None


def _parse_partitioned_by_inner(inner: str) -> List[str]:
    """Split PARTITIONED BY inner on commas at depth 0; first token of each piece is the column name."""
    cols: List[str] = []
    depth = 0
    buf: List[str] = []
    for ch in inner:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            piece = "".join(buf).strip()
            buf = []
            if piece:
                piece = piece.strip("`").strip()
                ident = piece.split()[0].strip("`") if piece else ""
                if ident:
                    cols.append(ident)
            continue
        buf.append(ch)
    piece = "".join(buf).strip()
    if piece:
        piece = piece.strip("`").strip()
        ident = piece.split()[0].strip("`") if piece else ""
        if ident:
            cols.append(ident)
    return cols


def _partition_cols_from_show_partitions(spark, fqn: str) -> List[str]:
    """Infer ordered partition column names from first SHOW PARTITIONS row."""
    try:
        df = spark.sql(f"SHOW PARTITIONS {fqn}")
    except Exception:
        return []
    rows = df.limit(5).collect()
    if not rows:
        return []
    row = rows[0]
    spec = None
    for v in row.asDict().values():
        if isinstance(v, str) and "=" in v:
            spec = v
            break
    if not spec:
        return []
    keys: List[str] = []
    for seg in spec.split("/"):
        seg = seg.strip()
        if not seg or "=" not in seg:
            continue
        k, _, _ = seg.partition("=")
        keys.append(k.strip())
    return keys


def _partition_cols_from_show_create(spark, fqn: str) -> List[str]:
    """Parse PARTITIONED BY (`c1`, `c2`, ...) from SHOW CREATE TABLE."""
    try:
        row = spark.sql(f"SHOW CREATE TABLE {fqn}").collect()[0]
    except Exception:
        return []
    ddl = row[0] if len(row) > 0 else None
    if not isinstance(ddl, str):
        return []
    inner = _extract_partitioned_by_paren(ddl)
    if not inner:
        return []
    return _parse_partitioned_by_inner(inner.strip())


def _partition_cols_from_describe_extended(spark, fqn: str) -> List[str]:
    """Parse DESCRIBE TABLE EXTENDED output for partition columns (fallback)."""
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect()
    except Exception:
        return []
    in_part = False
    keys: List[str] = []
    for r in rows:
        d = r.asDict()
        c1 = (d.get("col_name") or d.get("colName") or "").strip()
        if c1.startswith("# Partition Information"):
            in_part = True
            continue
        if not in_part:
            continue
        if c1.startswith("# col_name") or (c1.startswith("#") and "col_name" in c1):
            continue
        if c1.startswith("# Detailed") or c1.startswith("# Metadata") or c1.startswith("# Storage"):
            break
        if c1 and not c1.startswith("#"):
            keys.append(c1)
    return keys


def get_partition_columns(
    spark,
    catalog: str,
    schema: str,
    table: str,
    *,
    verbose: bool = False,
) -> List[str]:
    """
    Resolve Hive/Spark partition column names in order.
    Order: listColumns.isPartition -> SHOW PARTITIONS -> SHOW CREATE TABLE -> DESCRIBE EXTENDED.
    """
    fqn = _fqn(catalog, schema, table)
    keys: List[str] = []

    try:
        spark.sql(f"USE CATALOG {_safe_sql_ident(catalog)}")
        # Two-arg form: table name + database/namespace (Spark 3.x catalog API).
        for col in spark.catalog.listColumns(_safe_sql_ident(table), _safe_sql_ident(schema)):
            if getattr(col, "isPartition", False):
                keys.append(col.name)
    except Exception as e:
        if verbose:
            print(f"[verbose] listColumns failed for {fqn}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        keys = []

    if keys:
        return keys

    for fn in (
        _partition_cols_from_show_partitions,
        _partition_cols_from_show_create,
        _partition_cols_from_describe_extended,
    ):
        try:
            keys = fn(spark, fqn)
        except Exception as e:
            if verbose:
                print(f"[verbose] {fn.__name__} failed for {fqn}: {e}", file=sys.stderr)
            keys = []
        if keys:
            return keys

    return keys


def list_tables(spark, catalog: str, schema: str, verbose: bool = False) -> List[str]:
    """List non-temporary tables via SHOW TABLES IN catalog.schema, with USE fallback."""
    q = f"SHOW TABLES IN {_safe_sql_ident(catalog)}.{_safe_sql_ident(schema)}"
    try:
        df = spark.sql(q)
    except Exception as e:
        if verbose:
            print(f"[verbose] {q} failed: {e}; falling back to USE CATALOG", file=sys.stderr)
        try:
            spark.sql(f"USE CATALOG {_safe_sql_ident(catalog)}")
            spark.sql(f"USE {_safe_sql_ident(schema)}")
        except Exception as e2:
            if verbose:
                print(f"[verbose] USE fallback failed: {e2}", file=sys.stderr)
            raise
        out: List[str] = []
        for r in spark.catalog.listTables():
            if getattr(r, "isTemporary", False):
                continue
            out.append(_table_name_from_list_entry(r))
        return out

    names: List[str] = []
    for r in df.collect():
        name = _row_get(r, "tableName", "tablename", "table_name")
        tmp = _row_get(r, "isTemporary", "istemporary", "is_temporary")
        if tmp is None:
            tmp = False
        if name and not bool(tmp):
            names.append(str(name))
    return names


def target_table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {_fqn(catalog, schema, table)}")
        return True
    except Exception:
        return False


def ensure_target_namespace(spark, target_catalog: str, target_schema: str) -> None:
    """Create Iceberg namespace (Spark 3: CREATE NAMESPACE catalog.schema)."""
    ns = f"{_safe_sql_ident(target_catalog)}.{_safe_sql_ident(target_schema)}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def migrate_one(
    spark,
    source_catalog: str,
    source_schema: str,
    target_catalog: str,
    target_schema: str,
    table: str,
    *,
    mode: str,
    verbose: bool,
) -> None:
    src = _fqn(source_catalog, source_schema, table)
    tgt = _fqn(target_catalog, target_schema, table)
    parts = get_partition_columns(spark, source_catalog, source_schema, table, verbose=verbose)

    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {tgt}")

    part_clause = ""
    if parts:
        quoted = ", ".join(_safe_sql_ident(p) for p in parts)
        part_clause = f" PARTITIONED BY ({quoted})"

    sql = f"""
CREATE TABLE {tgt}
USING iceberg
{part_clause}
AS SELECT * FROM {src}
""".strip()
    print(f"[migrate] {table}: partitions={parts or '(none)'}")
    if verbose:
        print(sql)
    spark.sql(sql)


def build_spark_session(app_name: str):
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()


def parse_include_tables(arg: str | None) -> set[str] | None:
    if not arg:
        return None
    return {t.strip() for t in arg.split(",") if t.strip()}


def run_self_test() -> int:
    """Lightweight checks without Spark (regex / parsing)."""
    assert _parse_partitioned_by_inner("`d_date`, `hr`") == ["d_date", "hr"]
    ddl = "CREATE TABLE t (a int) PARTITIONED BY (`dt` string, `hr` int) STORED AS ORC"
    inner = _extract_partitioned_by_paren(ddl)
    assert inner is not None
    cols = _parse_partitioned_by_inner(inner.strip())
    assert cols == ["dt", "hr"], cols
    full = "CREATE TABLE t (x int) PARTITIONED BY (a int, b struct<x:int>) STORED AS ORC"
    ext = _extract_partitioned_by_paren(full)
    assert ext is not None and "struct" in ext
    print("[self-test] ok")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Hive -> Iceberg migration with partition spec preserved.")
    p.add_argument("--source-catalog", default="spark_catalog")
    p.add_argument("--source-schema", default="tpcds_bin_partitioned_varchar_orc_2")
    p.add_argument("--target-catalog", default="iceberg")
    p.add_argument("--target-schema", default=None, help="Defaults to --source-schema")
    p.add_argument(
        "--tables",
        default=None,
        help="Comma-separated table names; default all tables in source schema",
    )
    p.add_argument(
        "--mode",
        choices=("overwrite", "fail"),
        default="overwrite",
        help="overwrite: DROP IF EXISTS Iceberg table then CTAS; fail: skip if target exists",
    )
    p.add_argument("--dry-run", action="store_true", help="Print partition detection only, no DDL")
    p.add_argument("--verbose", "-v", action="store_true", help="Print SQL and parser errors to stderr")
    p.add_argument(
        "--self-test",
        action="store_true",
        help="Run parsing self-checks without Spark and exit",
    )
    args = p.parse_args(list(argv) if argv is not None else None)

    if args.self_test:
        return run_self_test()

    target_schema = args.target_schema or args.source_schema
    include = parse_include_tables(args.tables)

    spark = build_spark_session("iceberg_hive_partition_migration")

    tables = list_tables(spark, args.source_catalog, args.source_schema, verbose=args.verbose)
    if include is not None:
        tables = [t for t in tables if t in include]

    if not tables:
        print("No tables to migrate.", file=sys.stderr)
        return 1

    ensure_target_namespace(spark, args.target_catalog, target_schema)

    for tbl in sorted(tables):
        if args.dry_run:
            parts = get_partition_columns(
                spark, args.source_catalog, args.source_schema, tbl, verbose=args.verbose
            )
            print(f"[dry-run] {tbl}: {parts}")
            continue
        if args.mode == "fail" and target_table_exists(spark, args.target_catalog, target_schema, tbl):
            print(f"[skip] {tbl}: target exists", file=sys.stderr)
            continue

        migrate_one(
            spark,
            args.source_catalog,
            args.source_schema,
            args.target_catalog,
            target_schema,
            tbl,
            mode=args.mode,
            verbose=args.verbose,
        )

    print("[migrate] done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
