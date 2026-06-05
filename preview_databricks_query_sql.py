"""
Local, offline preview of the SQL a Databricks import job will actually run.

The unload job rewrites every source table reference in the customer's SQL into a
temp-view name before executing it. This tool performs the same rewrite and prints
the resulting query, plus structural lint checks — all without Spark, credentials,
or a cluster, so a customer can run it on a laptop.

Example:
    python3 preview_databricks_query_sql.py \\
        --table_versions_map cat.sch.a=0-5,cat.sch.b=0-5 \\
        --data_type EVENT \\
        --sql-file my_query.sql
"""
import argparse
import re

from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)


def transform_sql(sql: str, table_to_version_range: dict[str, list[int]]) -> str:
    """Apply the same table-name -> temp-view substitution the unload job uses."""
    transformed = sql
    for table in table_to_version_range:
        view_name = build_temp_view_name(table)
        transformed = replace_table_name_in_sql(transformed, table, view_name)
    return transformed


def _is_balanced(sql: str, open_char: str, close_char: str) -> bool:
    depth = 0
    for ch in sql:
        if ch == open_char:
            depth += 1
        elif ch == close_char:
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def lint_sql(sql: str, table_to_version_range: dict[str, list[int]]) -> list[str]:
    """Return a list of human-readable warnings; empty list means clean."""
    findings: list[str] = []

    if not _is_balanced(sql, "(", ")"):
        findings.append("Unbalanced parentheses '(' / ')' in SQL.")
    if sql.count("'") % 2 != 0:
        findings.append("Unbalanced single quotes (') in SQL.")

    # Tables present in the map but never referenced in the SQL (likely a typo or
    # stale mapping), and tables referenced but missing from the map (will not be
    # swapped to a temp view, so the query will hit the real table or fail).
    for table in table_to_version_range:
        if re.search(r"(?<![A-Za-z0-9_`])" + re.escape(table) + r"(?![A-Za-z0-9_`])", sql) is None:
            findings.append(
                f"Table '{table}' is in --table_versions_map but not referenced in the SQL."
            )

    # Fully-qualified names of the form catalog.schema.table that look like a
    # source table reference but are absent from the map.
    referenced = set(re.findall(r"(?<![A-Za-z0-9_`])([A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)(?![A-Za-z0-9_`])", sql))
    for name in sorted(referenced):
        if name not in table_to_version_range:
            findings.append(
                f"Table '{name}' is referenced in the SQL but missing from --table_versions_map; "
                "it will not be swapped for a versioned temp view."
            )

    # A '--' line comment is fine on its own line, but if newlines were ever
    # stripped upstream it would comment out the rest of the query. Warn so the
    # customer is aware of the fragility.
    if re.search(r"--", sql):
        findings.append(
            "SQL contains '--' line comments; these rely on newlines surviving. "
            "Prefer /* ... */ block comments to avoid accidental truncation."
        )

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preview the transformed SQL a Databricks import job will run (offline)."
    )
    parser.add_argument("--table_versions_map", required=True,
                        help="e.g. catalog.schema.table=0-12,catalog2.schema2.table2=10-100")
    parser.add_argument("--data_type", required=False,
                        choices=["EVENT", "USER_PROPERTY", "GROUP_PROPERTY", "WAREHOUSE_PROPERTY"],
                        help="informational only in preview mode")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="the transformation SQL")
    group.add_argument("--sql-file", help="path to a file containing the transformation SQL")
    args, _ = parser.parse_known_args()

    sql = args.sql if args.sql is not None else open(args.sql_file, "r").read()
    versions = parse_table_versions_map_arg(args.table_versions_map)

    print("=== Transformed SQL (this is what the job runs) ===")
    print(transform_sql(sql, versions))
    print()

    findings = lint_sql(sql, versions)
    if findings:
        print("=== Lint findings ===")
        for finding in findings:
            print(f"  - {finding}")
        return 1
    print("=== Lint findings ===")
    print("  none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
