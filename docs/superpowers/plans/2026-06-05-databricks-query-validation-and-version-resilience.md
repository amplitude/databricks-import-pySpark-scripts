# Databricks Query Validation & Version/Schema Resilience — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give customers two tools to inspect/validate their Databricks import SQL (offline preview + on-cluster validation), and make the unload scripts resilient to Delta CDF schema/version drift — all backed by a single shared SQL-helper module.

**Architecture:** Extract the pure (pyspark-free) SQL helpers into `databricks_sql_utils.py`; every entrypoint imports it (the repo is checked out whole on the cluster via `SparkPythonTask` `Source.GIT`, so sibling imports work). Add `preview_databricks_query_sql.py` (pure string substitution + lint, no Spark) and `validate_databricks_query.py` (creates views + runs `spark.sql` for parse/analysis, read-only). Add a `defaultSchemaModeForColumnMappingTable=endVersion` Spark conf to both unload scripts and an incompatible-schema latest-only retry to the main unload script.

**Tech Stack:** Python 3, PySpark (on-cluster only), `unittest` (no pytest in this repo — run with `python3 -m unittest`).

**Spec:** `docs/superpowers/specs/2026-06-05-databricks-query-validation-and-version-resilience-design.md`

**Deployment constraint:** Production Databricks jobs run off branch `main` via GIT source. The shared module and every entrypoint that imports it must merge to `main` together (one PR). Implement on the existing branch `ning/databricks-query-validation-and-resilience`.

---

## File Structure

| File | Responsibility |
|---|---|
| `databricks_sql_utils.py` (new) | Pure, pyspark-free helpers: `parse_table_versions_map_arg`, `build_temp_view_name`, `replace_table_name_in_sql`, `_IDENTIFIER_CHAR`. Single source of truth. |
| `preview_databricks_query_sql.py` (new) | Local, offline tool. `transform_sql`, `lint_sql`, `main`. No pyspark. |
| `validate_databricks_query.py` (new) | On-cluster, read-only tool. `required_columns_for`, `check_required_columns`, plus a pyspark `main`. |
| `unload_databricks_data_to_s3.py` (modify) | Import shared helpers (delete local copies); add endVersion conf; add incompatible-schema retry. |
| `unload_databricks_data_to_s3_partition.py` (modify) | Import shared helpers (delete local copies); add endVersion conf only. |
| `TEST_unload_databricks_data_to_s3.py` (modify) | Import shared helpers (delete local copies). No behavior change. |
| `test/test_table_name_substitution.py` (modify) | Point at the shared module directly (drop pyspark stub + `_MODULES_UNDER_TEST` fan-out). |
| `test/test_shared_module_wiring.py` (new) | Assert each unload script re-exports the shared `replace_table_name_in_sql` (import-wiring guard). |
| `test/test_schema_resilience.py` (new) | Unit-test `extract_incompatible_schema_error_signature`. |
| `test/test_preview_query_sql.py` (new) | Unit-test preview tool: transformed SQL + each lint check. |
| `test/test_validate_query.py` (new) | Unit-test `required_columns_for` / `check_required_columns`. |
| `README.md` (modify) | Usage docs for both tools + resilience behavior. |

---

## Task 1: Shared module `databricks_sql_utils.py`

**Files:**
- Create: `databricks_sql_utils.py`
- Modify: `test/test_table_name_substitution.py`

- [ ] **Step 1: Create the shared module**

Create `databricks_sql_utils.py` with exactly this content (helpers copied verbatim from `unload_databricks_data_to_s3.py:156-292`, no pyspark import):

```python
"""
Shared, pyspark-free SQL helpers for the Databricks unload scripts and the query
inspection tools.

This module is intentionally free of any pyspark import so the local preview tool
and the unit tests can use it with no Spark install. On the Databricks cluster the
whole repo is checked out (jobs run as SparkPythonTask with Source.GIT), so the
entrypoint scripts import it directly.
"""
import collections
import re
import time


def parse_table_versions_map_arg(table_versions_map: str) -> dict[str, list[int]]:
    """
    Extract table && version range numbers from input str.
    :param table_versions_map: table versions map. Sample input 'catalog.schema.table=1-2,catalog.schema2.table2=11-12'
    which means table 'catalog.schema.table' with version range [1,2] and table 'catalog.schema2.table2'
    with version range [11,12].
    :return: table to version ranges map. Sample output: {'catalog.schema.table': [1,2]}
    """
    dictionary = collections.defaultdict(list)
    table_and_versions_list = table_versions_map.split(",")
    for table_and_versions in table_and_versions_list:
        table_name = table_and_versions.split("=")[0]
        versions = table_and_versions.split("=")[1].split("-")
        dictionary[table_name].append(int(versions[0]))
        dictionary[table_name].append(int(versions[1]))
    return dictionary


def build_temp_view_name(table_full_name: str) -> str:
    """
    Build temp view name for the table. Wrap table name with '`' to escape '.'. Append `epoch` so view name is very
    unlikely collapse with another table.
    :param table_full_name: table name
    :return: temp view name for the table
    """
    return '`{table}.{epoch}`'.format(table=table_full_name, epoch=int(time.time()))


# Characters that are part of a single SQL identifier token (a backtick-quoted
# name counts). A match adjacent to one of these is part of a longer identifier
# and must NOT be replaced. '.' is intentionally excluded so a fully-qualified
# name used as a column prefix (`cat.sch.t.col`) is still rewritten to the view,
# while substring collisions (e.g. dim_product vs dim_product_version_map) are
# still blocked by the alphanumeric/underscore boundary.
_IDENTIFIER_CHAR = r"[A-Za-z0-9_`]"


def replace_table_name_in_sql(sql: str, table_name: str, replacement: str) -> str:
    """
    Replace whole-identifier occurrences of a fully-qualified table name in SQL
    with a replacement (temp view) name.

    A naive ``sql.replace(table_name, replacement)`` is unsafe when one source
    table name is a substring of another. For example ``cat.sch.dim_product`` is
    contained in ``cat.sch.dim_product_version_map``, so replacing the shorter
    name also rewrites the middle of the longer reference, producing invalid SQL
    that Databricks rejects with ``PARSE_SYNTAX_ERROR ... at or near 'AS'``.

    We only replace matches that are not adjacent to another identifier character
    (alphanumerics, ``_`` or a backtick), so a table name is never spliced into
    the middle of a longer table name. ``.`` is deliberately not treated as an
    identifier boundary, so a fully-qualified name used as a column prefix
    (``cat.sch.t.col``) is rewritten to the view as well.
    """
    pattern = re.compile(
        r"(?<!" + _IDENTIFIER_CHAR + r")"
        + re.escape(table_name)
        + r"(?!" + _IDENTIFIER_CHAR + r")"
    )
    return pattern.sub(lambda _match: replacement, sql)
```

- [ ] **Step 2: Rewrite the substitution test to target the shared module**

Replace the entire contents of `test/test_table_name_substitution.py` with (no pyspark stub needed — the shared module is pure):

```python
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from databricks_sql_utils import replace_table_name_in_sql


class TestReplaceTableNameInSql(unittest.TestCase):

    def test_replaces_a_standalone_table_reference(self):
        sql = "FROM cat.sch.dim_product AS dp"
        result = replace_table_name_in_sql(
            sql, "cat.sch.dim_product", "`cat.sch.dim_product.1700000000`"
        )
        self.assertEqual("FROM `cat.sch.dim_product.1700000000` AS dp", result)

    def test_does_not_corrupt_table_name_that_contains_the_target_as_a_prefix(self):
        # 'cat.sch.dim_product' is a substring of 'cat.sch.dim_product_version_map'.
        # Replacing the shorter name must leave the longer reference untouched,
        # otherwise the generated SQL becomes
        #   `cat.sch.dim_product.1700000000`_version_map AS dpvm
        # which Databricks rejects with PARSE_SYNTAX_ERROR at or near 'AS'.
        sql = (
            "FROM cat.sch.dim_product AS dp\n"
            "LEFT JOIN cat.sch.dim_product_version_map AS dpvm ON dp.id = dpvm.id"
        )
        result = replace_table_name_in_sql(
            sql, "cat.sch.dim_product", "`cat.sch.dim_product.1700000000`"
        )
        self.assertEqual(
            "FROM `cat.sch.dim_product.1700000000` AS dp\n"
            "LEFT JOIN cat.sch.dim_product_version_map AS dpvm ON dp.id = dpvm.id",
            result,
        )

    def test_replaces_every_standalone_occurrence(self):
        sql = "SELECT * FROM cat.sch.t WHERE EXISTS (SELECT 1 FROM cat.sch.t)"
        result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
        self.assertEqual(
            "SELECT * FROM `cat.sch.t.42` WHERE EXISTS (SELECT 1 FROM `cat.sch.t.42`)",
            result,
        )

    def test_rewrites_table_name_used_as_a_qualified_column_prefix(self):
        sql = "SELECT cat.sch.t.col FROM cat.sch.t"
        result = replace_table_name_in_sql(sql, "cat.sch.t", "`cat.sch.t.42`")
        self.assertEqual(
            "SELECT `cat.sch.t.42`.col FROM `cat.sch.t.42`",
            result,
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run the test to verify it passes against the shared module**

Run: `cd /Users/ningwang/workspace/databricks-import-pySpark-scripts && python3 -m unittest test.test_table_name_substitution -v`
Expected: `Ran 4 tests` … `OK`

- [ ] **Step 4: Commit**

```bash
git add databricks_sql_utils.py test/test_table_name_substitution.py
git commit -m "Add shared databricks_sql_utils module; retarget substitution tests"
```

---

## Task 2: Refactor the three unload scripts to import the shared helpers

Each script currently carries its own copy of the four helpers. Replace them with an import and delete the local copies (and now-unused imports). The module-level run code lives under `if __name__ == '__main__'`, so importing the modules in tests is side-effect-free.

**Files:**
- Modify: `unload_databricks_data_to_s3.py`
- Modify: `unload_databricks_data_to_s3_partition.py`
- Modify: `TEST_unload_databricks_data_to_s3.py`
- Create: `test/test_shared_module_wiring.py`

- [ ] **Step 1: Write the failing wiring test**

Create `test/test_shared_module_wiring.py`:

```python
import os
import sys
import types
import unittest


# The unload scripts import pyspark at module load. The wiring under test (that
# each script re-exports the shared substitution helper) needs no real Spark, so
# stub pyspark before importing them.
def _install_pyspark_stub() -> None:
    if "pyspark" in sys.modules:
        return
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    sqltypes = types.ModuleType("pyspark.sql.types")

    sql.SparkSession = object
    sql.DataFrame = object
    sql.Column = object
    functions.col = lambda *a, **k: None
    sql.functions = functions
    for name in ("StructType", "ArrayType", "MapType", "NullType", "DataType"):
        setattr(sqltypes, name, type(name, (), {}))

    pyspark.sql = sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions
    sys.modules["pyspark.sql.types"] = sqltypes


_install_pyspark_stub()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import importlib

import databricks_sql_utils

_ENTRYPOINTS = (
    "unload_databricks_data_to_s3",
    "unload_databricks_data_to_s3_partition",
    "TEST_unload_databricks_data_to_s3",
)


class TestSharedModuleWiring(unittest.TestCase):

    def test_each_entrypoint_reexports_shared_substitution(self):
        for name in _ENTRYPOINTS:
            with self.subTest(module=name):
                module = importlib.import_module(name)
                self.assertIs(
                    module.replace_table_name_in_sql,
                    databricks_sql_utils.replace_table_name_in_sql,
                )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python3 -m unittest test.test_shared_module_wiring -v`
Expected: FAIL — each subtest fails `assertIs` because the scripts still define their own local copy (not the shared object).

- [ ] **Step 3: Refactor `unload_databricks_data_to_s3.py`**

a) Remove the now-unused top-level imports `import collections` (line 2) and `import re` (line 5). Keep `import time` (still used by `calculate_num_partitions` / start-time logging).

b) Add the shared import directly after the pyspark imports block (after line 17, the `from pyspark.sql.types import (...)` group):

```python
from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)
```

c) Delete the four local helper definitions now living in the shared module:
`parse_table_versions_map_arg` (def at line 156), `build_temp_view_name` (line 174),
the `_IDENTIFIER_CHAR` block + comment (line 263), and `replace_table_name_in_sql`
(line 266). Delete each function from its `def`/assignment through the line before
the next surviving definition, including the leading comment block on
`_IDENTIFIER_CHAR`. Leave `build_sql_to_query_table_of_version`,
`build_sql_to_query_table_between_versions`, `fetch_data`, `filter_data`, and
everything else intact.

- [ ] **Step 4: Refactor `unload_databricks_data_to_s3_partition.py`**

a) Remove unused top-level imports `import collections` (line 2), `import re` (line 4), and `import time` (line 5). Keep `import math` and `import argparse`. (`time` is only used by `build_temp_view_name`, which moves to the shared module.)

b) Add directly after the pyspark imports (after line 9, `from pyspark.sql.functions import col`):

```python
from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)
```

c) Delete the local definitions of `parse_table_versions_map_arg` (line 17),
`build_temp_view_name` (line 35), the `_IDENTIFIER_CHAR` comment+assignment
(lines 45-51), and `replace_table_name_in_sql` (line 54). Leave
`build_sql_to_query_table_of_version`, `build_sql_to_query_table_between_versions`,
`fetch_data`, `filter_data`, `get_partition_count`, `export_meta_data` intact.

- [ ] **Step 5: Refactor `TEST_unload_databricks_data_to_s3.py`**

a) Remove unused top-level import `import collections` (line 9) and `import re`
(line 12). Keep `import time` (used elsewhere).

b) Add directly after the `from pyspark.sql.types import (...)` block (after line ~25):

```python
from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)
```

c) Delete local definitions of `parse_table_versions_map_arg` (line 163),
`build_temp_view_name` (line 181), the `_IDENTIFIER_CHAR` comment+assignment
(line 270), and `replace_table_name_in_sql` (line 273). Leave all other
functions (`build_views_for_tables`, `extract_missing_cdf_error_signature`, etc.)
intact.

- [ ] **Step 6: Run the wiring test to verify it passes**

Run: `python3 -m unittest test.test_shared_module_wiring -v`
Expected: `OK` (all three subtests pass).

- [ ] **Step 7: Run the full existing test suite to confirm nothing broke**

Run: `python3 -m unittest test.test_table_name_substitution test.test_shared_module_wiring test.test_export_dataframe_logging -v`
Expected: all `OK`. (These three import the refactored modules with the pyspark stub.)

- [ ] **Step 8: Commit**

```bash
git add unload_databricks_data_to_s3.py unload_databricks_data_to_s3_partition.py TEST_unload_databricks_data_to_s3.py test/test_shared_module_wiring.py
git commit -m "Import shared SQL helpers in unload scripts; delete duplicated copies"
```

---

## Task 3: Local preview tool `preview_databricks_query_sql.py`

A pure, offline tool. `transform_sql` applies the substitution for every mapped
table; `lint_sql` returns a list of human-readable warnings.

**Files:**
- Create: `preview_databricks_query_sql.py`
- Create: `test/test_preview_query_sql.py`

- [ ] **Step 1: Write the failing tests**

Create `test/test_preview_query_sql.py`:

```python
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from preview_databricks_query_sql import transform_sql, lint_sql


class TestTransformSql(unittest.TestCase):

    def test_substitutes_every_mapped_table_with_a_view_name(self):
        sql = "SELECT * FROM cat.sch.a JOIN cat.sch.b ON a.id = b.id"
        versions = {"cat.sch.a": [0, 5], "cat.sch.b": [0, 5]}
        out = transform_sql(sql, versions)
        # original fully-qualified names no longer appear as bare references
        self.assertNotIn("FROM cat.sch.a ", out)
        self.assertNotIn("JOIN cat.sch.b ", out)
        # each table becomes a backtick-wrapped temp view name
        self.assertIn("`cat.sch.a.", out)
        self.assertIn("`cat.sch.b.", out)

    def test_substring_table_not_corrupted(self):
        sql = "FROM cat.sch.dim_product AS dp LEFT JOIN cat.sch.dim_product_version_map AS m ON dp.id = m.id"
        versions = {"cat.sch.dim_product": [0, 1], "cat.sch.dim_product_version_map": [0, 1]}
        out = transform_sql(sql, versions)
        self.assertIn("_version_map AS m", out)


class TestLintSql(unittest.TestCase):

    def test_clean_sql_has_no_findings(self):
        sql = "SELECT a.x AS event_type FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        self.assertEqual([], lint_sql(sql, versions))

    def test_flags_unbalanced_parentheses(self):
        sql = "SELECT MAP('k', a.v FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("paren" in f.lower() for f in findings))

    def test_flags_table_in_map_not_referenced_in_sql(self):
        sql = "SELECT * FROM cat.sch.a"
        versions = {"cat.sch.a": [0, 1], "cat.sch.unused": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("cat.sch.unused" in f for f in findings))

    def test_flags_table_referenced_but_missing_from_map(self):
        sql = "SELECT * FROM cat.sch.a JOIN cat.sch.missing ON a.id = missing.id"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("cat.sch.missing" in f for f in findings))

    def test_flags_line_comment_hazard(self):
        sql = "SELECT a.x AS event_type, -- a.y AS device_id\n a.z FROM cat.sch.a AS a"
        versions = {"cat.sch.a": [0, 1]}
        findings = lint_sql(sql, versions)
        self.assertTrue(any("comment" in f.lower() for f in findings))
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 -m unittest test.test_preview_query_sql -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'preview_databricks_query_sql'`.

- [ ] **Step 3: Implement the preview tool**

Create `preview_databricks_query_sql.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest test.test_preview_query_sql -v`
Expected: all tests `OK`.

- [ ] **Step 5: Smoke-test the CLI**

Run:
```bash
python3 preview_databricks_query_sql.py \
  --table_versions_map cat.sch.dim_product=0-1,cat.sch.dim_product_version_map=0-1 \
  --data_type EVENT \
  --sql "SELECT dp.x AS event_type FROM cat.sch.dim_product AS dp LEFT JOIN cat.sch.dim_product_version_map AS m ON dp.id = m.id"
```
Expected: prints transformed SQL with both tables swapped for ``\`cat.sch.dim_product.<epoch>\``` and ``\`cat.sch.dim_product_version_map.<epoch>\``` (the substring table intact), then "none" findings, exit 0.

- [ ] **Step 6: Commit**

```bash
git add preview_databricks_query_sql.py test/test_preview_query_sql.py
git commit -m "Add local offline SQL preview tool"
```

---

## Task 4: On-cluster validation tool `validate_databricks_query.py`

Splits into a pure required-column contract (unit-tested) and a pyspark `main`
(run on the cluster). pyspark is imported lazily inside `main` so the pure helpers
import without Spark.

**Files:**
- Create: `validate_databricks_query.py`
- Create: `test/test_validate_query.py`

- [ ] **Step 1: Write the failing tests for the required-column contract**

Create `test/test_validate_query.py`:

```python
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_databricks_query import required_columns_for, check_required_columns


class TestRequiredColumnsFor(unittest.TestCase):

    def test_event_default_identity_is_user_id(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "user_id"},
            required_columns_for("EVENT", "USER_ID"),
        )

    def test_event_device_id_identity(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "device_id"},
            required_columns_for("EVENT", "DEVICE_ID"),
        )

    def test_event_both_identity(self):
        self.assertEqual(
            {"event_type", "event_properties", "time", "user_id", "device_id"},
            required_columns_for("EVENT", "USER_ID_AND_DEVICE_ID"),
        )

    def test_user_property_default(self):
        self.assertEqual(
            {"user_properties", "user_id"},
            required_columns_for("USER_PROPERTY", "USER_ID"),
        )

    def test_group_property_requires_groups_only_no_identity(self):
        self.assertEqual({"groups"}, required_columns_for("GROUP_PROPERTY", "USER_ID"))

    def test_warehouse_property_identity_only(self):
        self.assertEqual({"user_id"}, required_columns_for("WAREHOUSE_PROPERTY", "USER_ID"))


class TestCheckRequiredColumns(unittest.TestCase):

    def test_passes_when_all_present(self):
        missing = check_required_columns(
            ["event_type", "event_properties", "time", "user_id", "extra"],
            "EVENT", "USER_ID",
        )
        self.assertEqual([], missing)

    def test_reports_missing_event_type(self):
        missing = check_required_columns(
            ["event_properties", "time", "user_id"], "EVENT", "USER_ID",
        )
        self.assertEqual(["event_type"], missing)
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 -m unittest test.test_validate_query -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'validate_databricks_query'`.

- [ ] **Step 3: Implement the validation tool**

Create `validate_databricks_query.py`:

```python
"""
On-cluster, read-only validation of a Databricks import query.

Run as a SparkPythonTask on the customer's cluster (same environment as the real
unload job). It creates the temp views, runs spark.sql() for parse + analysis
(WITHOUT writing anything to S3), prints the resolved output schema, and checks
the required output columns for the data type. It never writes data.

The required-column contract mirrors Falcon's RequiredColumnsRule
(SQLTestSuite*Import.java + SQLUtils):
    EVENT             -> event_type, event_properties, time + identity
    USER_PROPERTY     -> user_properties + identity
    GROUP_PROPERTY    -> groups (no identity)
    WAREHOUSE_PROPERTY-> identity only
identity depends on the pipeline's record-identity setting:
    USER_ID (default) -> user_id ; DEVICE_ID -> device_id ;
    USER_ID_AND_DEVICE_ID -> user_id + device_id
"""
import argparse

from databricks_sql_utils import (
    parse_table_versions_map_arg,
    build_temp_view_name,
    replace_table_name_in_sql,
)

_BASE_REQUIRED = {
    "EVENT": {"event_type", "event_properties", "time"},
    "USER_PROPERTY": {"user_properties"},
    "GROUP_PROPERTY": {"groups"},
    "WAREHOUSE_PROPERTY": set(),
}
# GROUP_PROPERTY does not require identity columns (it keys on `groups`).
_DATA_TYPES_WITH_IDENTITY = {"EVENT", "USER_PROPERTY", "WAREHOUSE_PROPERTY"}


def _identity_columns(record_identity: str) -> set:
    if record_identity == "USER_ID_AND_DEVICE_ID":
        return {"user_id", "device_id"}
    if record_identity == "DEVICE_ID":
        return {"device_id"}
    return {"user_id"}  # USER_ID / default


def required_columns_for(data_type: str, record_identity: str) -> set:
    """Required output columns for a data type, mirroring Falcon's RequiredColumnsRule."""
    required = set(_BASE_REQUIRED[data_type])
    if data_type in _DATA_TYPES_WITH_IDENTITY:
        required |= _identity_columns(record_identity)
    return required


def check_required_columns(actual_columns, data_type: str, record_identity: str) -> list[str]:
    """Return the sorted list of required columns missing from actual_columns."""
    actual = set(actual_columns)
    return sorted(required_columns_for(data_type, record_identity) - actual)


def _transform_sql(sql, versions, spark, data_type, ingestion_in_mutability_mode):
    """Create a temp view per table at its end version and rewrite the SQL."""
    from pyspark.sql.functions import col  # lazy: only needed on-cluster

    transformed = sql
    for table, version_range in versions.items():
        ending_version = version_range[1]
        df = spark.sql(f"select * from {table} version as of {ending_version}")
        if not ingestion_in_mutability_mode and "_change_type" in df.columns:
            df = df.drop("_commit_version", "_commit_timestamp", "_change_type")
        view_name = build_temp_view_name(table)
        df.createOrReplaceTempView(view_name)
        transformed = replace_table_name_in_sql(transformed, table, view_name)
    return transformed


def main() -> int:
    from pyspark.sql import SparkSession  # lazy: only needed on-cluster

    parser = argparse.ArgumentParser(
        description="Validate a Databricks import query against the cluster (read-only, no S3 write)."
    )
    parser.add_argument("--table_versions_map", required=True,
                        help="e.g. catalog.schema.table=0-12,catalog2.schema2.table2=10-100")
    parser.add_argument("--data_type", required=True,
                        choices=["EVENT", "USER_PROPERTY", "GROUP_PROPERTY", "WAREHOUSE_PROPERTY"])
    parser.add_argument("--record-identity", default="USER_ID",
                        choices=["USER_ID", "DEVICE_ID", "USER_ID_AND_DEVICE_ID"],
                        help="record-identity setting of the pipeline (default USER_ID)")
    parser.add_argument("--ingestion_in_mutability_mode", action="store_true", default=False)
    parser.add_argument("--sample", type=int, default=0,
                        help="if > 0, show this many sample rows (read-only)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="the transformation SQL")
    group.add_argument("--sql-file", help="path to a file containing the transformation SQL")
    args, _ = parser.parse_known_args()

    sql = args.sql if args.sql is not None else open(args.sql_file, "r").read()
    versions = parse_table_versions_map_arg(args.table_versions_map)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set(
        "spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable",
        "endVersion",
    )

    transformed = _transform_sql(sql, versions, spark, args.data_type, args.ingestion_in_mutability_mode)
    print("=== Transformed SQL (this is what the job runs) ===")
    print(transformed)
    print()

    df = spark.sql(transformed)  # parse + analysis, no materialization
    actual_columns = df.columns
    print("=== Resolved output schema ===")
    df.printSchema()
    print()

    missing = check_required_columns(actual_columns, args.data_type, args.record_identity)
    if missing:
        print("=== Required-column check: FAIL ===")
        for column in missing:
            print(f"  - missing required column: {column}")
    else:
        print("=== Required-column check: PASS ===")

    if args.sample > 0:
        print(f"=== Sample ({args.sample} rows) ===")
        df.limit(args.sample).show(truncate=False)

    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m unittest test.test_validate_query -v`
Expected: all tests `OK`. (The pure helpers import with no pyspark, because pyspark is imported lazily inside `_transform_sql`/`main`.)

- [ ] **Step 5: Commit**

```bash
git add validate_databricks_query.py test/test_validate_query.py
git commit -m "Add on-cluster read-only query validation tool"
```

---

## Task 5: Resilience — endVersion conf (both scripts) + incompatible-schema retry (main script)

**Files:**
- Modify: `unload_databricks_data_to_s3.py`
- Modify: `unload_databricks_data_to_s3_partition.py`
- Create: `test/test_schema_resilience.py`

- [ ] **Step 1: Write the failing test for the new signature detector**

Create `test/test_schema_resilience.py`:

```python
import os
import sys
import types
import unittest


def _install_pyspark_stub() -> None:
    if "pyspark" in sys.modules:
        return
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    sqltypes = types.ModuleType("pyspark.sql.types")

    sql.SparkSession = object
    sql.DataFrame = object
    sql.Column = object
    functions.col = lambda *a, **k: None
    sql.functions = functions
    for name in ("StructType", "ArrayType", "MapType", "NullType", "DataType"):
        setattr(sqltypes, name, type(name, (), {}))

    pyspark.sql = sql
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions
    sys.modules["pyspark.sql.types"] = sqltypes


_install_pyspark_stub()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unload_databricks_data_to_s3 as mod


class TestIncompatibleSchemaSignature(unittest.TestCase):

    def test_matches_incompatible_data_schema(self):
        err = Exception(
            "[DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA] Retrieving table "
            "changes between version 31 and 53 failed because of an incompatible data schema."
        )
        self.assertEqual(
            "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
            mod.extract_incompatible_schema_error_signature(err),
        )

    def test_matches_incompatible_schema_change(self):
        err = Exception("oops [DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE] column renamed")
        self.assertEqual(
            "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            mod.extract_incompatible_schema_error_signature(err),
        )

    def test_returns_none_for_unrelated_error(self):
        self.assertIsNone(mod.extract_incompatible_schema_error_signature(Exception("network timeout")))

    def test_returns_none_for_empty(self):
        self.assertIsNone(mod.extract_incompatible_schema_error_signature(None))
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 -m unittest test.test_schema_resilience -v`
Expected: FAIL — `AttributeError: module 'unload_databricks_data_to_s3' has no attribute 'extract_incompatible_schema_error_signature'`.

- [ ] **Step 3: Add the signature constants + detector to `unload_databricks_data_to_s3.py`**

a) After the existing signature constants (lines 25-26: `MISSING_CDF_FILE_ERROR_SIGNATURE = ...` and `SPARK_DBR_FILE_NOT_EXIST_SIGNATURE = ...`), add:

```python
INCOMPATIBLE_SCHEMA_ERROR_SIGNATURES = (
    "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
    "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
)
```

b) Directly after the `extract_missing_cdf_error_signature` function (it ends at line 89 with `return None`), add:

```python
def extract_incompatible_schema_error_signature(error: Exception) -> Optional[str]:
    """
    Return a signature string if the exception indicates a Delta CDF incompatible
    schema change across the requested [start, end] version range.

    Databricks surfaces this under two codes depending on DBR/Delta version:
      - DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA: broader incompatibility,
        seen on DBR 15+ column-mapping tables (e.g. reading CDF from a checkpoint
        whose schema differs from the current table schema).
      - DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE: column-mapping renames.
    Both are unrecoverable for the [start, end) range; the caller recovers by
    re-reading the table at end-version only (latest-only).
    """
    message = str(error) if error else ""
    if not message:
        return None
    for signature in INCOMPATIBLE_SCHEMA_ERROR_SIGNATURES:
        if signature in message:
            return signature
    return None
```

- [ ] **Step 4: Run the test to verify the detector passes**

Run: `python3 -m unittest test.test_schema_resilience -v`
Expected: all 4 tests `OK`.

- [ ] **Step 5: Wire the detector into the per-table fallback**

In `unload_databricks_data_to_s3.py`, in `build_views_for_tables`, replace the
existing per-table `except` block (currently lines 362-377):

```python
        except Exception as fetch_error:
            fallback_signature = extract_missing_cdf_error_signature(fetch_error)
            if fallback_signature is None:
                # propagate non-CDF errors
                raise
            log_info(
                f"Encountered missing CDF files for {table} (signature={fallback_signature}). "
                f"Skipping versions {table_results[table]['initialStartVersion']}-{table_results[table]['initialEndVersion'] - 1} and re-reading at last known good version {ending_version}."
            )
            table_results[table]["initialFetchError"] = str(fetch_error)
            table_results[table]["finalStartVersion"] = ending_version
            table_results[table]["finalEndVersion"] = ending_version

            view_name = _fetch_and_create_view(ending_version, ending_version)
            sql_local = replace_table_name_in_sql(sql_local, table, view_name)
            log_info(f"Successfully read {table} at version {ending_version}.")
```

with (only the signature lookup and the log wording change):

```python
        except Exception as fetch_error:
            fallback_signature = (
                extract_missing_cdf_error_signature(fetch_error)
                or extract_incompatible_schema_error_signature(fetch_error)
            )
            if fallback_signature is None:
                # propagate errors we cannot recover by reading latest-only
                raise
            log_info(
                f"Encountered recoverable CDF error for {table} (signature={fallback_signature}). "
                f"Skipping versions {table_results[table]['initialStartVersion']}-{table_results[table]['initialEndVersion'] - 1} and re-reading at last known good version {ending_version}."
            )
            table_results[table]["initialFetchError"] = str(fetch_error)
            table_results[table]["finalStartVersion"] = ending_version
            table_results[table]["finalEndVersion"] = ending_version

            view_name = _fetch_and_create_view(ending_version, ending_version)
            sql_local = replace_table_name_in_sql(sql_local, table, view_name)
            log_info(f"Successfully read {table} at version {ending_version}.")
```

- [ ] **Step 6: Wire the detector into the top-level fallback**

In `unload_databricks_data_to_s3.py`, in the `__main__` block, replace the
existing top-level `except` (currently lines 553-561, the part before the
latest-only retry call):

```python
    except Exception as e:
        sig = extract_missing_cdf_error_signature(e)
        if sig is None:
            # non-CDF error: re-raise immediately
            raise
        log_info(
            f"Failed with CDF missing-file signature ({sig}). "
            f"Retrying with latest-only (start=end=end_version) for all tables."
        )
```

with:

```python
    except Exception as e:
        sig = (
            extract_missing_cdf_error_signature(e)
            or extract_incompatible_schema_error_signature(e)
        )
        if sig is None:
            # error we cannot recover by reading latest-only: re-raise
            raise
        log_info(
            f"Failed with recoverable CDF signature ({sig}). "
            f"Retrying with latest-only (start=end=end_version) for all tables."
        )
```

(Leave the `write_export_data_for_versions(... force_latest_only=True)` call that
follows unchanged.)

- [ ] **Step 7: Add the endVersion conf to `unload_databricks_data_to_s3.py`**

In the `__main__` block, immediately after `spark = SparkSession.builder.getOrCreate()`
(line 523) and before the `fs.s3a.*` conf lines, add:

```python
    # Read Delta Change Data Feed using the END-version schema when a column-mapping
    # table's schema changed across the requested version range. Without this, CDF
    # reads fail with DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA / _SCHEMA_CHANGE
    # on otherwise-compatible (e.g. additive) schema changes.
    spark.conf.set("spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable", "endVersion")
```

- [ ] **Step 8: Add the endVersion conf to `unload_databricks_data_to_s3_partition.py`**

In the `__main__` block, immediately after `spark = SparkSession.builder.getOrCreate()`
(line 156) and before the `fs.s3a.*` conf lines, add the identical block:

```python
    # Read Delta Change Data Feed using the END-version schema when a column-mapping
    # table's schema changed across the requested version range. Without this, CDF
    # reads fail with DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA / _SCHEMA_CHANGE
    # on otherwise-compatible (e.g. additive) schema changes.
    spark.conf.set("spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable", "endVersion")
```

- [ ] **Step 9: Run the resilience + regression tests**

Run: `python3 -m unittest test.test_schema_resilience test.test_shared_module_wiring test.test_export_dataframe_logging -v`
Expected: all `OK`.

- [ ] **Step 10: Commit**

```bash
git add unload_databricks_data_to_s3.py unload_databricks_data_to_s3_partition.py test/test_schema_resilience.py
git commit -m "Add CDF endVersion conf and incompatible-schema latest-only retry"
```

---

## Task 6: README usage docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace `README.md` with documented usage**

Overwrite `README.md` with:

```markdown
# databricks-import-pySpark-scripts

PySpark scripts that import data from Databricks into Amplitude (run by Falcon as
SparkPythonTask jobs via Git source).

## Scripts

- `unload_databricks_data_to_s3.py` — the production unload job.
- `unload_databricks_data_to_s3_partition.py` — unload variant with repartitioning.
- `databricks_sql_utils.py` — shared, pyspark-free SQL helpers used by all scripts
  and tools (table-name → temp-view substitution, etc.).

## Inspecting / testing a query

### Preview the transformed SQL locally (no Spark, no credentials)

The unload job rewrites every source table reference into a temp-view name before
running your SQL. Preview exactly what runs, and lint it, on your laptop:

    python3 preview_databricks_query_sql.py \
      --table_versions_map cat.sch.a=0-5,cat.sch.b=0-5 \
      --data_type EVENT \
      --sql-file my_query.sql

It prints the transformed SQL and structural warnings (unbalanced parens/quotes,
tables in the version map that aren't referenced, tables referenced that are
missing from the map, and `--` line-comment fragility). Exit code is non-zero if
there are findings.

### Validate against your cluster (read-only, never writes to S3)

Run as a SparkPythonTask on your Databricks cluster to actually parse/analyze the
query and check the required output columns:

    python3 validate_databricks_query.py \
      --table_versions_map cat.sch.a=0-5 \
      --data_type EVENT \
      --record-identity USER_ID \
      --sql-file my_query.sql \
      --sample 5

It creates the temp views, runs `spark.sql()` for parse + analysis (no write),
prints the resolved schema, and verifies the required columns for the data type:

| data_type | required columns |
|---|---|
| EVENT | `event_type`, `event_properties`, `time` + identity |
| USER_PROPERTY | `user_properties` + identity |
| GROUP_PROPERTY | `groups` |
| WAREHOUSE_PROPERTY | identity only |

`--record-identity` (default `USER_ID`) controls the identity columns:
`USER_ID` → `user_id`, `DEVICE_ID` → `device_id`, `USER_ID_AND_DEVICE_ID` → both.
Exit code is non-zero if a required column is missing.

## Resilience to table schema/version changes

The unload scripts set
`spark.databricks.delta.changeDataFeed.defaultSchemaModeForColumnMappingTable=endVersion`,
so reading the Change Data Feed across a column-mapping schema change uses the
end-version schema (handles compatible/additive changes transparently — no cluster
config needed).

If a table still hits `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA` or
`DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE` (e.g. drops/renames/type
changes), `unload_databricks_data_to_s3.py` automatically re-reads that table at
its end version only ("latest-only"). This keeps the run from failing; the
trade-off is that the changed-version range for that table is skipped (slightly
less data), which is logged in the run's `table_results.json`.

## Tests

    python3 -m unittest discover -s test -v
```

- [ ] **Step 2: Verify the documented commands actually work**

Run the preview command from the README against a quick inline SQL:
```bash
python3 preview_databricks_query_sql.py --table_versions_map cat.sch.a=0-5 --data_type EVENT --sql "SELECT a.x AS event_type FROM cat.sch.a AS a"
```
Expected: prints transformed SQL + "none" findings, exit 0.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Document query tools and CDF resilience in README"
```

---

## Task 7: Full suite + final verification

- [ ] **Step 1: Run the entire test suite**

Run: `cd /Users/ningwang/workspace/databricks-import-pySpark-scripts && python3 -m unittest discover -s test -v`
Expected: all tests `OK`. (Note: `test/unload_databricks_data_to_s3_tests.py` imports real pyspark; if pyspark is not installed locally it will error on import — run the pyspark-free suites explicitly instead: `python3 -m unittest test.test_table_name_substitution test.test_shared_module_wiring test.test_preview_query_sql test.test_validate_query test.test_schema_resilience test.test_export_dataframe_logging -v`.)

- [ ] **Step 2: Confirm no stray references to deleted local helpers**

Run: `grep -rn "_IDENTIFIER_CHAR" unload_databricks_data_to_s3.py unload_databricks_data_to_s3_partition.py TEST_unload_databricks_data_to_s3.py`
Expected: no output (the constant now lives only in `databricks_sql_utils.py`).

- [ ] **Step 3: Confirm the endVersion conf is present in both unload scripts**

Run: `grep -c "defaultSchemaModeForColumnMappingTable" unload_databricks_data_to_s3.py unload_databricks_data_to_s3_partition.py`
Expected: `1` for each file.

---

## Self-Review

**Spec coverage:**
- Shared module `databricks_sql_utils.py` → Task 1. ✓
- All entrypoints import it, copies deleted → Task 2. ✓
- Tool A `preview_databricks_query_sql.py` (transform + lint) → Task 3. ✓
- Tool B `validate_databricks_query.py` (views + parse + required-column check + `--record-identity` + `--sample`, no write) → Task 4. ✓
- Required-column contract per data type + identity → Task 4 (`required_columns_for`). ✓
- Feature 2(a) endVersion conf in both unload scripts → Task 5 steps 7-8. ✓
- Feature 2(b) incompatible-schema retry in main script only → Task 5 steps 3-6. ✓
- Tests: substitution retargeted, wiring guard, schema-signature, preview, validate → Tasks 1-5. ✓
- README usage docs → Task 6. ✓
- Single-PR-to-main constraint → stated in header. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; required-column values are concrete.

**Type/name consistency:** `transform_sql`/`lint_sql` (preview), `required_columns_for(data_type, record_identity)`/`check_required_columns(actual_columns, data_type, record_identity)` (validate), `extract_incompatible_schema_error_signature` (resilience) used identically in their tasks and tests. `_IDENTIFIER_CHAR`, `parse_table_versions_map_arg`, `build_temp_view_name`, `replace_table_name_in_sql` names match across module and importers.
