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
