"""Extracts the sources of a transformation query from the SQL syntax tree.

This is the data flow counterpart of ``sql_parser.SQLParser``. It needs the optional ``dataflow`` extra
(sqlglot) and never raises: anything it cannot resolve becomes an ``unresolved`` source.
"""

import importlib.util
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlglot import exp

# Part of the cache token of a transformation, so a change in the extraction rules re-parses every query.
SQL_EXTRACTOR_VERSION = "1"

MISSING_DEPENDENCY_REASON = "sqlglot is not installed. Install with 'pip install \"cognite-toolkit[dataflow]\"'"

_FRAGMENT_LIMIT = 200
_PLACEHOLDER_RE = re.compile(r"\{\{.*?\}\}|\$\{.*?\}", re.DOTALL)
_SENTINEL_RE = re.compile(r"__tkph(\d+)__")
_FUNCTION_ARGUMENT_COUNT = {"cdf_nodes": 3, "cdf_edges": 3, "cdf_data_models": 4, "ext_onelake": 1}


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ParseStatus(str, Enum):
    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


class SourceKind(str, Enum):
    RAW_TABLE = "raw_table"
    CDF_RESOURCE = "cdf_resource"
    DM_NODES = "dm_nodes"
    DM_EDGES = "dm_edges"
    DM_DATA_MODEL = "dm_data_model"
    EXTERNAL_SOURCE = "external_source"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class SqlSource:
    kind: SourceKind
    confidence: Confidence = Confidence.HIGH
    # raw_table
    database: str | None = None
    table: str | None = None
    # cdf_resource: assets, events, timeseries, ...
    resource: str | None = None
    # dm_nodes, dm_edges and dm_data_model. A source without space and version is the whole function.
    space: str | None = None
    external_id: str | None = None
    version: str | None = None
    view: str | None = None
    # external_source
    source_id: str | None = None
    # unresolved
    fragment: str | None = None
    reason: str | None = None


@dataclass
class SqlSources:
    sources: list[SqlSource]
    parse_status: ParseStatus
    reason: str | None = None


def extract_sql_sources(query: str) -> SqlSources:
    if importlib.util.find_spec("sqlglot") is None:
        return _failed(query, MISSING_DEPENDENCY_REASON)
    try:
        return _extract(query)
    except Exception as error:
        message = str(error).splitlines()[0][:100] if str(error) else ""
        return _failed(query, f"{type(error).__name__}: {message}")


def _failed(query: str, reason: str) -> SqlSources:
    source = SqlSource(SourceKind.UNRESOLVED, Confidence.LOW, fragment=_shorten(query.strip()), reason=reason)
    return SqlSources([source], ParseStatus.FAILED, reason)


def _shorten(text: str) -> str:
    return text if len(text) <= _FRAGMENT_LIMIT else text[:_FRAGMENT_LIMIT] + "..."


def _extract(query: str) -> SqlSources:
    import sqlglot
    from sqlglot import exp

    # sqlglot does not fail on placeholders: {{ db }}.t parses as the table t. Swap them for a marker that
    # survives parsing, so a table built from a placeholder is reported as unresolved instead of as a wrong table.
    placeholders: list[str] = []

    def to_marker(match: re.Match[str]) -> str:
        placeholders.append(match.group(0))
        return f"__tkph{len(placeholders) - 1}__"

    def restore(text: str) -> str:
        return _SENTINEL_RE.sub(lambda m: placeholders[int(m.group(1))], text)

    trees = [tree for tree in sqlglot.parse(_PLACEHOLDER_RE.sub(to_marker, query), read="spark") if tree is not None]

    found: dict[SqlSource, None] = {}
    for tree in trees:
        cte_names = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
        for table in tree.find_all(exp.Table):
            source = _table_source(table, cte_names, restore)
            if source is not None:
                found[source] = None

    sources = sorted(found, key=lambda s: (s.kind.value, *(str(v) for v in _sort_values(s))))
    has_unresolved = any(source.kind is SourceKind.UNRESOLVED for source in sources)
    if not has_unresolved:
        return SqlSources(sources, ParseStatus.OK)
    has_resolved = any(source.kind is not SourceKind.UNRESOLVED for source in sources)
    reasons = sorted({source.reason for source in sources if source.reason})
    return SqlSources(sources, ParseStatus.PARTIAL if has_resolved else ParseStatus.FAILED, "; ".join(reasons))


def _sort_values(source: SqlSource) -> tuple[str | None, ...]:
    return (
        source.database,
        source.table,
        source.resource,
        source.space,
        source.external_id,
        source.version,
        source.view,
        source.source_id,
        source.fragment,
    )


def _table_source(table: "exp.Table", cte_names: set[str], restore: Callable[[str], str]) -> SqlSource | None:
    from sqlglot import exp

    inner = table.this
    if isinstance(inner, exp.Func):
        return _function_source(inner, restore)
    parts = [part for part in (table.catalog, table.db, table.name) if part]
    if not table.db and table.name in cte_names:
        return None
    if any(_SENTINEL_RE.search(part) for part in parts):
        return _unresolved(restore(".".join(parts)), "The table name is built from a placeholder")
    if table.db == "_cdf":
        return SqlSource(SourceKind.CDF_RESOURCE, resource=table.name)
    if not table.db:
        return _unresolved(table.name, "The table has no database")
    # A catalog prefix (cat.db.t) is not a CDF concept. The database and table are still the best match.
    confidence = Confidence.MEDIUM if table.catalog else Confidence.HIGH
    return SqlSource(SourceKind.RAW_TABLE, confidence, database=table.db, table=table.name)


def _function_source(function: "exp.Func", restore: Callable[[str], str]) -> SqlSource:
    from sqlglot import exp

    name = (function.name if isinstance(function, exp.Anonymous) else function.sql_name()).lower()
    fragment = _shorten(restore(function.sql(dialect="spark")))
    if name not in _FUNCTION_ARGUMENT_COUNT:
        return _unresolved(fragment, f"Unknown table function {name!r}")
    arguments = function.expressions
    if not arguments and name != "ext_onelake":
        # cdf_nodes() without arguments reads every node.
        kind = {"cdf_nodes": SourceKind.DM_NODES, "cdf_edges": SourceKind.DM_EDGES}.get(name)
        if kind is not None:
            return SqlSource(kind, Confidence.MEDIUM)
        return _unresolved(fragment, f"{name}() without arguments")
    literals = [arg.this for arg in arguments if isinstance(arg, exp.Literal) and arg.is_string]
    if len(literals) != len(arguments) or any(_SENTINEL_RE.search(value) for value in literals):
        return _unresolved(fragment, f"Arguments of {name}() are not constant strings")
    if len(literals) < _FUNCTION_ARGUMENT_COUNT[name]:
        return _unresolved(fragment, f"Too few arguments for {name}()")
    if name == "ext_onelake":
        return SqlSource(SourceKind.EXTERNAL_SOURCE, source_id=literals[0])
    if name == "cdf_data_models":
        return SqlSource(
            SourceKind.DM_DATA_MODEL,
            space=literals[0],
            external_id=literals[1],
            version=literals[2],
            view=literals[3],
        )
    kind = SourceKind.DM_NODES if name == "cdf_nodes" else SourceKind.DM_EDGES
    return SqlSource(kind, space=literals[0], external_id=literals[1], version=literals[2])


def _unresolved(fragment: str, reason: str) -> SqlSource:
    return SqlSource(SourceKind.UNRESOLVED, Confidence.LOW, fragment=fragment, reason=reason)
