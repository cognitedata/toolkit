import sys
import tempfile
import time
from collections.abc import Hashable, Iterator
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Literal, overload
from urllib.parse import urlparse

from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    ClientCredentials,
    OidcCredentials,
)
from cognite.client.data_classes.data_modeling import Edge, Node, ViewId
from cognite.client.data_classes.filters import SpaceFilter
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from filelock import BaseFileLock, FileLock, Timeout
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.constants import ENV_VAR_PATTERN, MAX_ROW_ITERATION_RUN_QUERY, MAX_RUN_QUERY_FREQUENCY_MIN
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitError,
    ToolkitRequiredValueError,
    ToolkitThrottledError,
    ToolkitTypeError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    MediumSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .file import to_directory_compatible
from .sql_parser import SQLParser

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


def try_find_error(credentials: OidcCredentials | ClientCredentials | None) -> str | None:
    if credentials is None:
        return None
    missing: list[str] = []
    if client_id := ENV_VAR_PATTERN.match(credentials.client_id):
        missing.append(client_id.group(1))
    if client_secret := ENV_VAR_PATTERN.match(credentials.client_secret):
        missing.append(client_secret.group(1))
    if missing:
        plural = "s are" if len(missing) > 1 else " is"
        return f"The environment variable{plural} not set: {humanize_collection(missing)}."
    if isinstance(credentials, ClientCredentials):
        return None
    try:
        result = urlparse(credentials.token_uri)
        if not all([result.scheme, result.netloc]):
            raise ValueError
    except ValueError:
        return f"The tokenUri {credentials.token_uri!r} is not a valid URI."
    return None


@overload
def iterate_instances(
    client: ToolkitClient,
    instance_type: Literal["node"] = "node",
    space: str | SequenceNotStr[str] | None = None,
    source: ViewId | None = None,
    console: Console | None = None,
) -> Iterator[Node]: ...


@overload
def iterate_instances(
    client: ToolkitClient,
    instance_type: Literal["edge"],
    space: str | SequenceNotStr[str] | None = None,
    source: ViewId | None = None,
    console: Console | None = None,
) -> Iterator[Edge]: ...


def iterate_instances(
    client: ToolkitClient,
    instance_type: Literal["node", "edge"] = "node",
    space: str | SequenceNotStr[str] | None = None,
    source: ViewId | None = None,
    console: Console | None = None,
) -> Iterator[Node] | Iterator[Edge]:
    """Toolkit specific implementation of the client.data_modeling.instances(...) method to account for 408.

    In addition, we enforce sort based on the argument below (provided by Alex B.).
    """
    body: dict[str, Any] = {"limit": 1_000, "cursor": None, "instanceType": instance_type}
    # Without a sort, the sort is implicitly by the internal id, as cursoring needs a stable sort.
    # By making the sort be on external_id, Postgres should pick the index that's on (project_id, space, external_id)
    # WHERE deleted_at IS NULL. In other words, avoiding soft deleted instances.
    body["sort"] = [
        {
            "property": [instance_type, "externalId"],
            "direction": "ascending",
        }
    ]
    url = f"/api/{client._API_VERSION}/projects/{client.config.project}/models/instances/list"
    if space:
        body["filter"] = SpaceFilter(space=space, instance_type=instance_type).dump()
    if source:
        body["sources"] = [{"source": source.dump(include_type=True, camel_case=True)}]
    while True:
        try:
            response = client.post(url=url, json=body)
        except CogniteAPIError as e:
            if e.code == 408 and body["limit"] > 1:
                MediumSeverityWarning(
                    f"Timeout with limit {body['limit']}, retrying with {body['limit'] // 2}."
                ).print_warning(include_timestamp=True, console=console)
                body["limit"] = body["limit"] // 2
                continue
            raise e
        response_body = response.json()
        if instance_type == "node":
            yield from (Node.load(node) for node in response_body["items"])
        else:
            yield from (Edge.load(edge) for edge in response_body["items"])
        next_cursor = response_body.get("nextCursor")
        if next_cursor is None:
            break
        body["cursor"] = next_cursor


@overload
def read_auth(
    authentication: object,
    client_config: ToolkitClientConfig,
    identifier: Hashable,
    resource_name: str,
    allow_oidc: Literal[False] = False,
    console: Console | None = None,
) -> ClientCredentials: ...


@overload
def read_auth(
    authentication: object,
    client_config: ToolkitClientConfig,
    identifier: Hashable,
    resource_name: str,
    allow_oidc: Literal[True],
    console: Console | None = None,
) -> ClientCredentials | OidcCredentials: ...


def read_auth(
    authentication: object,
    client_config: ToolkitClientConfig,
    identifier: Hashable,
    resource_name: str,
    allow_oidc: bool = False,
    console: Console | None = None,
) -> ClientCredentials | OidcCredentials:
    if authentication is None:
        if client_config.is_strict_validation or not isinstance(client_config.credentials, OAuthClientCredentials):
            raise ToolkitRequiredValueError(f"Authentication is missing for {resource_name} {identifier!r}.")
        else:
            HighSeverityWarning(
                f"Authentication is missing for {resource_name} {identifier!r}. Falling back to the Toolkit credentials"
            ).print_warning(console=console)
        return ClientCredentials(client_config.credentials.client_id, client_config.credentials.client_secret)
    elif not isinstance(authentication, dict):
        raise ToolkitTypeError(f"Authentication must be a dictionary for {resource_name} {identifier!r}")
    elif "clientId" not in authentication or "clientSecret" not in authentication:
        raise ToolkitRequiredValueError(
            f"Authentication must contain clientId and clientSecret for {resource_name} {identifier!r}"
        )
    elif allow_oidc and "tokenUri" in authentication and "cdfProjectName" in authentication:
        return OidcCredentials.load(authentication)
    else:
        return ClientCredentials(authentication["clientId"], authentication["clientSecret"])


def get_transformation_sources(query: str) -> list[RawTable | str]:
    """Search the SQL query for source tables."""
    parser = SQLParser(query, operation="Lookup transformation source")
    parser.parse()

    tables: list[RawTable | str] = []
    for table in parser.sources:
        if table.schema == "_cdf":
            tables.append(table.name)
        else:
            tables.append(RawTable(db_name=table.schema, table_name=table.name))
    return tables


def get_transformation_destination_columns(query: str) -> list[str]:
    """Search the SQL query for destination columns."""
    parser = SQLParser(query, operation="Lookup transformation destination columns")
    parser.parse()
    return parser.destination_columns


def metadata_key_counts(
    client: ToolkitClient,
    resource: Literal["assets", "events", "files", "timeseries", "sequences"],
    data_sets: list[int] | None = None,
    hierarchies: list[int] | None = None,
) -> list[tuple[str, int]]:
    """Get the metadata key counts for a given resource.

    Args:
        client: ToolkitClient instance
        resource: The resource to get the metadata key counts for. Can be one of "assets", "events", "files", "timeseries", or "sequences".
        data_sets: A list of data set IDs to filter by. If None, no filtering is applied.
        hierarchies: A list of hierarchy IDs to filter by. If None, no filtering is applied.

    Returns:
        A dictionary with the metadata keys as keys and the counts as values.
    """
    if hierarchies and resource != "assets":
        raise ToolkitValueError(
            f"Hierarchies filtering for metadata keys are only supported for assets, but {resource} was provided."
        )
    where_clause = _create_where_clause(data_sets, hierarchies)

    query = f"""WITH meta AS (
         SELECT cast_to_strings(metadata) AS metadata_array
         FROM _cdf.{resource}{where_clause}
       ),
       exploded AS (
         SELECT explode(metadata_array) AS json_str
         FROM meta
       ),
       parsed AS (
         SELECT from_json(json_str, 'map<string,string>') AS json_map
         FROM exploded
       ),
       keys_extracted AS (
         SELECT map_keys(json_map) AS keys_array
         FROM parsed
       ),
       all_keys AS (
         SELECT explode(keys_array) AS key
         FROM keys_extracted
       )
       SELECT key, COUNT(key) AS key_count
       FROM all_keys
       GROUP BY key
       ORDER BY key_count DESC;
"""
    results = client.transformations.preview(query, convert_to_string=False, limit=None, source_limit=None)
    return [(item["key"], item["key_count"]) for item in results.results or []]


def _create_where_clause(data_sets: list[int] | None, hierarchies: list[int] | None) -> str:
    conditions = []
    if data_sets:
        if not all(isinstance(item, int) for item in data_sets):
            raise ToolkitValueError("All items in data_sets must be integers for SQL filtering.")
        conditions.append(f"dataSetId IN ({','.join(map(str, data_sets))})")
    if hierarchies:
        if not all(isinstance(item, int) for item in hierarchies):
            raise ToolkitValueError("All items in hierarchies must be integers for SQL filtering.")
        conditions.append(f"rootId IN ({','.join(map(str, hierarchies))})")

    if not conditions:
        return ""
    return f"\n         WHERE {' AND '.join(conditions)}"


def label_count(
    client: ToolkitClient,
    resource: Literal["assets", "events", "files", "timeseries", "sequences"],
    data_sets: list[int] | None = None,
    hierarchies: list[int] | None = None,
) -> list[tuple[str, int]]:
    """Get the label counts for a given resource.

    Args:
        client: ToolkitClient instance
        resource: The resource to get the label counts for. Can be one of "assets", "events", "files", "timeseries", or "sequences".
        data_sets: A list of data set IDs to filter by. If None, no filtering is applied.
        hierarchies: A list of hierarchy IDs to filter by. If None, no filtering is applied.

    Returns:
        A list of tuples with the label and its count.
    """
    if hierarchies and resource != "assets":
        raise ToolkitValueError(
            f"Hierarchies filtering for labels are only supported for assets, but {resource} was provided."
        )
    where_clause = _create_where_clause(data_sets, hierarchies)

    query = f"""WITH labels as (SELECT explode(labels) AS label
	FROM _cdf.{resource}{where_clause}
  )
SELECT label, COUNT(label) as label_count
FROM labels
GROUP BY label
ORDER BY label_count DESC;
"""
    results = client.transformations.preview(query, convert_to_string=False, limit=None, source_limit=None)
    # We know from the SQL that the result is a list of dictionaries with string keys and int values.
    return [(item["label"], item["label_count"]) for item in results.results or []]


@dataclass
class RelationshipCount:
    source_type: str
    target_type: str
    count: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(
            source_type=data["sourceType"],
            target_type=data["targetType"],
            count=int(data["relationshipCount"]),
        )


def relationship_aggregate_count(client: ToolkitClient, data_sets: list[int] | None = None) -> list[RelationshipCount]:
    """Get the relationship counts for the asset-centric resources.

    Args:
        client: ToolkitClient instance
        data_sets: A list of data set IDs to filter by. If None, no filtering is applied.

    """
    where_clause = ""
    if data_sets is not None:
        where_clause = f"\n         WHERE dataSetId IN ({','.join(map(str, data_sets))})"

    query = f"""SELECT
    sourceType,
    targetType,
    COUNT(externalId) AS relationshipCount
FROM
    _cdf.relationships{where_clause}
GROUP BY
    sourceType,
    targetType
"""
    results = client.transformations.preview(query, convert_to_string=False, limit=None, source_limit=None)
    return [
        RelationshipCount(item["sourceType"], item["targetType"], item["relationshipCount"])
        for item in results.results or []
    ]


def label_aggregate_count(client: ToolkitClient, data_sets: list[int] | None = None) -> int:
    """Get the total count of labels in the CDF project.

    Args:
        client: ToolkitClient instance
        data_sets: A list of data set IDs to filter by. If None, no filtering is applied.

    Returns:
        The total count of labels across all resources in the CDF project.
    """
    where_clause = ""
    if data_sets is not None:
        where_clause = f"\n         WHERE dataSetId IN ({','.join(map(str, data_sets))})"

    query = f"""SELECT
    COUNT(externalId) AS labelCount
FROM
    _cdf.labels{where_clause}"""

    results = client.transformations.preview(query, convert_to_string=False, limit=None, source_limit=None)
    if results.results:
        return int(results.results[0]["labelCount"])
    return 0


@dataclass
class ThrottlerState:
    _FILENAME = "tk-last-raw_count-{project}.txt"

    lock: BaseFileLock
    project: str
    _allowed_this_run: bool | None = None

    @classmethod
    def get(cls, project: str) -> "ThrottlerState":
        if project in _STATE_BY_PROJECT:
            return _STATE_BY_PROJECT[project]

        with _STATE_LOCK:
            # Re-check after acquiring the lock to handle the race condition.
            if project in _STATE_BY_PROJECT:
                return _STATE_BY_PROJECT[project]
            filepath = cls._filepath(project)
            # filelock needs a string path
            lock = FileLock(str(filepath.with_suffix(".lock")), timeout=10.0)
            state = cls(lock=lock, project=project)
            _STATE_BY_PROJECT[project] = state
            return state

    @classmethod
    def _filepath(cls, project: str) -> Path:
        # Enure the project name is a valid string for hashing.
        project = to_directory_compatible(project)
        filename = cls._FILENAME.format(project=project)
        return Path(tempfile.gettempdir()) / filename

    def throttle(self) -> None:
        """Checks if the function can be run, and if so, updates the last call timestamp.

        This is an atomic operation protected by a file lock.

        Raises:
            ToolkitThrottledError: If the function has been called too recently.
        """
        try:
            with self.lock:
                now = time.time()
                filepath = self._filepath(self.project)
                if self._allowed_this_run is None:
                    last_call_epoch = 0.0
                    if filepath.exists():
                        try:
                            last_call_epoch = float(filepath.read_text(encoding="utf-8"))
                        except (ValueError, FileNotFoundError):
                            # File is corrupt or was deleted after check. Allow to run and overwrite.
                            pass

                    to_wait = (MAX_RUN_QUERY_FREQUENCY_MIN * 60) - (now - last_call_epoch)
                    if to_wait > 0:
                        raise ToolkitThrottledError(
                            f"Row count is limited to once every {MAX_RUN_QUERY_FREQUENCY_MIN} minutes. "
                            f"Please wait {to_wait:.2f} seconds before calling again.",
                            to_wait,
                        )
                    self._allowed_this_run = True

                # We are allowed to run, so we update the timestamp.
                filepath.write_text(str(now), encoding="utf-8")
        except Timeout:
            raise ToolkitError(
                "Could not acquire lock for throttling raw row count. "
                "Another toolkit process might be running. Please try again."
            )


_STATE_LOCK = RLock()
_STATE_BY_PROJECT: dict[str, ThrottlerState] = {}


def raw_row_count(client: ToolkitClient, raw_table_id: RawTable, max_count: int = MAX_ROW_ITERATION_RUN_QUERY) -> int:
    """Get the number of rows in a raw table.

    Args:
        client: ToolkitClient instance
        raw_table_id: The ID of the raw table to count rows in.
        max_count: The maximum number of rows to count. If the table has more rows than this, the count will be capped.

    Returns:
        The number of rows in the raw table.

    Raises:
        ToolkitThrottledError: If the row count is limited to once every MAX_RUN_QUERY_FREQUENCY_MIN minutes.
        ValueError: If max_count is not between 0 and MAX_ROW_ITERATION_RUN_QUERY (inclusive).
    """
    ThrottlerState.get(client.config.project).throttle()

    if not 0 <= max_count <= MAX_ROW_ITERATION_RUN_QUERY:
        raise ValueError(f"max_count must be between 0 and {MAX_ROW_ITERATION_RUN_QUERY} (inclusive).")

    query = f"""SELECT COUNT(key) AS row_count
FROM (
    SELECT key
    FROM `{raw_table_id.db_name}`.`{raw_table_id.table_name}`
    LIMIT {max_count}
) AS limited_keys"""

    results = client.transformations.preview(query, convert_to_string=False, limit=None, source_limit=None)
    if results.results:
        return int(results.results[0]["row_count"])
    return 0
