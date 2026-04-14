# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import random
import re
import time
import warnings
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Sequence
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, final

from cognite.client.data_classes import (
    ClientCredentials,
    OidcCredentials,
)
from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import (
    ExternalId,
    InternalId,
    RawDatabaseId,
    RawTableId,
    TransformationNotificationId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    TransformationFilter,
    TransformationNotificationFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DataModelId,
    SpaceId,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    ScopeDefinition,
    TransformationsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    NonceCredentials,
    TransformationRequest,
    TransformationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_notification import (
    TransformationNotificationRequest,
    TransformationNotificationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import (
    TransformationScheduleRequest,
    TransformationScheduleResponse,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ToolkitFileNotFoundError,
    ToolkitInvalidParameterNameError,
    ToolkitNotSupported,
    ToolkitRequiredValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import (
    calculate_hash,
    calculate_secure_hash,
    humanize_collection,
    in_dict,
    load_yaml_inject_variables,
    quote_int_value_by_key_in_yaml,
    safe_read,
    sanitize_filename,
)
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.utils.cdf import read_auth, try_find_error
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable
from cognite_toolkit._cdf_tk.yaml_classes import (
    TransformationNotificationYAML,
    TransformationScheduleYAML,
    TransformationYAML,
)
from cognite_toolkit._cdf_tk.yaml_classes.transformation_destination import (
    DataModelSource,
    RawDataSource,
    ViewDataSource,
)

from .auth import GroupAllScopedCRUD
from .data_organization import DataSetsIO
from .datamodel import DataModelIO, SpaceCRUD, ViewIO
from .group_scoped import GroupResourceScopedCRUD
from .raw import RawDatabaseCRUD, RawTableCRUD

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildVariable


@final
class TransformationIO(ResourceIO[ExternalId, TransformationRequest, TransformationResponse]):
    folder_name = "transformations"
    resource_cls = TransformationResponse
    resource_write_cls = TransformationRequest
    kind = "Transformation"
    yaml_cls = TransformationYAML
    dependencies = frozenset(
        {
            DataSetsIO,
            RawDatabaseCRUD,
            GroupAllScopedCRUD,
            SpaceCRUD,
            ViewIO,
            DataModelIO,
            RawTableCRUD,
            RawDatabaseCRUD,
            GroupResourceScopedCRUD,
        }
    )
    _doc_url = "Transformations/operation/createTransformations"
    _hash_key = "-- cdf-auth"

    # The API supports creating/updating 1000 transformations in a single request,
    # however, when the transformation has credentials, the session API times out on
    # larger number of transformations. Thus, we use a conservative batch size.
    _BATCH_SIZE = 20  # The maximum number of transformations to create in a single batch

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None):
        super().__init__(client, build_dir, console)
        self._authentication_by_id_operation: dict[
            tuple[str, Literal["read", "write"]], OidcCredentials | ClientCredentials
        ] = {}

    @property
    def display_name(self) -> str:
        return "transformations"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[TransformationRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield TransformationsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: TransformationResponse | TransformationRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("Transformation must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: TransformationResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if item.id is None:
            raise ToolkitRequiredValueError("Transformation must have id set.")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsIO, ExternalId(external_id=item["dataSetExternalId"])
        if destination := item.get("destination", {}):
            if not isinstance(destination, dict):
                return
            if destination.get("type") == "raw" and in_dict(("database", "table"), destination):
                yield RawDatabaseCRUD, RawDatabaseId(name=destination["database"])
                yield RawTableCRUD, RawTableId(db_name=destination["database"], name=destination["table"])
            elif destination.get("type") in ("nodes", "edges") and (view := destination.get("view", {})):
                if space := destination.get("instanceSpace"):
                    yield SpaceCRUD, SpaceId(space=space)
                if in_dict(("space", "externalId", "version"), view):
                    view["version"] = str(view["version"])
                    yield ViewIO, ViewId.model_validate(view)
            elif destination.get("type") == "instances":
                if space := destination.get("instanceSpace"):
                    yield SpaceCRUD, SpaceId(space=space)
                if data_model := destination.get("dataModel"):
                    if in_dict(("space", "externalId", "version"), data_model):
                        data_model["version"] = str(data_model["version"])
                        yield DataModelIO, DataModelId.model_validate(data_model)

    @classmethod
    def get_dependencies(cls, resource: TransformationYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.data_set_external_id)
        if destination := resource.destination:
            if isinstance(destination, RawDataSource):
                yield RawDatabaseCRUD, RawDatabaseId(name=destination.database)
                yield RawTableCRUD, RawTableId(db_name=destination.database, name=destination.table)
            elif isinstance(destination, ViewDataSource):
                if destination.instance_space:
                    yield SpaceCRUD, SpaceId(space=destination.instance_space)
                if destination.view:
                    yield (
                        ViewIO,
                        ViewId(
                            space=destination.view.space,
                            external_id=destination.view.external_id,
                            version=destination.view.version,
                        ),
                    )
            elif isinstance(destination, DataModelSource):
                if destination.instance_space:
                    yield SpaceCRUD, SpaceId(space=destination.instance_space)
                yield (
                    DataModelIO,
                    DataModelId(
                        space=destination.data_model.space,
                        external_id=destination.data_model.external_id,
                        version=destination.data_model.version,
                    ),
                )

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: ExternalId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        """Get extra files for a Transformation resource.

        This includes an optional .sql file with the query.
        """
        # Check if queryFile is specified in the YAML
        query_file: Path | None = None
        if "queryFile" in item:
            query_file = filepath.parent / Path(item["queryFile"])
        else:
            # Check for conventional file names: {stem}.sql or {external_id}.sql
            sql_candidates = [
                filepath.parent / f"{filepath.stem}.sql",
                filepath.parent / f"{identifier.external_id}.sql",
            ]
            query_file = next((p for p in sql_candidates if p.exists()), None)

        if query_file is None or not query_file.exists():
            # No external SQL file - query might be inline, which is valid
            return

        content = safe_read(query_file, encoding=BUILD_FOLDER_ENCODING)
        source_hash = calculate_hash(content, shorten=True)
        yield SuccessExtra(
            source_path=query_file,
            source_hash=source_hash,
            suffix=".sql",
            content=content,
            description="transformation query",
        )

    @classmethod
    def substitute_variables_content(cls, content: str, variables: "list[BuildVariable]") -> str:
        """Overwritten to handle the query field that needs .sql style substitution."""
        # avoid circular import
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import FileSuffix

        for variable in variables:
            file_suffix: FileSuffix = ".sql" if cls._is_in_query_field(content, variable.name) else ".yaml"
            pattern, replace = variable.get_pattern_replace_pair(file_suffix)
            content = re.sub(pattern, replace, content)
        return content

    @staticmethod
    def _is_in_query_field(content: str, variable_key: str) -> bool:
        """Check if a variable is within a query field in YAML.

        Assumes query is a top-level property. This detects various YAML formats:
        - query: >-
        - query: |
        - query: "..."
        - query: ...
        """
        lines = content.split("\n")
        variable_pattern = rf"{{{{\s*{re.escape(variable_key)}\s*}}}}"
        in_query_field = False

        for line in lines:
            # Check if this line starts a top-level query field
            query_match = re.match(r"^query\s*:\s*(.*)$", line)
            if query_match:
                in_query_field = True
                query_content_start = query_match.group(1).strip()

                # Check if variable is on the same line as query: declaration
                if re.search(variable_pattern, line):
                    return True

                # If query content starts on same line (not a block scalar), check it
                if query_content_start and not query_content_start.startswith(("|", ">", "|-", ">-", "|+", ">+")):
                    if re.search(variable_pattern, query_content_start):
                        return True
                continue

            # Check if we're still in the query field
            if in_query_field:
                # If we hit another top-level property, we've exited the query field
                if re.match(r"^\w+\s*:", line):
                    in_query_field = False
                    continue

                # We're still in the query field, check for variable
                if re.search(variable_pattern, line):
                    return True

        return False

    @classmethod
    def safe_read(cls, filepath: Path | str) -> str:
        # If the destination is a DataModel or a View we need to ensure that the version is a string
        return quote_int_value_by_key_in_yaml(safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="version")

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )

        raw_list = resources if isinstance(resources, list) else [resources]
        for item in raw_list:
            query_file: Path | None = None
            if "queryFile" in item:
                if filepath is None:
                    raise ValueError("filepath must be set if queryFile is set")
                query_file = filepath.parent / Path(item.pop("queryFile"))

            external_id = self.get_id(item).external_id
            if query_file is None and "query" not in item:
                if filepath is None:
                    raise ValueError("filepath must be set if query is not set")
                warning = HighSeverityWarning(
                    f"query property or is missing in {filepath.as_posix()!r}. It can be inline or a separate file named {filepath.stem}.sql or {external_id}.sql",
                )
                warning.print_warning(console=self.console)
            elif query_file and not query_file.exists():
                # We checked above that filepath is not None
                raise ToolkitFileNotFoundError(f"Query file {query_file.as_posix()} not found", filepath)
            elif query_file and "query" in item:
                raise ToolkitYAMLFormatError(
                    f"query property is ambiguously defined in both the yaml file and a separate file named {query_file}\n"
                    f"Please remove one of the definitions, either the query property in {filepath} or the file {query_file}",
                    filepath,
                )
            elif query_file:
                item["query"] = safe_read(query_file, encoding=BUILD_FOLDER_ENCODING)

            auth_dict: dict[str, Any] = {}
            if "authentication" in item:
                auth_dict["authentication"] = item["authentication"]
            if auth_dict:
                auth_hash = calculate_secure_hash(auth_dict, shorten=True)
                if "query" in item:
                    hash_str = f"{self._hash_key}: {auth_hash}"
                    if not item["query"].startswith(self._hash_key):
                        item["query"] = f"{hash_str}\n{item['query']}"

            if "sourceOidcCredentials" in item:
                raise ToolkitNotSupported(
                    "The property 'sourceOidcCredentials' is not supported. Use 'authentication.read' instead."
                )

            if "destinationOidcCredentials" in item:
                raise ToolkitNotSupported(
                    "The property 'destinationOidcCredentials' is not supported. Use 'authentication.write' instead."
                )

            if "sourceNonce" in item:
                raise ToolkitNotSupported(
                    "The property 'sourceNonce' is not supported by Toolkit. Use 'authentication.read' instead,"
                    "then Toolkit will dynamically set the nonce."
                )

            if "destinationNonce" in item:
                raise ToolkitNotSupported(
                    "The property 'destinationNonce' is not supported by Toolkit. Use 'authentication.write' instead,"
                    "then Toolkit will dynamically set the nonce."
                )
            auth = item.pop("authentication", None)
            if isinstance(auth, dict) and "read" in auth:
                self._authentication_by_id_operation[(external_id, "read")] = read_auth(
                    auth["read"],
                    self.client.config,
                    external_id,
                    "transformation",
                    allow_oidc=True,
                    console=self.console,
                )
            elif isinstance(auth, dict) and "write" not in auth:
                self._authentication_by_id_operation[(external_id, "read")] = read_auth(
                    auth,
                    self.client.config,
                    external_id,
                    "transformation",
                    allow_oidc=True,
                    console=self.console,
                )
            if isinstance(auth, dict) and "write" in auth:
                self._authentication_by_id_operation[(external_id, "write")] = read_auth(
                    auth["write"],
                    self.client.config,
                    external_id,
                    "transformation",
                    allow_oidc=True,
                    console=self.console,
                )
            elif isinstance(auth, dict) and "read" not in auth:
                self._authentication_by_id_operation[(external_id, "write")] = read_auth(
                    auth,
                    self.client.config,
                    external_id,
                    "transformation",
                    allow_oidc=True,
                    console=self.console,
                )
        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> TransformationRequest:
        invalid_parameters: dict[str, str] = {}
        if "action" in resource and "conflictMode" not in resource:
            invalid_parameters["action"] = "conflictMode"
        if "shared" in resource and "isPublic" not in resource:
            invalid_parameters["shared"] = "isPublic"
        if invalid_parameters:
            raise ToolkitInvalidParameterNameError(
                "Parameters invalid. These are specific for the "
                "'transformation-cli' and not supported by cognite-toolkit",
                resource.get("externalId", "<Missing>"),
                invalid_parameters,
            )

        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if "conflictMode" not in resource:
            resource["conflictMode"] = "upsert"
        if "ignoreNullFields" not in resource:
            resource["ignoreNullFields"] = True
        return TransformationRequest.model_validate(resource)

    def dump_resource(self, resource: TransformationResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if "isPublic" in dumped and "isPublic" not in local:
            # Default set from server side.
            dumped.pop("isPublic")
        if "authentication" in local:
            # The hash added to the beginning of the query detects the change in the authentication
            dumped["authentication"] = local["authentication"]
        cdf_destination = dumped.get("destination", {})
        local_destination = local.get("destination", {})
        if isinstance(cdf_destination, dict) and isinstance(local_destination, dict):
            if cdf_destination.get("instanceSpace") is None and "instanceSpace" not in local_destination:
                cdf_destination.pop("instanceSpace", None)
        if not dumped.get("query") and "query" not in local:
            dumped.pop("query", None)
        if dumped.get("conflictMode") == "upsert" and "conflictMode" not in local:
            # Default set from server side.
            dumped.pop("conflictMode", None)
        return dumped

    def split_resource(
        self, base_filepath: Path, resource: dict[str, Any]
    ) -> Iterable[tuple[Path, dict[str, Any] | str]]:
        if query := resource.pop("query", None):
            yield base_filepath.with_suffix(".sql"), cast(str, query)

        yield base_filepath, resource

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[-1] == "scopes":
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)

    def create(self, items: Sequence[TransformationRequest]) -> list[TransformationResponse]:
        return self._execute_in_batches(items, self.client.tool.transformations.create)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[TransformationResponse]:
        return self.client.tool.transformations.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[TransformationRequest]) -> list[TransformationResponse]:
        def update(transformations: Sequence[TransformationRequest]) -> list[TransformationResponse]:
            return self.client.tool.transformations.update(list(transformations), mode="replace")

        return self._execute_in_batches(items, update)

    def _create_auth_creation_error(self, items: Sequence[TransformationRequest]) -> ResourceCreationError | None:
        hints_by_id: dict[str, list[str]] = defaultdict(list)
        for item in items:
            if not item.external_id:
                continue
            read_credentials = self._authentication_by_id_operation.get((item.external_id, "read"))
            hint_source = try_find_error(read_credentials)
            if hint_source:
                hints_by_id[item.external_id].append(hint_source)
            write_credentials = self._authentication_by_id_operation.get((item.external_id, "write"))
            if hint_dest := try_find_error(write_credentials):
                if hint_dest != hint_source:
                    hints_by_id[item.external_id].append(hint_dest)
        if hints_by_id:
            body = "\n".join(f"  {id_} - {humanize_collection(hints)}" for id_, hints in hints_by_id.items())
            return ResourceCreationError(
                f"Failed to create Transformation(s) du to likely invalid credentials:\n{body}",
            )
        return None

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.transformations.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _execute_in_batches(
        self,
        items: Sequence[TransformationRequest],
        api_call: Callable[[Sequence[TransformationRequest]], list[TransformationResponse]],
    ) -> list[TransformationResponse]:
        results: list[TransformationResponse] = []
        for chunk in chunker(items, self._BATCH_SIZE):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self._update_nonce(chunk)
            try:
                chunk_results = api_call(chunk)
            except ToolkitAPIError as e:
                if e.code in (401, 403):
                    if error := self._create_auth_creation_error(chunk):
                        raise error from e
                    raise
                elif "Failed to bind session using nonce" in e.message and len(chunk) > 1:
                    results.extend(self._execute_one_by_one(chunk, api_call))
                else:
                    raise
            else:
                results.extend(chunk_results)
        return results

    def _execute_one_by_one(
        self,
        chunk: Sequence[TransformationRequest],
        api_call: Callable[[Sequence[TransformationRequest]], list[TransformationResponse]],
    ) -> list[TransformationResponse]:
        MediumSeverityWarning(
            f"Failed to create {len(chunk)} transformations in a batch due to nonce binding error. "
            "Trying to recover by creating them one by one."
        ).print_warning(console=self.client.console)
        # Retry one by one
        failed_ids: list[str] = []
        success_count = 0
        delay = 0.3
        self._sleep_with_jitter(delay, delay + 0.3)
        results: list[TransformationResponse] = []
        for item in chunk:
            try:
                recovered = api_call([item])
            except ToolkitAPIError as e:
                if "Failed to bind session using nonce" in e.message:
                    failed_ids.append(item.external_id or "<missing>")
                    self._sleep_with_jitter(delay, delay + 0.3)
                else:
                    raise
            else:
                results.extend(recovered)
                success_count += 1
        message = f"  [bold]RECOVERY COMPLETE:[/] Successfully created {success_count:,} transformations"
        if failed_ids:
            message += f", failed to create {len(failed_ids):,} transformations: {humanize_collection(failed_ids)}"
        else:
            message += "."
        if failed_ids:
            HighSeverityWarning(message).print_warning(include_timestamp=True, console=self.client.console)
        else:
            self.client.console.print(message)
        return results

    @staticmethod
    def _sleep_with_jitter(base_delay: float, max_delay: float) -> None:
        """Sleeps for a random duration between base_delay and max_delay (inclusive)."""
        sleep_time = random.uniform(base_delay, max_delay)
        time.sleep(sleep_time)

    def _update_nonce(self, items: Sequence[TransformationRequest]) -> None:
        for item in items:
            if not item.external_id:
                raise ToolkitRequiredValueError("Transformation must have external_id set.")
            if item.source_nonce is None and (
                read_credentials := self._authentication_by_id_operation.get((item.external_id, "read"))
            ):
                item.source_nonce = self._create_nonce(read_credentials)
            if item.destination_nonce is None and (
                write_credentials := self._authentication_by_id_operation.get((item.external_id, "write"))
            ):
                item.destination_nonce = self._create_nonce(write_credentials)

    def _create_nonce(self, credentials: OidcCredentials | ClientCredentials) -> NonceCredentials:
        if isinstance(credentials, ClientCredentials):
            session = self.client.iam.sessions.create(credentials)
            nonce = NonceCredentials(
                session_id=session.id,
                nonce=session.nonce,
                cdf_project_name=self.client.config.project,
                client_id=credentials.client_id,
            )
        elif isinstance(credentials, OidcCredentials):
            config = deepcopy(self.client.config)
            config.project = credentials.cdf_project_name
            config.credentials = credentials.as_credential_provider()
            other_client = ToolkitClient(config)
            session = other_client.iam.sessions.create(credentials.as_client_credentials())
            nonce = NonceCredentials(
                session_id=session.id,
                nonce=session.nonce,
                cdf_project_name=credentials.cdf_project_name,
                client_id=credentials.client_id,
            )
        else:
            raise ValueError(f"Error in TransformationLoader: {type(credentials)} is not a valid credentials type")
        return nonce

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[TransformationResponse]:
        filter = TransformationFilter()
        if data_set_external_id is not None:
            filter.data_set_ids = [ExternalId(external_id=data_set_external_id)]
        for transformations in self.client.tool.transformations.iterate(limit=None, filter=filter):
            yield from transformations

    def sensitive_strings(self, item: TransformationRequest) -> Iterable[str]:
        external_id = item.external_id
        if read_credentials := self._authentication_by_id_operation.get((external_id, "read")):
            yield read_credentials.client_secret
        if write_credentials := self._authentication_by_id_operation.get((external_id, "write")):
            yield write_credentials.client_secret


@final
class TransformationScheduleIO(
    ResourceIO[
        ExternalId,
        TransformationScheduleRequest,
        TransformationScheduleResponse,
    ]
):
    folder_name = "transformations"
    resource_cls = TransformationScheduleResponse
    resource_write_cls = TransformationScheduleRequest
    kind = "Schedule"
    yaml_cls = TransformationScheduleYAML
    dependencies = frozenset({TransformationIO})
    _doc_url = "Transformation-Schedules/operation/createTransformationSchedules"
    parent_resource = frozenset({TransformationIO})

    @property
    def display_name(self) -> str:
        return "transformation schedules"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[TransformationScheduleRequest]) -> ScopeDefinition | None:
        return None

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        yield from ()

    @classmethod
    def get_id(cls, item: TransformationScheduleResponse | TransformationScheduleRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("TransformationSchedule must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        if "externalId" in item:
            yield TransformationIO, ExternalId(external_id=item["externalId"])

    @classmethod
    def get_dependencies(cls, resource: TransformationScheduleYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield TransformationIO, ExternalId(external_id=resource.external_id)

    def create(self, items: Sequence[TransformationScheduleRequest]) -> list[TransformationScheduleResponse]:
        try:
            return self.client.tool.transformations.schedules.create(list(items))
        except ToolkitAPIError as e:
            if not e.duplicated:
                raise
            existing = {external_id for dup in e.duplicated if (external_id := dup.get("externalId", None))}
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation schedules already exist(s): {existing}"
            )
            new_items = [item for item in items if item.external_id not in existing]
            return self.client.tool.transformations.schedules.create(new_items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[TransformationScheduleResponse]:
        return self.client.tool.transformations.schedules.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[TransformationScheduleRequest]) -> list[TransformationScheduleResponse]:
        return self.client.tool.transformations.schedules.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.transformations.schedules.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[TransformationScheduleResponse]:
        if not parent_ids:
            for schedules in self.client.tool.transformations.schedules.iterate(limit=None):
                yield from schedules
        else:
            transformation_external_ids = [parent_id for parent_id in parent_ids if isinstance(parent_id, ExternalId)]
            if transformation_external_ids:
                yield from self.client.tool.transformations.schedules.retrieve(
                    transformation_external_ids, ignore_unknown_ids=True
                )


@final
class TransformationNotificationIO(
    ResourceIO[
        TransformationNotificationId,
        TransformationNotificationRequest,
        TransformationNotificationResponse,
    ]
):
    folder_name = "transformations"
    resource_cls = TransformationNotificationResponse
    resource_write_cls = TransformationNotificationRequest
    kind = "Notification"
    dependencies = frozenset({TransformationIO})
    _doc_url = "Transformation-Notifications/operation/createTransformationNotifications"
    parent_resource = frozenset({TransformationIO})
    yaml_cls = TransformationNotificationYAML

    support_update = False

    @property
    def display_name(self) -> str:
        return "transformation notifications"

    @classmethod
    def get_id(
        cls, item: TransformationNotificationResponse | TransformationNotificationRequest | dict
    ) -> TransformationNotificationId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"transformationExternalId", "destination"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return TransformationNotificationId(
                transformation_external_id=item["transformationExternalId"], destination=item["destination"]
            )

        return TransformationNotificationId(
            transformation_external_id=item.transformation_external_id or "<missing>",
            destination=item.destination,
        )

    @classmethod
    def dump_id(cls, id: TransformationNotificationId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[TransformationNotificationRequest]) -> ScopeDefinition | None:
        return None

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        yield from ()

    def dump_resource(
        self, resource: TransformationNotificationResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if local and "transformationExternalId" in local:
            dumped.pop("transformationId", None)
            dumped["transformationExternalId"] = local["transformationExternalId"]
        return dumped

    @classmethod
    def as_str(cls, id: TransformationNotificationId) -> str:
        return sanitize_filename(f"{id.transformation_external_id}_{id.destination}")

    def create(self, items: Sequence[TransformationNotificationRequest]) -> list[TransformationNotificationResponse]:
        return self.client.tool.transformations.notifications.create(list(items))

    def retrieve(self, ids: Sequence[TransformationNotificationId]) -> list[TransformationNotificationResponse]:
        unique_ids: set[ExternalId] = {ExternalId(external_id=id_.transformation_external_id) for id_ in ids}
        targets_ids = {(id_.transformation_external_id, id_.destination) for id_ in ids}
        return [
            notification
            for notification in self.iterate(parent_ids=list(unique_ids))
            if (notification.transformation_external_id, notification.destination) in targets_ids
        ]

    def delete(self, ids: Sequence[TransformationNotificationId]) -> int:
        # Note that it is theoretically possible that more items will be deleted than
        # input ids. This is because TransformationNotifications are identified by an internal id,
        # while the toolkit uses the transformationExternalId + destination as the id. Thus, there could
        # be multiple notifications for the same transformationExternalId + destination.
        if existing := self.retrieve(ids):
            self.client.tool.transformations.notifications.delete(InternalId.from_ids([item.id for item in existing]))
        return len(existing)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[TransformationNotificationResponse]:
        if parent_ids is None:
            for notifications in self.client.tool.transformations.notifications.iterate(limit=None):
                yield from notifications
        else:
            parent_external_ids = [parent_id for parent_id in parent_ids if isinstance(parent_id, ExternalId)]
            if parent_external_ids:
                existing_parents = self.client.tool.transformations.retrieve(
                    parent_external_ids, ignore_unknown_ids=True
                )
                for parent_id in existing_parents:
                    filter = TransformationNotificationFilter(transformation_external_id=parent_id.external_id)
                    for notifications in self.client.tool.transformations.notifications.iterate(
                        limit=None, filter=filter
                    ):
                        for notification in notifications:
                            # This is not set by the API.
                            notification.transformation_external_id = parent_id.external_id
                            yield notification

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "transformationExternalId" in item:
            yield TransformationIO, ExternalId(external_id=item["transformationExternalId"])

    @classmethod
    def get_dependencies(
        cls, resource: TransformationNotificationYAML
    ) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        yield TransformationIO, ExternalId(external_id=resource.transformation_external_id)
