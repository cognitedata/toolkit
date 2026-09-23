from collections.abc import Hashable, Iterable, Sequence, Sized
from pathlib import Path
from typing import Any, Literal, NoReturn, final

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function_app import FunctionAppRequest, FunctionAppResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    FilesAcl,
    FunctionsAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ReadExtra, ResourceIO
from cognite_toolkit._cdf_tk.utils import calculate_secure_hash
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import FunctionAppsYAML

from ._auth import GroupAllScopedCRUD
from ._data_organization import DataSetsIO
from ._file import CogniteFileCRUD, FileMetadataCRUD
from ._function_code_bundle import FunctionCodeBundle


@final
class FunctionAppIO(ResourceIO[ExternalId, FunctionAppRequest, FunctionAppResponse, FunctionAppsYAML]):
    """Build support for Function Apps while the deployment API remains pre-GA."""

    support_deploy = False
    support_drop = False
    support_update = False
    deploy_not_supported_message = (
        "Function App deployment is not available yet. This alpha resource supports build only; "
        "wait for Function App deployment support in a later Toolkit release."
    )
    folder_name = "functions"
    kind = "FunctionApp"
    resource_cls = FunctionAppResponse
    resource_write_cls = FunctionAppRequest
    yaml_cls = FunctionAppsYAML
    dependencies = frozenset({DataSetsIO, GroupAllScopedCRUD})
    extra_kinds = frozenset({FileMetadataCRUD.kind, CogniteFileCRUD.kind})

    class _MetadataKey:
        function_hash = "cognite-toolkit-hash"

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Any) -> None:
        super().__init__(client, build_path, console)
        self.data_set_id_by_external_id: dict[str, int] = {}
        self.space_by_external_id: dict[str, str] = {}
        self._code_bundle = FunctionCodeBundle(client)

    @property
    def filemetadata_path_by_external_id(self) -> dict[str, Path]:
        return self._code_bundle.filemetadata_path_by_external_id

    @property
    def cognitefile_path_by_external_id(self) -> dict[str, Path]:
        return self._code_bundle.cognitefile_path_by_external_id

    @property
    def display_name(self) -> str:
        return "function apps"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[FunctionAppRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield FunctionsAcl(actions=sorted(actions), scope=AllScope())
            yield FilesAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: FunctionAppResponse | FunctionAppRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("FunctionApp must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_dependencies(cls, resource: FunctionAppsYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.data_set_external_id)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_list = super().load_resource_file(filepath, environment_variables)
        filestem = filepath.stem
        if filestem.lower().endswith(self.kind.lower()):
            filestem = filestem[: -len(self.kind)].rstrip(".")
        for item in raw_list:
            if item.get("metadata") is None:
                item["metadata"] = {}
            if "secrets" in item:
                item["metadata"]["cdf-toolkit-secret-hash"] = calculate_secure_hash(item["secrets"])
            self._code_bundle.map_sidecar_paths(filepath, filestem, [self.get_id(item).external_id])
        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> FunctionAppRequest:
        resource = resource.copy()
        external_id = self.get_id(resource).external_id
        if data_set_external_id := resource.pop("dataSetExternalId", None):
            data_set_id = self.client.lookup.data_sets.id(data_set_external_id, is_dry_run)
            self.data_set_id_by_external_id[external_id] = data_set_id
            resource["dataSetId"] = data_set_id
        if space := resource.pop("space", None):
            self.space_by_external_id[external_id] = space
        resource.pop("package", None)
        resource.setdefault("fileId", -1)
        return FunctionAppRequest.model_validate(resource)

    @classmethod
    def get_function_code_implicitly(cls, filepath: Path, identifier: ExternalId) -> Path:
        return FunctionCodeBundle.get_code_implicitly(filepath, identifier.external_id)

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: ExternalId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        yield from FunctionCodeBundle.get_extra_files(
            filepath,
            identifier.external_id,
            item,
            cls._MetadataKey.function_hash,
            package=item.get("package"),
            export_uv_requirements=True,
            hash_build_file=True,
            remove_fields=["package"],
        )

    def sensitive_strings(self, item: FunctionAppRequest) -> Iterable[str]:
        if item.secrets:
            yield from item.secrets.values()

    def _not_available(self) -> NoReturn:
        raise ToolkitNotSupported(self.deploy_not_supported_message)

    def create(self, items: Sequence[FunctionAppRequest]) -> Sized:
        return self._not_available()

    def retrieve(self, ids: Sequence[ExternalId]) -> Sequence[FunctionAppResponse]:
        return self._not_available()

    def delete(self, ids: Sequence[ExternalId]) -> int:
        return self._not_available()

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[FunctionAppResponse]:
        return self._not_available()
