from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.external_data_source import (
    ExternalDataSourceRequest,
    ExternalDataSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    ScopeDefinition,
    TransformationsExternalDataSourcesAcl,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.data_organization import DataSetsIO
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.yaml_classes import ExternalDataSourceYAML


@final
class ExternalDataSourceIO(
    ResourceIO[ExternalId, ExternalDataSourceRequest, ExternalDataSourceResponse],
):
    folder_name = "transformations"
    resource_cls = ExternalDataSourceResponse
    resource_write_cls = ExternalDataSourceRequest
    kind = "ExternalDataSource"
    yaml_cls = ExternalDataSourceYAML
    dependencies = frozenset({DataSetsIO})
    _doc_url = "Transformations-External-Data-Sources/operation/upsertExternalDataSources"

    @property
    def display_name(self) -> str:
        return "transformation external data sources"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[ExternalDataSourceRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield TransformationsExternalDataSourcesAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: ExternalDataSourceRequest | ExternalDataSourceResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            external_id = item.get("externalId") or item.get("external_id")
            if external_id is None:
                raise ToolkitRequiredValueError("ExternalDataSource must have externalId set.")
            return ExternalId(external_id=external_id)
        if not item.external_id:
            raise ToolkitRequiredValueError("ExternalDataSource must have external_id set.")
        return ExternalId(external_id=item.external_id)

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

    @classmethod
    def get_dependencies(cls, resource: ExternalDataSourceYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.data_set_external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> ExternalDataSourceRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return ExternalDataSourceRequest.model_validate(resource, by_alias=True)

    def dump_resource(
        self, resource: ExternalDataSourceResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        dumped.pop("format", None)
        local_settings = local.get("settings", {})
        local_credentials = local_settings.get("credentials", {}) if isinstance(local_settings, dict) else {}
        if local_credentials and "settings" in dumped and dumped["settings"]:
            cdf_credentials = dumped["settings"].get("credentials", {})
            if isinstance(cdf_credentials, dict) and "clientSecret" in local_credentials:
                cdf_credentials["clientSecret"] = local_credentials["clientSecret"]
        return dumped

    def sensitive_strings(self, item: ExternalDataSourceRequest) -> Iterable[str]:
        if item.settings and item.settings.credentials and item.settings.credentials.client_secret:
            yield item.settings.credentials.client_secret

    def create(self, items: Sequence[ExternalDataSourceRequest]) -> list[ExternalDataSourceResponse]:
        return self.client.tool.transformations.external_data_sources.upsert(list(items))

    def update(self, items: Sequence[ExternalDataSourceRequest]) -> list[ExternalDataSourceResponse]:
        return self.client.tool.transformations.external_data_sources.upsert(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[ExternalDataSourceResponse]:
        if not ids:
            return []
        id_set = {id_.external_id for id_ in ids}
        return [
            source
            for source in self.client.tool.transformations.external_data_sources.list(limit=None)
            if source.external_id in id_set
        ]

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.transformations.external_data_sources.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ExternalDataSourceResponse]:
        if space or parent_ids:
            return
        if data_set_external_id is None:
            yield from self.client.tool.transformations.external_data_sources.list(limit=None)
            return
        data_sets = self.client.tool.datasets.retrieve(
            [ExternalId(external_id=data_set_external_id)], ignore_unknown_ids=True
        )
        if not data_sets:
            raise ToolkitRequiredValueError(f"DataSet {data_set_external_id!r} does not exist")
        data_set = data_sets[0]
        for source in self.client.tool.transformations.external_data_sources.list(limit=None):
            if source.data_set_id == data_set.id:
                yield source
