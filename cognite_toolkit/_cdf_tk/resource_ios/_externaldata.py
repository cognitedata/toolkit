from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from typing_extensions import TypeForm

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequestAdapter,
    ExternalDataSourceRequestUnion,
    ExternalDataSourceResponseUnion,
    OneLakeExternalDataSourceRequest,
    SnowflakeExternalDataSourceResponse,
    UnknownExternalDataSourceRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    ScopeDefinition,
    TransformationsExternalDataSourcesAcl,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._data_organization import DataSetsIO
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.yaml_classes.externaldata import ExternalDataSource, ExternalDataSourceYAML


@final
class ExternalDataSourceIO(
    ResourceIO[
        ExternalId,
        ExternalDataSourceRequestUnion,
        ExternalDataSourceResponseUnion,
        ExternalDataSourceYAML,
    ],
):
    folder_name = "transformations"
    resource_cls: TypeForm[ExternalDataSourceResponseUnion] = ExternalDataSourceResponseUnion
    resource_write_cls: TypeForm[ExternalDataSourceRequestUnion] = ExternalDataSourceRequestUnion
    kind = "ExternalDataSource"
    yaml_cls = ExternalDataSourceYAML
    dependencies = frozenset({DataSetsIO})
    _doc_url = "Transformation-External-Data-Sources/operation/createExternalDataSources"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    support_update = False

    @property
    def display_name(self) -> str:
        return "transformation external data sources"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[ExternalDataSourceRequestUnion]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            acl_actions: set[Literal["READ", "WRITE", "USE"]] = set()
            if "READ" in actions:
                acl_actions.update({"READ", "USE"})
            if "WRITE" in actions:
                acl_actions.add("WRITE")
            yield TransformationsExternalDataSourcesAcl(actions=sorted(acl_actions), scope=scope)

    @classmethod
    def get_id(
        cls, item: ExternalDataSourceRequestUnion | ExternalDataSourceResponseUnion | ExternalId | dict
    ) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
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
    def as_filename(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_dependencies(cls, resource: ExternalDataSource) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.data_set_external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> ExternalDataSourceRequestUnion:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        loaded = ExternalDataSourceRequestAdapter.validate_python(resource)
        if isinstance(loaded, UnknownExternalDataSourceRequest):
            # Without a known format we cannot tell which fields are sensitive, so we refuse to deploy it.
            raise ToolkitNotSupported(
                f"External data source format {loaded.format!r} is not supported. "
                "Please contact support to request support for this format."
            )
        return loaded

    def dump_resource(
        self, resource: ExternalDataSourceResponseUnion, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # OneLake secrets cannot be compared because the API never returns clientSecret. Dumping only
        # the identifier makes deploy recreate OneLake sources. Snowflake has no write-only input, so
        # its comparable fields are dumped to avoid generating a new key pair on every deploy.
        if local and resource.format == "one_lake":
            HighSeverityWarning(
                "OneLake external data sources will always be considered different, and thus will always be "
                "deleted and re-created on deploy."
            ).print_warning(console=self.client.console)
            return self.dump_id(self.get_id(resource))
        dumped = resource.dump()
        dumped.pop("createdTime", None)
        dumped.pop("lastUpdatedTime", None)
        if resource.format == "one_lake":
            dumped.pop("format", None)
        elif isinstance(resource, SnowflakeExternalDataSourceResponse):
            dumped["settings"]["credentials"].pop("publicKey", None)
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped

    def sensitive_strings(self, item: ExternalDataSourceRequestUnion) -> Iterable[str]:
        if isinstance(item, OneLakeExternalDataSourceRequest) and item.settings and item.settings.credentials:
            yield item.settings.credentials.client_secret

    def create(self, items: Sequence[ExternalDataSourceRequestUnion]) -> list[ExternalDataSourceResponseUnion]:
        return self.client.transformations.external_data_sources.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[ExternalDataSourceResponseUnion]:
        if not ids:
            return []
        return self.client.transformations.external_data_sources.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.transformations.external_data_sources.delete(id_list)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ExternalDataSourceResponseUnion]:
        if space or parent_ids:
            return
        if data_set_external_id is None:
            yield from self.client.transformations.external_data_sources.list(limit=None)
            return
        data_set_id = self.client.lookup.data_sets.id(data_set_external_id)
        if data_set_id is None:
            return
        for source in self.client.transformations.external_data_sources.list(limit=None):
            if source.data_set_id == data_set_id:
                yield source
