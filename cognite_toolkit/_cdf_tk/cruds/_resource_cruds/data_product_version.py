from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.data_product_version import (
    DataProductVersionRequest,
    DataProductVersionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import DataProductVersionId, ExternalId
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import DataProductVersionYAML

from .data_product import DataProductCRUD


@final
class DataProductVersionCRUD(ResourceCRUD[DataProductVersionId, DataProductVersionRequest, DataProductVersionResponse]):
    folder_name = "data_products"
    resource_cls = DataProductVersionResponse
    resource_write_cls = DataProductVersionRequest
    kind = "DataProductVersion"
    yaml_cls = DataProductVersionYAML
    dependencies = frozenset({DataProductCRUD})
    parent_resource = frozenset({DataProductCRUD})
    support_drop = True
    support_update = True
    _doc_url = "Data-Products/operation/createDataProductVersions"

    @property
    def display_name(self) -> str:
        return "data product versions"

    @classmethod
    def get_id(cls, item: DataProductVersionRequest | DataProductVersionResponse | dict) -> DataProductVersionId:
        if isinstance(item, dict):
            return DataProductVersionId(
                data_product_external_id=item["dataProductExternalId"],
                version=item["version"],
            )
        return item.as_id()

    @classmethod
    def dump_id(cls, id: DataProductVersionId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataProductVersionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # TODO: dataproductsAcl is not yet in the SDK — return empty to skip capability verification.
        # Once available, require: READ + UPDATE (all version mutations use dataproductsAcl:UPDATE).
        return []

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataProductExternalId" in item:
            yield DataProductCRUD, ExternalId(external_id=item["dataProductExternalId"])

    def dump_resource(
        self, resource: DataProductVersionResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        dumped["dataProductExternalId"] = resource.data_product_external_id
        local = local or {}
        defaults: list[tuple[str, Any]] = [
            ("status", "draft"),
            ("description", None),
            ("terms", None),
        ]
        for key, default in defaults:
            if dumped.get(key) == default and key not in local:
                dumped.pop(key)
        return dumped

    def create(self, items: Sequence[DataProductVersionRequest]) -> list[DataProductVersionResponse]:
        if not items:
            return []
        return self.client.tool.data_products.versions.create(list(items))

    def retrieve(self, ids: SequenceNotStr[DataProductVersionId]) -> list[DataProductVersionResponse]:
        if not ids:
            return []
        return self.client.tool.data_products.versions.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DataProductVersionRequest]) -> list[DataProductVersionResponse]:
        if not items:
            return []
        return self.client.tool.data_products.versions.update(list(items))

    def delete(self, ids: SequenceNotStr[DataProductVersionId]) -> int:
        if not ids:
            return 0
        self.client.tool.data_products.versions.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataProductVersionResponse]:
        if parent_ids is not None:
            dp_ext_ids = {
                pid.external_id if isinstance(pid, ExternalId) else pid
                for pid in parent_ids
                if isinstance(pid, (str, ExternalId))
            }
        else:
            dp_ext_ids = {dp.external_id for dp in self.client.tool.data_products.list(limit=None)}

        for dp_ext_id in dp_ext_ids:
            for versions in self.client.tool.data_products.versions.iterate(dp_ext_id, limit=None):
                yield from versions
