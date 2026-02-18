from collections import defaultdict
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
            if "dataModel" in item:
                return DataProductVersionId(
                    data_product_external_id=item["dataProductExternalId"],
                    data_model_external_id=item["dataModel"]["externalId"],
                    data_model_version=item["dataModel"]["version"],
                )
            return DataProductVersionId(
                data_product_external_id=item["dataProductExternalId"],
                data_model_external_id=item["dataModelExternalId"],
                data_model_version=item["dataModelVersion"],
            )
        return item.as_id()

    @classmethod
    def dump_id(cls, id: DataProductVersionId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataProductVersionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # dataproductsAcl is not yet in the SDK
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
        by_parent = self._group_by_parent(items)
        results: list[DataProductVersionResponse] = []
        for dp_ext_id, group in by_parent.items():
            results.extend(self.client.tool.data_products.versions.create(dp_ext_id, group))
        return results

    def retrieve(self, ids: SequenceNotStr[DataProductVersionId]) -> list[DataProductVersionResponse]:
        if not ids:
            return []
        dp_ext_ids = {id_.data_product_external_id for id_ in ids}
        all_versions: list[DataProductVersionResponse] = []
        for dp_ext_id in dp_ext_ids:
            all_versions.extend(self.client.tool.data_products.versions.list(dp_ext_id, limit=None))

        id_set = set(ids)
        return [v for v in all_versions if v.as_id() in id_set]

    def update(self, items: Sequence[DataProductVersionRequest]) -> list[DataProductVersionResponse]:
        if not items:
            return []
        # We need the CDF versionId to update. Retrieve all existing versions for the affected data products.
        dp_ext_ids = {item.data_product_external_id for item in items}
        cdf_versions: list[DataProductVersionResponse] = []
        for dp_ext_id in dp_ext_ids:
            cdf_versions.extend(self.client.tool.data_products.versions.list(dp_ext_id, limit=None))

        cdf_by_id = {v.as_id(): v for v in cdf_versions}
        results: list[DataProductVersionResponse] = []
        for item in items:
            item_id = item.as_id()
            cdf_version = cdf_by_id.get(item_id)
            if cdf_version is None:
                results.extend(self.client.tool.data_products.versions.create(item.data_product_external_id, [item]))
            else:
                results.append(
                    self.client.tool.data_products.versions.update(
                        item.data_product_external_id, cdf_version.version_id, item
                    )
                )
        return results

    def delete(self, ids: SequenceNotStr[DataProductVersionId]) -> int:
        if not ids:
            return 0
        # We need the CDF versionId to delete. Retrieve existing versions.
        dp_ext_ids = {id_.data_product_external_id for id_ in ids}
        cdf_versions: list[DataProductVersionResponse] = []
        for dp_ext_id in dp_ext_ids:
            cdf_versions.extend(self.client.tool.data_products.versions.list(dp_ext_id, limit=None))

        cdf_by_id = {v.as_id(): v for v in cdf_versions}
        to_delete: defaultdict[str, list[int]] = defaultdict(list)
        for id_ in ids:
            cdf_version = cdf_by_id.get(id_)
            if cdf_version is not None:
                to_delete[id_.data_product_external_id].append(cdf_version.version_id)

        count = 0
        for dp_ext_id, version_ids in to_delete.items():
            self.client.tool.data_products.versions.delete(dp_ext_id, version_ids)
            count += len(version_ids)
        return count

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

    @staticmethod
    def _group_by_parent(items: Sequence[DataProductVersionRequest]) -> dict[str, list[DataProductVersionRequest]]:
        by_parent: defaultdict[str, list[DataProductVersionRequest]] = defaultdict(list)
        for item in items:
            by_parent[item.data_product_external_id].append(item)
        return dict(by_parent)
