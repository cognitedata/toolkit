from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import AllScope, Capability, UnknownAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.data_product import DataProductRequest, DataProductResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.auth import GroupAllScopedCRUD
from cognite_toolkit._cdf_tk.resource_classes import DataProductYAML


@final
class DataProductCRUD(ResourceCRUD[str, DataProductRequest, DataProductResponse]):  # pyright: ignore[reportInvalidTypeArguments]
    folder_name = "data_products"
    resource_cls = DataProductResponse
    resource_write_cls = DataProductRequest
    kind = "DataProduct"
    yaml_cls = DataProductYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    support_drop = True
    support_update = True
    _doc_url = "Data-Products/operation/createDataProduct"

    @property
    def display_name(self) -> str:
        return "data products"

    @classmethod
    def get_id(cls, item: DataProductRequest | DataProductResponse | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataProductRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        return UnknownAcl(
            actions=[],
            scope=AllScope(),
            capability_name="dataProductsAcl",
            allow_unknown=True,
        )

    def create(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.tool.data_products.create(list(items))

    def retrieve(self, ids: SequenceNotStr[str]) -> list[DataProductResponse]:
        return self.client.tool.data_products.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DataProductRequest]) -> list[DataProductResponse]:
        return self.client.tool.data_products.update(list(items))

    def delete(self, ids: SequenceNotStr[str]) -> int:
        if not ids:
            return 0
        self.client.tool.data_products.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataProductResponse]:
        for items in self.client.tool.data_products.iterate():
            yield from items
