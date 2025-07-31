from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from cognite.client.data_classes import AssetList, AssetWriteList, RowList, RowWrite, RowWriteList
from cognite.client.data_classes._base import (
    CogniteResourceList,
    T_CogniteResourceList,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import InstanceApply, ViewId
from cognite.client.data_classes.data_modeling.instances import Instance

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumn, SchemaColumnList
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

T_StorageID = TypeVar("T_StorageID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)


@dataclass(frozen=True)
class AssetCentricData:
    data_set_id: tuple[int, ...] | None = None
    hierarchy: tuple[int, ...] | None = None

    def as_filter(self) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=list(self.data_set_id) if self.data_set_id else None,
            asset_subtree_external_ids=list(self.hierarchy) if self.hierarchy else None,
        )


@dataclass(frozen=True)
class InstanceData:
    view_id: ViewId
    instance_spaces: tuple[str, ...] | None = None


class InstanceApplyList(CogniteResourceList[InstanceApply]):
    _RESOURCE = InstanceApply


class InstanceList(WriteableCogniteResourceList[InstanceApply, Instance]):
    _RESOURCE = Instance

    def as_write(self) -> InstanceApplyList:
        return InstanceApplyList([item.as_write() for item in self])


class StorageIO(ABC, Generic[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList]):
    """This is a base class for all storage classes in Cognite Toolkit"""

    folder_name: str
    kind: str
    display_name: str
    supported_download_formats: frozenset[str]
    supported_compressions: frozenset[str]
    supported_read_formats: frozenset[str]

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @abstractmethod
    def download_iterable(
        self, identifier: T_StorageID, limit: int | None = None
    ) -> Iterable[T_WritableCogniteResourceList]:
        raise NotImplementedError()

    @abstractmethod
    def upload_items(self, data_chunk: T_CogniteResourceList, identifier: T_StorageID) -> None:
        raise NotImplementedError()

    @abstractmethod
    def data_to_json_chunk(self, data_chunk: T_WritableCogniteResourceList) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    @abstractmethod
    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_CogniteResourceList:
        raise NotImplementedError()


class TableStorageIO(StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList], ABC):
    @abstractmethod
    def get_schema(self, identifier: T_StorageID) -> list[SchemaColumn]: ...


class AssetIO(TableStorageIO[AssetCentricData, AssetWriteList, AssetList]):
    folder_name = "classic"
    kind = "Assets"
    display_name = "Assets"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson"})
    chunk_size = 1000

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._loader = AssetLoader.create_loader(client)

    def get_schema(self, identifier: AssetCentricData) -> list[SchemaColumn]:
        return [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="parentExternalId", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="labels", type="string", is_array=True),
            SchemaColumn(name="geoLocation", type="json"),
        ]

    def download_iterable(self, identifier: AssetCentricData, limit: int | None = None) -> Iterable[AssetList]:
        yield from self.client.assets(chunk_size=self.chunk_size, limit=limit, **identifier.as_filter())

    def upload_items(self, data_chunk: AssetWriteList, identifier: AssetCentricData) -> None:
        if not data_chunk:
            return
        # Todo Use HTTP Batch Processor to handle existing assets
        self.client.assets.create(data_chunk)

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> AssetWriteList:
        output = AssetWriteList([])
        for item in data_chunk:
            output.append(self._loader.load_resource(item))
        return output

    def data_to_json_chunk(self, data_chunk: AssetList) -> list[dict[str, JsonVal]]:
        output: list[dict[str, JsonVal]] = []
        for item in data_chunk:
            output.append(self._loader.dump_resource(item))
        return output


class RawIO(StorageIO[RawTable, RowWriteList, RowList]):
    folder_name = "raw"
    kind = "RawRows"
    display_name = "Raw Rows"
    supported_download_formats = frozenset({".yaml", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    chunk_size = 10_000

    def download_iterable(self, identifier: RawTable, limit: int | None = None) -> Iterable[RowList]:
        yield from self.client.raw.rows(
            db_name=identifier.db_name,
            table_name=identifier.table_name,
            limit=limit,
            partitions=8,
            chunk_size=self.chunk_size,
        )

    def upload_items(self, data_chunk: RowWriteList, identifier: RawTable) -> None:
        self.client.raw.rows.insert(db_name=identifier.db_name, table_name=identifier.table_name, row=data_chunk)

    def data_to_json_chunk(self, data_chunk: RowList) -> list[dict[str, JsonVal]]:
        return [row.as_write().dump() for row in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> RowWriteList:
        return RowWriteList([RowWrite._load(row) for row in data_chunk])


class InstanceIO(TableStorageIO[InstanceData, InstanceApplyList, InstanceList]):
    folder_name = "instances"
    kind = "Instances"
    display_name = "Instances"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    chunk_size = 1_000

    def get_schema(self, identifier: InstanceData) -> list[SchemaColumn]:
        view = self.client.data_modeling.views.retrieve(identifier.view_id)
        if not view:
            raise ValueError(f"View {identifier.view_id} not found.")
        return SchemaColumnList.create_from_view_properties(view[0].properties)

    def download_iterable(self, identifier: InstanceData, limit: int | None = None) -> Iterable[InstanceList]:
        batch = InstanceList([])
        for instance in iterate_instances(
            self.client, instance_type="node", space=identifier.instance_spaces, source=identifier.view_id
        ):
            batch.append(instance)
            if len(batch) >= self.chunk_size:
                yield batch
                batch = InstanceList([])
        if batch:
            yield batch

    def upload_items(self, data_chunk: InstanceApplyList, identifier: InstanceData) -> None:
        self.client.data_modeling.instances.apply_fast(data_chunk)

    def data_to_json_chunk(self, data_chunk: InstanceList) -> list[dict[str, JsonVal]]:
        return [instance.as_write().dump() for instance in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        return InstanceApplyList([InstanceApply._load(instance) for instance in data_chunk])
