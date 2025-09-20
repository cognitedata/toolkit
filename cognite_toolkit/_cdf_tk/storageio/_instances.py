from collections.abc import Iterable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import ClassVar

from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import Edge, EdgeApply, Node, NodeApply
from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList, InstanceList
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIOConfig, TableStorageIO
from ._selectors import InstanceFileSelector, InstanceSelector, InstanceViewSelector


class InstanceIO(TableStorageIO[InstanceId, InstanceSelector, InstanceApplyList, InstanceList]):
    FOLDER_NAME = "instances"
    KIND = "Instances"
    DISPLAY_NAME = "Instances"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = "/models/instances"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = MappingProxyType({"autoCreateDirectRelations": True})

    def as_id(self, item: dict[str, JsonVal] | object) -> InstanceId:
        if isinstance(item, dict) and isinstance(item.get("space"), str) and isinstance(item.get("externalId"), str):
            # MyPy checked above.
            return InstanceId(space=item["space"], external_id=item["externalId"])  # type: ignore[arg-type]
        if isinstance(item, InstanceId):
            return item
        elif isinstance(item, Node | Edge | NodeApply | EdgeApply):
            return item.as_id()
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def stream_data(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[InstanceList]:
        if isinstance(selector, InstanceViewSelector):
            chunk = InstanceList([])
            total = 0
            for instance in iterate_instances(
                client=self.client,
                source=selector.view,
                instance_type=selector.instance_type,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            ):
                if limit is not None and total >= limit:
                    break
                total += 1
                chunk.append(instance)
                if len(chunk) >= self.CHUNK_SIZE:
                    yield chunk
                    chunk = InstanceList([])
            if chunk:
                yield chunk
        elif isinstance(selector, InstanceFileSelector):
            for node_chunk in chunker_sequence(selector.node_ids, self.CHUNK_SIZE):
                yield InstanceList(self.client.data_modeling.instances.retrieve(nodes=node_chunk).nodes)
            for edge_chunk in chunker_sequence(selector.edge_ids, self.CHUNK_SIZE):
                yield InstanceList(self.client.data_modeling.instances.retrieve(edges=edge_chunk).edges)
        else:
            raise NotImplementedError()

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[list[InstanceId]]:
        if isinstance(selector, InstanceFileSelector) and selector.validate is False:
            instances_to_yield = selector.instance_ids
            if limit is not None:
                instances_to_yield = instances_to_yield[:limit]
            yield from chunker_sequence(instances_to_yield, self.CHUNK_SIZE)
        else:
            yield from ([instance.as_id() for instance in chunk] for chunk in self.stream_data(selector, limit))  # type: ignore[attr-defined]

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector):
            result = self.client.data_modeling.instances.aggregate(
                view=selector.view,
                aggregates=Count("externalId"),
                instance_type=selector.instance_type,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            )
            return int(result.value or 0)
        elif isinstance(selector, InstanceFileSelector):
            return len(selector.instance_ids)
        raise NotImplementedError()

    def upload_items(self, data_chunk: InstanceApplyList, selector: InstanceSelector) -> None:
        raise NotImplementedError()

    def data_to_json_chunk(self, data_chunk: InstanceList) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        raise NotImplementedError()

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def load_selector(self, datafile: Path) -> InstanceSelector:
        raise NotImplementedError()

    def ensure_configurations(self, selector: InstanceSelector, console: Console | None = None) -> None:
        raise NotImplementedError()

    def get_schema(self, selector: InstanceSelector) -> list[SchemaColumn]:
        raise NotImplementedError()
