from collections.abc import Iterable
from pathlib import Path

from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import Edge, EdgeApply, Node, NodeApply
from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList, InstanceList
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIOConfig, TableStorageIO
from ._selectors import InstanceSelector, InstanceViewSelector


class InstanceIO(TableStorageIO[InstanceId, InstanceSelector, InstanceApplyList, InstanceList]):
    folder_name = "instances"
    kind = "Instances"
    display_name = "Instances"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    chunk_size = 1000

    def as_id(self, item: dict[str, JsonVal] | object) -> InstanceId:
        if isinstance(item, dict) and isinstance(item.get("space"), str) and isinstance(item.get("externalId"), str):
            # MyPy checked above.
            return InstanceId(space=item["space"], external_id=item["externalId"])  # type: ignore[arg-type]
        if isinstance(item, InstanceId):
            return item
        elif isinstance(item, Node | Edge | NodeApply | EdgeApply):
            return item.as_id()
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def download_iterable(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[InstanceList]:
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
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = InstanceList([])
            if chunk:
                yield chunk
        else:
            raise NotImplementedError()

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[list[InstanceId]]:
        if isinstance(selector, InstanceViewSelector):
            yield from ([instance.as_id() for instance in chunk] for chunk in self.download_iterable(selector, limit))  # type: ignore[attr-defined]
        else:
            raise NotImplementedError()

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector):
            result = self.client.data_modeling.instances.aggregate(
                view=selector.view,
                aggregates=Count("externalId"),
                instance_type=selector.instance_type,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            )
            return int(result.value or 0)
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
