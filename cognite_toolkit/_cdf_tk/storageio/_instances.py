from collections.abc import Iterable
from pathlib import Path

from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList, InstanceList
from cognite_toolkit._cdf_tk.loaders import ViewLoader
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ..utils.cdf import iterate_instances
from ._base import StorageIOConfig, TableStorageIO
from ._selectors import InstanceFileSelector, InstanceSelector, InstanceViewSelector


class InstanceIO(TableStorageIO[InstanceSelector, InstanceApplyList, InstanceList]):
    chunk_size = 1000

    def download_iterable(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[InstanceList]:
        if isinstance(selector, InstanceViewSelector):
            chunk = InstanceList([])
            total = 0
            for instance in iterate_instances(
                client=self.client,
                source=selector.view,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            ):
                if limit is not None and total >= limit:
                    break
                total += 1
                chunk.append(instance)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[list[InstanceId]]:
        if isinstance(selector, InstanceViewSelector):
            yield from ([instance.as_id() for instance in chunk] for chunk in self.download_iterable(selector, limit))  # type: ignore[arg-type]

    def count(self, selector: InstanceSelector) -> int | None:
        pass

    def upload_items(self, data_chunk: InstanceApplyList, selector: InstanceSelector) -> None:
        pass

    def data_to_json_chunk(self, data_chunk: InstanceList) -> list[dict[str, JsonVal]]:
        pass

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        pass

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        if isinstance(selector, InstanceViewSelector):
            view_loader = ViewLoader.create_loader(self.client)
            views = view_loader.retrieve([selector.view])
            for view in views:
                yield StorageIOConfig(
                    kind=view_loader.kind, folder_name=view_loader.folder_name, value=view_loader.dump_resource(view)
                )

    def load_selector(self, datafile: Path) -> InstanceSelector:
        return InstanceFileSelector(datafile=datafile)

    def ensure_configurations(self, selector: InstanceSelector, console: Console | None = None) -> None:
        raise NotImplementedError()

    def get_schema(self, selector: InstanceSelector) -> list[SchemaColumn]:
        raise NotImplementedError()
