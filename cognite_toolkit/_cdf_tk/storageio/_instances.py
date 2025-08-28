from collections.abc import Iterable
from collections.abc import Iterable
from pathlib import Path

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
)
from rich.console import Console

from cognite_toolkit._cdf_tk.loaders import ContainerLoader, ViewLoader
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from ._base import TableStorageIO, StorageIOConfig
from ._selectors import InstanceSelector, InstanceFileSelector, InstanceViewSelector, InstanceContainerSelector


class InstanceIO(TableStorageIO[InstanceSelector, InstanceApplyList, InstanceList]):
    def get_schema(self, selector: InstanceSelector) -> list[SchemaColumn]:
        pass

    def download_iterable(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[
        T_WritableCogniteResourceList]:
        pass

    def count(self, selector: InstanceSelector) -> int | None:
        pass

    def upload_items(self, data_chunk: T_CogniteResourceList, selector: InstanceSelector) -> None:
        pass

    def data_to_json_chunk(self, data_chunk: T_WritableCogniteResourceList) -> list[dict[str, JsonVal]]:
        pass

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> T_CogniteResourceList:
        pass

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        if isinstance(selector, InstanceViewSelector):
            view = self.client.data_modeling.views.retrieve([selector.view])

            yield StorageIOConfig(
                kind=ViewLoader.kind,
                folder_name=selector.folder_name,

            )
        elif isinstance(selector, InstanceContainerSelector):
            container = self.client.data_modeling.containers.retrieve([selector.container])
            yield StorageIOConfig(
                kind=ContainerLoader.kind,
                folder_name=selector.folder_name,
                value=container.dump()
            )

    def load_selector(self, datafile: Path) -> InstanceSelector:
        return InstanceFileSelector(datafile=datafile)

    def ensure_configurations(self, selector: InstanceSelector, console: Console | None = None) -> None:
        raise NotImplementedError()
