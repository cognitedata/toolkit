from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.storageio._annotations import FileAnnotationIO
from cognite_toolkit._cdf_tk.storageio._asset_centric import (
    AssetIO,
    BaseAssetCentricIO,
    EventIO,
    FileMetadataIO,
    TimeSeriesIO,
)
from cognite_toolkit._cdf_tk.storageio._base import (
    ConfigurableStorageIO,
    Page,
    StorageIOConfig,
)
from cognite_toolkit._cdf_tk.storageio.selectors import AssetCentricSelector
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricResource, JsonVal


class HierarchyIO(ConfigurableStorageIO[AssetCentricSelector, AssetCentricResource]):
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._asset_io = AssetIO(client)
        self._file_io = FileMetadataIO(client)
        self._timeseries_io = TimeSeriesIO(client)
        self._event_io = EventIO(client)
        self._annotations_io = FileAnnotationIO(client)
        self._io_by_kind: dict[str, BaseAssetCentricIO] = {
            self._asset_io.KIND: self._asset_io,
            self._file_io.KIND: self._file_io,
            self._timeseries_io.KIND: self._timeseries_io,
            self._event_io.KIND: self._event_io,
            # Todo: Add a protocol or a shared base class for asset/events/timeseries/files + annotations.
            self._annotations_io.KIND: self._annotations_io,  # type: ignore[dict-item]
        }

    def as_id(self, item: AssetCentricResource) -> str:
        return item.external_id or BaseAssetCentricIO.create_internal_identifier(item.id, self.client.config.project)

    def stream_data(
        self, selector: AssetCentricSelector, limit: int | None = None
    ) -> Iterable[Page[AssetCentricResource]]:
        yield from self.get_resource_io(selector.kind).stream_data(selector, limit)

    def count(self, selector: AssetCentricSelector) -> int | None:
        return self.get_resource_io(selector.kind).count(selector)

    def data_to_json_chunk(
        self, data_chunk: Sequence[AssetCentricResource], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        if selector is None:
            raise ValueError(f"Selector must be provided to convert data to JSON chunk for {type(self).__name__}.)")
        return self.get_resource_io(selector.kind).data_to_json_chunk(data_chunk, selector)

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        yield from self.get_resource_io(selector.kind).configurations(selector)

    def get_resource_io(self, kind: str) -> BaseAssetCentricIO:
        return self._io_by_kind[kind]
