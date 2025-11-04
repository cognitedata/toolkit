from collections.abc import Iterable, Sequence

from cognite.client.data_classes import Annotation

from cognite_toolkit._cdf_tk.storageio import Page
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIO
from .selectors import AssetCentricSelector


class AnnotationIO(StorageIO[AssetCentricSelector, Annotation]):
    def as_id(self, item: Annotation) -> str:
        raise NotImplementedError()

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError()

    def count(self, selector: AssetCentricSelector) -> int | None:
        """There is no efficient way to count annotations in CDF."""
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[Annotation], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()
