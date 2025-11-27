from collections.abc import Sequence

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient


class ThreeDAPI:
    ENDPOINT = "/3d/models"

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def create(self, models: Sequence[ThreeDModelRequest]) -> Sequence[ThreeDModelResponse]: ...

    def retrieve(self, id: int) -> ThreeDModelResponse: ...

    def update(self, models: Sequence[ThreeDModelRequest]) -> Sequence[ThreeDModelResponse]: ...

    def delete(self, ids: Sequence[int]) -> None: ...

    def list(
        self, published: bool, include_revision_info: bool = False, limit: int = 100
    ) -> Sequence[ThreeDModelResponse]: ...
