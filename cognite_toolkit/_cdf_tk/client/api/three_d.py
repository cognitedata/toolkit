from collections.abc import Sequence

from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.cdf_client import CDFClient
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient


class ThreeModelDAPI:
    ENDPOINT = "/3d/models"

    def __init__(self, http_client: HTTPClient, cdf_client: CDFClient, console: Console) -> None:
        self._http_client = http_client
        self._cdf_client = cdf_client
        self._console = console
        self._config = http_client.config

    def create(self, models: Sequence[ThreeDModelRequest]) -> Sequence[ThreeDModelResponse]:
        raise NotImplementedError()

    def retrieve(self, id: int) -> ThreeDModelResponse:
        raise NotImplementedError()

    def update(self, models: Sequence[ThreeDModelRequest]) -> Sequence[ThreeDModelResponse]:
        raise NotImplementedError()

    def delete(self, ids: Sequence[int]) -> None:
        raise NotImplementedError()

    def list(
        self, published: bool, include_revision_info: bool = False, limit: int = 100
    ) -> Sequence[ThreeDModelResponse]:
        raise NotImplementedError()


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient, cdf_client: CDFClient, console: Console) -> None:
        self.models = ThreeModelDAPI(http_client, cdf_client, console)
