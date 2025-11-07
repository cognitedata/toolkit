from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.data_classes.infield import InfieldLocationConfig
from cognite_toolkit._cdf_tk.client.data_classes.instances import NodeIdentifier, NodeResult
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient


class InfieldConfigAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def apply(self, items: Sequence[InfieldLocationConfig]) -> list[NodeResult]:
        raise NotImplementedError()

    def retrieve(self, items: Sequence[NodeIdentifier]) -> list[InfieldLocationConfig]:
        raise NotImplementedError()

    def delete(self, items: Sequence[NodeIdentifier]) -> list[NodeIdentifier]:
        raise NotImplementedError()


class InfieldAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client)
