from cognite_toolkit._cdf_tk.client.data_classes.base import T_Resource
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient


class CDFClient:
    """This is an abstraction on top of the HTTP Client that provides some higher-level methods
    like pagination, serialization, and so on."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def paginate(
        self,
    ): ...

    def item_response(self) -> list[T_Resource]: ...

    def chunker(self, items: list[T_Resource], request_limit: int) -> list[list[T_Resource]]: ...
