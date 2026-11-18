import builtins
from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.unit import UnitResponse, UnitSystemResponse


class UnitsAPI:
    """Read the CDF unit catalog. The catalog is the same for all projects and cannot be changed."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _request(
        self, path: str, method: Literal["GET", "POST"] = "GET", body: dict[str, Any] | None = None
    ) -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(path), method=method, body_content=body
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def list(self) -> builtins.list[UnitResponse]:
        """List all units in the catalog."""
        return ResponseItems[UnitResponse].model_validate_json(self._request("/units").body).items

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> builtins.list[UnitResponse]:
        """Retrieve units by external ID, for example "temperature:deg_c".

        Args:
            items: The units to retrieve.
            ignore_unknown_ids: Whether to skip units that do not exist.

        Returns:
            The retrieved units.
        """
        if not items:
            return []
        body = {"items": [item.dump() for item in items], "ignoreUnknownIds": ignore_unknown_ids}
        response = self._request("/units/byids", "POST", body)
        return ResponseItems[UnitResponse].model_validate_json(response.body).items

    def list_systems(self) -> builtins.list[UnitSystemResponse]:
        """List the unit systems, such as SI and Imperial, with the default unit of each quantity."""
        return ResponseItems[UnitSystemResponse].model_validate_json(self._request("/units/systems").body).items
