import builtins
from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.unit import UnitResponse, UnitSystemResponse


class UnitsAPI(CDFResourceAPI[UnitResponse]):
    """Read the CDF unit catalog. The catalog is the same for all projects and cannot be changed."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                # The list endpoint returns the whole catalog in one response and does not paginate.
                "list": Endpoint(method="GET", path="/units", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/units/byids", item_limit=1000),
            },
        )
        self._systems_endpoint = Endpoint(method="GET", path="/units/systems", item_limit=1000)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[UnitResponse]:
        return PagedResponse[UnitResponse].model_validate_json(response.body)

    def _get(self, endpoint: Endpoint) -> SuccessResponse:
        request = RequestMessage(endpoint_url=self._make_url(endpoint.path), method=endpoint.method)
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def list(self) -> builtins.list[UnitResponse]:
        """List all units in the catalog."""
        return self._validate_page_response(self._get(self._method_endpoint_map["list"])).items

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> builtins.list[UnitResponse]:
        """Retrieve units by external ID, for example "temperature:deg_c".

        Args:
            items: The units to retrieve.
            ignore_unknown_ids: Whether to skip units that do not exist.

        Returns:
            The retrieved units.
        """
        return self._request_item_response(items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list_systems(self) -> builtins.list[UnitSystemResponse]:
        """List the unit systems, such as SI and Imperial, with the default unit of each quantity."""
        return ResponseItems[UnitSystemResponse].model_validate_json(self._get(self._systems_endpoint).body).items
