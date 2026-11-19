"""Metering API for reading CDF consumption data (alpha).

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Metering
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import MeterId
from cognite_toolkit._cdf_tk.client.resource_classes.metering import Meter


class MeteringAPI(CDFResourceAPI[Meter]):
    """API for the Cognite Metering endpoints (read-only).

    Without ``start`` and ``number_of_datapoints`` only meter metadata is returned. When both are given,
    the response includes historical consumption data with averaged values.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="POST", path="/metering/meters/list", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/metering/meters/byids", item_limit=100),
            },
            api_version="20230101-alpha",
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[Meter]:
        return PagedResponse[Meter].model_validate_json(response.body)

    @staticmethod
    def _create_body(
        prefix: str | None = None,
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
    ) -> dict[str, Any]:
        if number_of_datapoints is not None and not (0 <= number_of_datapoints <= 600):
            raise ValueError(f"number_of_datapoints must be between 0 and 600, got {number_of_datapoints}.")
        body: dict[str, Any] = {}
        if prefix is not None:
            body["filter"] = {"prefix": {"property": ["meterId"], "value": prefix}}
        if start is not None:
            body["start"] = start
        if end is not None:
            body["end"] = end
        if number_of_datapoints is not None:
            body["numberOfDatapoints"] = number_of_datapoints
        return body

    def retrieve(
        self,
        items: Sequence[MeterId],
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
    ) -> builtins.list[Meter]:
        """Retrieve consumption data for the given meters.

        Args:
            items: The meter IDs to retrieve.
            start: Start timestamp (inclusive) in milliseconds since epoch for historical data.
            end: End timestamp (inclusive) in milliseconds since epoch. Defaults to now in the API.
            number_of_datapoints: Number of datapoints (0-600) to return between start and end.

        Returns:
            The meters in the same order as requested.
        """
        body = self._create_body(start=start, end=end, number_of_datapoints=number_of_datapoints)
        return self._request_item_response(items, "retrieve", extra_body=body)

    def paginate(
        self,
        prefix: str | None = None,
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[Meter]:
        """Fetch a single page of meters.

        Args:
            prefix: Only return meters whose ID starts with this prefix, e.g. ``atlas.``.
            start: Start timestamp (inclusive) in milliseconds since epoch for historical data.
            end: End timestamp (inclusive) in milliseconds since epoch. Defaults to now in the API.
            number_of_datapoints: Number of datapoints (0-600) to return between start and end.
            limit: Maximum number of meters to return.
            cursor: Cursor for pagination.
        """
        body = self._create_body(prefix, start, end, number_of_datapoints)
        return self._paginate(limit=limit, cursor=cursor, body=body)

    def iterate(
        self,
        prefix: str | None = None,
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
        limit: int | None = None,
    ) -> Iterable[builtins.list[Meter]]:
        """Iterate over meters, handling pagination automatically."""
        body = self._create_body(prefix, start, end, number_of_datapoints)
        return self._iterate(limit=limit, body=body)

    def list(
        self,
        prefix: str | None = None,
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
        limit: int | None = 100,
    ) -> builtins.list[Meter]:
        """List meters.

        Args:
            prefix: Only return meters whose ID starts with this prefix, e.g. ``atlas.``.
            start: Start timestamp (inclusive) in milliseconds since epoch for historical data.
            end: End timestamp (inclusive) in milliseconds since epoch. Defaults to now in the API.
            number_of_datapoints: Number of datapoints (0-600) to return between start and end.
            limit: Maximum number of meters to return. None returns all.
        """
        body = self._create_body(prefix, start, end, number_of_datapoints)
        return self._list(limit=limit, body=body)
