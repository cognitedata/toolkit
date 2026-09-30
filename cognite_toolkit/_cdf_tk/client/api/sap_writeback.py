from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId
from cognite_toolkit._cdf_tk.client.resource_classes.sap_writeback import (
    SAPEndpointConnectionCheck,
    SAPEndpointRequest,
    SAPEndpointResponse,
    SAPInstanceRequest,
    SAPInstanceResponse,
    SchemaMappingRequest,
    SchemaMappingResponse,
    WritebackRequestRequest,
    WritebackRequestResponse,
)

_ITEM_LIMIT = 100


class SAPInstancesAPI(CDFResourceAPI[SAPInstanceResponse]):
    """API for SAP instance destinations used by the SAP writeback service."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/writeback/sap/instances", item_limit=_ITEM_LIMIT),
                "retrieve": Endpoint(method="POST", path="/writeback/sap/instances/byids", item_limit=_ITEM_LIMIT),
                "delete": Endpoint(method="POST", path="/writeback/sap/instances/delete", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/writeback/sap/instances", item_limit=_ITEM_LIMIT),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SAPInstanceResponse]:
        return PagedResponse[SAPInstanceResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SAPInstanceRequest]) -> list[SAPInstanceResponse]:
        """Create SAP instances.

        Args:
            items: SAP instances to create. At most 100 per request.
        Returns:
            The created SAP instances. Passwords are not included.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[SAPInstanceResponse]:
        """Retrieve SAP instances by external ID.

        Args:
            items: External IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved SAP instances.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False, force: bool = False) -> None:
        """Delete SAP instances by external ID.

        Deletion fails when an instance is still referenced by an SAP endpoint, unless ``force`` is true.

        Args:
            items: External IDs to delete. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
            force: Delete instances even when they are associated with SAP endpoints.
        """
        self._request_no_response(
            items,
            "delete",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids, "force": force},
        )

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[SAPInstanceResponse]:
        """Fetch one page of SAP instances.

        Args:
            limit: Maximum number of instances to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of SAP instances.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[SAPInstanceResponse]]:
        """Iterate over SAP instances.

        Args:
            limit: Maximum number of instances to return in total. None returns all instances.
        Returns:
            Pages of SAP instances.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[SAPInstanceResponse]:
        """List SAP instances.

        Args:
            limit: Maximum number of instances to return. None returns all instances.
        Returns:
            SAP instances.
        """
        return self._list(limit=limit)


class SAPEndpointsAPI(CDFResourceAPI[SAPEndpointResponse]):
    """API for SAP OData endpoints used by the SAP writeback service."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/writeback/sap/endpoints", item_limit=_ITEM_LIMIT),
                "retrieve": Endpoint(method="POST", path="/writeback/sap/endpoints/byids", item_limit=_ITEM_LIMIT),
                "delete": Endpoint(method="POST", path="/writeback/sap/endpoints/delete", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/writeback/sap/endpoints", item_limit=_ITEM_LIMIT),
            },
        )
        self._verify_endpoint = Endpoint(method="POST", path="/writeback/sap/endpoints/verify", item_limit=1)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SAPEndpointResponse]:
        return PagedResponse[SAPEndpointResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SAPEndpointRequest]) -> list[SAPEndpointResponse]:
        """Create SAP endpoints.

        Args:
            items: SAP endpoints to create. At most 100 per request.
        Returns:
            The created SAP endpoints.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[SAPEndpointResponse]:
        """Retrieve SAP endpoints by external ID.

        Args:
            items: External IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved SAP endpoints.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete SAP endpoints by external ID.

        Args:
            items: External IDs to delete. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def verify(self, external_id: ExternalId) -> SAPEndpointConnectionCheck:
        """Verify connectivity between CDF and an SAP endpoint.

        Args:
            external_id: External ID of the SAP endpoint to verify.
        Returns:
            The connection check result.
        """
        request = RequestMessage(
            endpoint_url=self._make_url(self._verify_endpoint.path),
            method=self._verify_endpoint.method,
            body_content=external_id.dump(),
            api_version=self._api_version,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise(request)
        return SAPEndpointConnectionCheck.model_validate_json(response.body)

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[SAPEndpointResponse]:
        """Fetch one page of SAP endpoints.

        Args:
            limit: Maximum number of endpoints to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of SAP endpoints.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[SAPEndpointResponse]]:
        """Iterate over SAP endpoints.

        Args:
            limit: Maximum number of endpoints to return in total. None returns all endpoints.
        Returns:
            Pages of SAP endpoints.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[SAPEndpointResponse]:
        """List SAP endpoints.

        Args:
            limit: Maximum number of endpoints to return. None returns all endpoints.
        Returns:
            SAP endpoints.
        """
        return self._list(limit=limit)


class SchemaMappingsAPI(CDFResourceAPI[SchemaMappingResponse]):
    """API for schema mappings used by the SAP writeback service."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/writeback/sap/mappings", item_limit=_ITEM_LIMIT),
                "retrieve": Endpoint(method="POST", path="/writeback/sap/mappings/byids", item_limit=_ITEM_LIMIT),
                "delete": Endpoint(method="POST", path="/writeback/sap/mappings/delete", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/writeback/sap/mappings", item_limit=_ITEM_LIMIT),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[SchemaMappingResponse]:
        return PagedResponse[SchemaMappingResponse].model_validate_json(response.body)

    def create(self, items: Sequence[SchemaMappingRequest]) -> list[SchemaMappingResponse]:
        """Create schema mappings.

        Args:
            items: Schema mappings to create. At most 100 per request.
        Returns:
            The created schema mappings.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[SchemaMappingResponse]:
        """Retrieve schema mappings by external ID.

        Args:
            items: External IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        Returns:
            The retrieved schema mappings.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete schema mappings by external ID.

        Args:
            items: External IDs to delete. At most 100 per request.
            ignore_unknown_ids: Ignore external IDs that are not found.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[SchemaMappingResponse]:
        """Fetch one page of schema mappings.

        Args:
            limit: Maximum number of mappings to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of schema mappings.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[SchemaMappingResponse]]:
        """Iterate over schema mappings.

        Args:
            limit: Maximum number of mappings to return in total. None returns all mappings.
        Returns:
            Pages of schema mappings.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[SchemaMappingResponse]:
        """List schema mappings.

        Args:
            limit: Maximum number of mappings to return. None returns all mappings.
        Returns:
            Schema mappings.
        """
        return self._list(limit=limit)


class SAPWritebackAPI(CDFResourceAPI[WritebackRequestResponse]):
    """API for SAP writeback requests, plus instance, endpoint, and schema mapping configuration."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/writeback/sap/requests", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/writeback/sap/requests/byids", item_limit=_ITEM_LIMIT),
                "list": Endpoint(method="GET", path="/writeback/sap/requests", item_limit=_ITEM_LIMIT),
            },
        )
        self.instances = SAPInstancesAPI(http_client)
        self.endpoints = SAPEndpointsAPI(http_client)
        self.mappings = SchemaMappingsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WritebackRequestResponse]:
        return PagedResponse[WritebackRequestResponse].model_validate_json(response.body)

    def create(self, items: Sequence[WritebackRequestRequest]) -> list[WritebackRequestResponse]:
        """Create writeback requests.

        The API accepts one request per call. Larger sequences are sent as separate calls.

        Args:
            items: Writeback requests to create.
        Returns:
            The created writeback requests.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[WritebackRequestId], ignore_unknown_ids: bool = False
    ) -> list[WritebackRequestResponse]:
        """Retrieve writeback requests by request ID.

        Args:
            items: Request IDs to retrieve. At most 100 per request.
            ignore_unknown_ids: Ignore request IDs that are not found.
        Returns:
            The retrieved writeback requests.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[WritebackRequestResponse]:
        """Fetch one page of writeback requests.

        Args:
            limit: Maximum number of requests to return. The server caps this at 100.
            cursor: Cursor for the next page.
        Returns:
            One page of writeback requests.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[WritebackRequestResponse]]:
        """Iterate over writeback requests.

        Args:
            limit: Maximum number of requests to return in total. None returns all requests.
        Returns:
            Pages of writeback requests.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[WritebackRequestResponse]:
        """List writeback requests.

        Args:
            limit: Maximum number of requests to return. None returns all requests.
        Returns:
            Writeback requests. Listed items omit the request payload.
        """
        return self._list(limit=limit)
