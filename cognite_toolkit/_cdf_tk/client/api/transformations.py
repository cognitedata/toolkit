from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.api.transformation_notifications import TransformationNotificationsAPI
from cognite_toolkit._cdf_tk.client.api.transformation_schedules import TransformationSchedulesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import TransformationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    QueryResponse,
    TransformationRequest,
    TransformationResponse,
)


class TransformationsAPI(CDFResourceAPI[InternalOrExternalId, TransformationRequest, TransformationResponse]):
    DEFAULT_TIMEOUT_RUN_QUERY = 240.0  # seconds, this is the maximum timeout for running queries in CDF

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/transformations", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/transformations/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/transformations/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/transformations/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/transformations/filter", item_limit=1000),
            },
        )
        self.schedules = TransformationSchedulesAPI(http_client)
        self.notifications = TransformationNotificationsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TransformationResponse]:
        return PagedResponse[TransformationResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TransformationRequest]) -> list[TransformationResponse]:
        """Create transformations in CDF.

        Args:
            items: List of TransformationRequest objects to create.
        Returns:
            List of created TransformationResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False, with_job_details: bool = False
    ) -> list[TransformationResponse]:
        """Retrieve transformations from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
            with_job_details: Whether the transformations will be returned with running job and last created job details.
        Returns:
            List of retrieved TransformationResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids, "withJobDetails": with_job_details},
        )

    def update(
        self, items: Sequence[TransformationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[TransformationResponse]:
        """Update transformations in CDF.

        Args:
            items: List of TransformationRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated TransformationResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete transformations from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def preview(
        self,
        query: str | None = None,
        convert_to_string: bool = False,
        limit: int | None = 100,
        source_limit: int | None = 100,
        infer_schema_limit: int | None = 10_000,
        timeout: float | None = DEFAULT_TIMEOUT_RUN_QUERY,
    ) -> QueryResponse:
        """`Preview the result of a query. <https://developer.cognite.com/api#tag/Query/operation/runPreview>`_

        Toolkit runs long-running queries that takes longer than the typical default of 30 seconds. In addition,
        we do not want to retry, which typically up to 10 times, as the user will have to wait for a long time. Instead,
        we want to fail provide the user with the error and then let the user decide whether to retry or not by
        running the CLI command again.

        Args:
            query (str | None): SQL query to run for preview.
            convert_to_string (bool): Stringify values in the query results, default is False.
            limit (int | None): Maximum number of rows to return in the final result, default is 100.
            source_limit (int | None): Maximum number of items to read from the data source or None to run without limit, default is 100.
            infer_schema_limit (int | None): Limit for how many rows that are used for inferring result schema, default is 10 000.
            timeout (int | None): Number of seconds to wait before cancelling a query. The default, and maximum, is 240.

        Returns:
            QueryResponse: Result of the executed query
        """
        body: dict[str, Any] = {
            "query": query,
            "convertToString": convert_to_string,
        }
        if limit is not None:
            body["limit"] = limit
        if source_limit is not None:
            body["sourceLimit"] = source_limit
        if infer_schema_limit is not None:
            body["inferSchemaLimit"] = infer_schema_limit
        if timeout is not None:
            # This is the server-side timeout for how long the query is allowed to run before it is cancelled.
            body["timeout"] = timeout

        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._http_client.config.create_api_url("/transformations/query/run"),
                method="POST",
                body_content=body,
                client_timeout=timeout or (self.DEFAULT_TIMEOUT_RUN_QUERY + 60),  # add a buffer to the timeout
                retry=False,
            )
        ).get_success_or_raise()
        return QueryResponse.model_validate_json(response.body)

    def paginate(
        self,
        filter: TransformationFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TransformationResponse]:
        """Iterate over all transformations in CDF.

        Args:
            filter: TransformationFilter object to filter transformations.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TransformationResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter.model_dump(by_alias=True) if filter else {}},
        )

    def iterate(
        self,
        filter: TransformationFilter | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[TransformationResponse]]:
        """Iterate over all transformations in CDF.

        Args:
            filter: TransformationFilter object to filter transformations.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of TransformationResponse objects.
        """
        return self._iterate(
            limit=limit,
            body={"filter": filter.model_dump(by_alias=True) if filter else {}},
        )

    def list(
        self,
        filter: TransformationFilter | None = None,
        limit: int | None = 100,
    ) -> list[TransformationResponse]:
        """List all transformations in CDF.

        Args:
            filter: TransformationFilter object to filter transformations.
            limit: Maximum number of items to return.

        Returns:
            List of TransformationResponse objects.
        """
        return self._list(limit=limit, body={"filter": filter.model_dump(by_alias=True) if filter else {}})
