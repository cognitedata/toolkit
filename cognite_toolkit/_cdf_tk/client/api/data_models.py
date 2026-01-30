"""Data Models API for managing CDF data models.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Data-models/operation/createDataModels
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import DataModelFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DataModelReference,
    DataModelRequest,
    DataModelResponse,
)


class DataModelsAPI(CDFResourceAPI[DataModelReference, DataModelRequest, DataModelResponse]):
    """API for managing CDF data models.

    Data models use an apply/upsert pattern for create and update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/datamodels", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/models/datamodels/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/datamodels/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/datamodels", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DataModelResponse]:
        return PagedResponse[DataModelResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        """Apply (create or update) data models in CDF.

        Args:
            items: List of DataModelRequest objects to apply.

        Returns:
            List of applied DataModelResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[DataModelRequest]) -> list[DataModelResponse]:
        """Apply (create or update) data models in CDF.

        Args:
            items: List of DataModelRequest objects to apply.
        Returns:
            List of applied DataModelResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[DataModelReference], inline_views: bool = False) -> list[DataModelResponse]:
        """Retrieve data models from CDF.

        Args:
            items: List of DataModelReference objects to retrieve.
            inline_views: Whether to include full view definitions in the response.

        Returns:
            List of retrieved DataModelResponse objects.
        """
        return self._request_item_response(items, method="retrieve", extra_body={"inlineViews": inline_views})

    def delete(self, items: Sequence[DataModelReference]) -> None:
        """Delete data models from CDF.

        Args:
            items: List of DataModelReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: DataModelFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[DataModelResponse]:
        """Get a page of data models from CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum number of data models to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of DataModelResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def iterate(
        self,
        filter: DataModelFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[list[DataModelResponse]]:
        """Iterate over all data models in CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum total number of data models to return.

        Returns:
            Iterable of lists of DataModelResponse objects.
        """
        return self._iterate(
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def list(
        self,
        filter: DataModelFilter | None = None,
        limit: int | None = None,
    ) -> list[DataModelResponse]:
        """List all data models in CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum total number of data models to return.

        Returns:
            List of DataModelResponse objects.
        """
        return self._list(limit=limit, params=filter.dump() if filter else None)
