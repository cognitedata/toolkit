import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.api._classic_aggregate import (
    aggregate_cardinality,
    aggregate_count,
    aggregate_unique,
)
from cognite_toolkit._cdf_tk.client.api._classic_list import (
    ObjectIds,
    Sort,
    TimeRange,
    classic_list_body,
    int_ids,
    object_ids,
    str_ids,
    time_range,
)
from cognite_toolkit._cdf_tk.client.api.datapoints import DatapointsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.classic_aggregate import (
    ClassicAggregateUniqueBucket,
    TimeSeriesPropertyPath,
)
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse


class TimeSeriesAPI(CDFResourceAPI[TimeSeriesResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/timeseries", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/timeseries/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/timeseries/update", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/timeseries/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path="/timeseries/list", item_limit=1000),
                "aggregate": Endpoint(method="POST", path="/timeseries/aggregate", item_limit=1000),
            },
            api_version="alpha",
        )
        self.datapoints = DatapointsAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[TimeSeriesResponse]:
        return PagedResponse[TimeSeriesResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TimeSeriesRequest]) -> builtins.list[TimeSeriesResponse]:
        """Create time series in CDF.

        Args:
            items: List of TimeSeriesRequest objects to create.
        Returns:
            List of created TimeSeriesResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalId | ExternalId | InstanceId], ignore_unknown_ids: bool = False
    ) -> builtins.list[TimeSeriesResponse]:
        """Retrieve time series from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved TimeSeriesResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[TimeSeriesRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[TimeSeriesResponse]:
        """Update time series in CDF.

        Args:
            items: List of TimeSeriesRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated TimeSeriesResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete time series from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        filter: ClassicFilter | dict[str, Any] | None = None,
        limit: int = 100,
        cursor: str | None = None,
        *,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: int | Sequence[int] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        external_id_prefix: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> PagedResponse[TimeSeriesResponse]:
        """Fetch one page of time series.

        Takes the same filter arguments as :meth:`list`.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/listTimeSeries>`_.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._list_body(
                filter=filter,
                name=name,
                unit=unit,
                unit_external_id=unit_external_id,
                unit_quantity=unit_quantity,
                is_string=is_string,
                is_step=is_step,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                external_id_prefix=external_id_prefix,
                created_time=created_time,
                last_updated_time=last_updated_time,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    def iterate(
        self,
        filter: ClassicFilter | dict[str, Any] | None = None,
        limit: int | None = 100,
        *,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: int | Sequence[int] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        external_id_prefix: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> Iterable[builtins.list[TimeSeriesResponse]]:
        """Iterate over time series in CDF.

        Takes the same filter arguments as :meth:`list`. ``limit`` is the maximum number of
        time series to return in total; ``None`` reads every matching time series.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/listTimeSeries>`_.
        """
        return self._iterate(
            limit=limit,
            body=self._list_body(
                filter=filter,
                name=name,
                unit=unit,
                unit_external_id=unit_external_id,
                unit_quantity=unit_quantity,
                is_string=is_string,
                is_step=is_step,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                external_id_prefix=external_id_prefix,
                created_time=created_time,
                last_updated_time=last_updated_time,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    def list(
        self,
        limit: int | None = 100,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: int | Sequence[int] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        external_id_prefix: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> builtins.list[TimeSeriesResponse]:
        """List time series in CDF.

        ``filter`` is a strict filter. Individual arguments override the same field on ``filter``.
        ``advanced_filter`` is the filter DSL. ``sort`` is one item or a list of ``{property, order, nulls}``.
        ``partition`` is an ``"M/N"`` string. ``root_asset_ids`` are internal asset ids.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/listTimeSeries>`_.
        """
        return self._list(
            limit=limit,
            body=self._list_body(
                filter=filter,
                name=name,
                unit=unit,
                unit_external_id=unit_external_id,
                unit_quantity=unit_quantity,
                is_string=is_string,
                is_step=is_step,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                root_asset_ids=root_asset_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                external_id_prefix=external_id_prefix,
                created_time=created_time,
                last_updated_time=last_updated_time,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    @staticmethod
    def _list_body(
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        root_asset_ids: int | Sequence[int] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        external_id_prefix: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> dict[str, Any]:
        return classic_list_body(
            filter=filter,
            fields={
                "name": name,
                "unit": unit,
                "unitExternalId": unit_external_id,
                "unitQuantity": unit_quantity,
                "isString": is_string,
                "isStep": is_step,
                "metadata": metadata,
                "assetIds": int_ids(asset_ids),
                "assetExternalIds": str_ids(asset_external_ids),
                "rootAssetIds": int_ids(root_asset_ids),
                "assetSubtreeIds": object_ids(asset_subtree_ids, asset_subtree_external_ids),
                "dataSetIds": object_ids(data_set_ids, data_set_external_ids),
                "externalIdPrefix": external_id_prefix,
                "createdTime": time_range(created_time),
                "lastUpdatedTime": time_range(last_updated_time),
            },
            advanced_filter=advanced_filter,
            sort=sort,
            partition=partition,
        )

    def count(
        self,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
    ) -> int:
        """Count time series matching optional filters.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/aggregateTimeSeries>`_.
        """
        return aggregate_count(self, filter=filter, advanced_filter=advanced_filter)

    def cardinality(
        self,
        property: TimeSeriesPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> int:
        """Approximate number of distinct values for ``property``.

        Uses ``cardinalityProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``cardinalityValues`` for every other path.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/aggregateTimeSeries>`_.
        """
        return aggregate_cardinality(
            self,
            property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def unique(
        self,
        property: TimeSeriesPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> builtins.list[ClassicAggregateUniqueBucket]:
        """Distinct values for ``property``, each with a count.

        Uses ``uniqueProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``uniqueValues`` for every other path. The service returns at most 1000 buckets.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Time-series/operation/aggregateTimeSeries>`_.
        """
        return aggregate_unique(
            self,
            property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def set_pending_ids(self, items: Sequence[PendingInstanceId]) -> builtins.list[TimeSeriesResponse]:
        """Set pending instance IDs for one or more time series.

        This links asset-centric time series to DM nodes that will be created
        by the syncer service.

        Args:
            items: Sequence of PendingInstanceId objects containing the pending
                instance IDs and the time series id or external_id to link them to.

        Returns:
            List of updated TimeSeriesResponse objects.
        """
        return self._request_item_response(items, method="retrieve", endpoint="/timeseries/set-pending-instance-ids")

    def unlink_instance_ids(self, items: Sequence[InternalOrExternalId]) -> builtins.list[TimeSeriesResponse]:
        """Unlink instance IDs from time series.

        This allows a CogniteTimeSeries node in Data Modeling to be deleted
        without deleting the underlying time series data.

        Args:
            items: Sequence of InternalOrExternalId identifying the time series to unlink.

        Returns:
            List of updated TimeSeriesResponse objects.
        """
        return self._request_item_response(items, method="retrieve", endpoint="/timeseries/unlink-instance-ids")
