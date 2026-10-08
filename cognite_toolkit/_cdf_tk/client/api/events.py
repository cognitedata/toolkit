import builtins
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.api._classic_aggregate import (
    aggregate_cardinality,
    aggregate_count,
    aggregate_unique,
)
from cognite_toolkit._cdf_tk.client.api._classic_list import ObjectIds, Sort, TimeRange, event_list_body
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.classic_aggregate import (
    ClassicAggregateUniqueBucket,
    EventPropertyPath,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse


class EventsAPI(CDFResourceAPI[EventResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/events", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/events/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/events/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/events/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/events/list", item_limit=1000),
                "aggregate": Endpoint(method="POST", path="/events/aggregate", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[EventResponse]:
        return PagedResponse[EventResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[EventRequest]) -> builtins.list[EventResponse]:
        """Create events in CDF.

        Args:
            items: List of EventRequest objects to create.
        Returns:
            List of created EventResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[EventResponse]:
        """Retrieve events from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved EventResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[EventRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[EventResponse]:
        """Update events in CDF.

        Args:
            items: List of EventRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated EventResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete events from CDF.

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
        start_time: TimeRange | None = None,
        end_time: TimeRange | None = None,
        active_at_time: TimeRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> PagedResponse[EventResponse]:
        """Fetch one page of events.

        Takes the same filter arguments as :meth:`list`.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/advancedListEvents>`_.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._list_body(
                filter=filter,
                start_time=start_time,
                end_time=end_time,
                active_at_time=active_at_time,
                type=type,
                subtype=subtype,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                external_id_prefix=external_id_prefix,
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
        start_time: TimeRange | None = None,
        end_time: TimeRange | None = None,
        active_at_time: TimeRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> Iterable[builtins.list[EventResponse]]:
        """Iterate over events in CDF.

        Takes the same filter arguments as :meth:`list`. ``limit`` is the maximum number of
        events to return in total; ``None`` reads every matching event.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/advancedListEvents>`_.
        """
        return self._iterate(
            limit=limit,
            body=self._list_body(
                filter=filter,
                start_time=start_time,
                end_time=end_time,
                active_at_time=active_at_time,
                type=type,
                subtype=subtype,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                external_id_prefix=external_id_prefix,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    def list(
        self,
        filter: ClassicFilter | dict[str, Any] | None = None,
        limit: int | None = 100,
        *,
        start_time: TimeRange | None = None,
        end_time: TimeRange | None = None,
        active_at_time: TimeRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> builtins.list[EventResponse]:
        """List events in CDF.

        ``filter`` is a strict filter, either :class:`ClassicFilter` or the API filter object.
        Individual arguments override the same field on ``filter``. ``advanced_filter`` is the filter DSL.
        ``sort`` is one item or a list of ``{property, order, nulls}``. ``partition`` is an ``"M/N"`` string.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/advancedListEvents>`_.
        """
        return self._list(
            limit=limit,
            body=self._list_body(
                filter=filter,
                start_time=start_time,
                end_time=end_time,
                active_at_time=active_at_time,
                type=type,
                subtype=subtype,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_external_ids=asset_external_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                external_id_prefix=external_id_prefix,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    @staticmethod
    def _list_body(
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        start_time: TimeRange | None = None,
        end_time: TimeRange | None = None,
        active_at_time: TimeRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: int | Sequence[int] | None = None,
        asset_external_ids: str | Sequence[str] | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        external_id_prefix: str | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> dict[str, Any]:
        return event_list_body(
            filter=filter,
            start_time=start_time,
            end_time=end_time,
            active_at_time=active_at_time,
            type=type,
            subtype=subtype,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_ids=data_set_ids,
            data_set_external_ids=data_set_external_ids,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            advanced_filter=advanced_filter,
            sort=sort,
            partition=partition,
        )

    def count(
        self,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        property: EventPropertyPath | None = None,
    ) -> int:
        """Count events matching optional filters.

        When ``property`` is set, count events where that property is present.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/aggregateEvents>`_.
        """
        return aggregate_count(self, filter=filter, advanced_filter=advanced_filter, property=property)

    def cardinality(
        self,
        property: EventPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> int:
        """Approximate number of distinct values for ``property``.

        Uses ``cardinalityProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``cardinalityValues`` for every other path.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/aggregateEvents>`_.
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
        property: EventPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> builtins.list[ClassicAggregateUniqueBucket]:
        """Distinct values for ``property``, each with a count.

        Uses ``uniqueProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``uniqueValues`` for every other path. Text values are aggregated case-insensitively.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Events/operation/aggregateEvents>`_.
        """
        return aggregate_unique(
            self,
            property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )
