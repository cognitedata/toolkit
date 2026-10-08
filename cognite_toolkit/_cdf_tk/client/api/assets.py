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
    AggregatedAssetProperty,
    ObjectIds,
    Sort,
    TimeRange,
    _aggregated_properties,
    _dump_model,
    _int_ids,
    _object_ids,
    _str_ids,
    _time_range,
    classic_list_body,
)
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter, GeoLocationFilter, LabelFilter
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.classic_aggregate import (
    AssetPropertyPath,
    ClassicAggregateUniqueBucket,
)


class AssetsAPI(CDFResourceAPI[AssetResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/assets", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/assets/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/assets/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/assets/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/assets/list", item_limit=1000),
                "aggregate": Endpoint(method="POST", path="/assets/aggregate", item_limit=1000),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[AssetResponse]:
        return PagedResponse[AssetResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[AssetRequest]) -> builtins.list[AssetResponse]:
        """Create assets in CDF.

        Args:
            items: List of AssetRequest objects to create.
        Returns:
            List of created AssetResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> builtins.list[AssetResponse]:
        """Retrieve assets from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved AssetResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[AssetRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[AssetResponse]:
        """Update assets in CDF.

        Args:
            items: List of AssetRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated AssetResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(
        self, items: Sequence[InternalOrExternalId], recursive: bool = False, ignore_unknown_ids: bool = False
    ) -> None:
        """Delete assets from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            recursive: Whether to delete assets recursively.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(
            items, "delete", extra_body={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids}
        )

    def paginate(
        self,
        aggregated_properties: bool | Sequence[AggregatedAssetProperty] = False,
        filter: ClassicFilter | dict[str, Any] | None = None,
        limit: int = 100,
        cursor: str | None = None,
        *,
        name: str | None = None,
        parent_ids: int | Sequence[int] | None = None,
        parent_external_ids: str | Sequence[str] | None = None,
        root_ids: ObjectIds | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> PagedResponse[AssetResponse]:
        """Fetch one page of assets.

        Takes the same filter arguments as :meth:`list`.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/listAssets>`_.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._list_body(
                aggregated_properties=aggregated_properties,
                filter=filter,
                name=name,
                parent_ids=parent_ids,
                parent_external_ids=parent_external_ids,
                root_ids=root_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                metadata=metadata,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                root=root,
                external_id_prefix=external_id_prefix,
                labels=labels,
                geo_location=geo_location,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    def iterate(
        self,
        aggregated_properties: bool | Sequence[AggregatedAssetProperty] = False,
        filter: ClassicFilter | dict[str, Any] | None = None,
        limit: int | None = 100,
        *,
        name: str | None = None,
        parent_ids: int | Sequence[int] | None = None,
        parent_external_ids: str | Sequence[str] | None = None,
        root_ids: ObjectIds | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> Iterable[builtins.list[AssetResponse]]:
        """Iterate over assets in CDF.

        Takes the same filter arguments as :meth:`list`. ``limit`` is the maximum number of
        assets to return in total; ``None`` reads every matching asset.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/listAssets>`_.
        """
        return self._iterate(
            limit=limit,
            body=self._list_body(
                aggregated_properties=aggregated_properties,
                filter=filter,
                name=name,
                parent_ids=parent_ids,
                parent_external_ids=parent_external_ids,
                root_ids=root_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                metadata=metadata,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                root=root,
                external_id_prefix=external_id_prefix,
                labels=labels,
                geo_location=geo_location,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    def list(
        self,
        limit: int | None = 100,
        *,
        aggregated_properties: bool | Sequence[AggregatedAssetProperty] = False,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        parent_ids: int | Sequence[int] | None = None,
        parent_external_ids: str | Sequence[str] | None = None,
        root_ids: ObjectIds | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> builtins.list[AssetResponse]:
        """List assets in CDF.

        ``filter`` is a strict filter. Individual arguments override the same field on ``filter``.
        ``advanced_filter`` is the filter DSL (``equals``, ``prefix``, ``exists``, and so on, combined
        with ``and``, ``or``, and ``not``). ``sort`` is one item or a list of ``{property, order, nulls}``.
        ``partition`` is an ``"M/N"`` string; follow the cursor within that partition to read it all.
        ``aggregated_properties`` includes ``childCount``, ``path``, and/or ``depth``. ``True`` includes all three.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/listAssets>`_.
        """
        return self._list(
            limit=limit,
            body=self._list_body(
                aggregated_properties=aggregated_properties,
                filter=filter,
                name=name,
                parent_ids=parent_ids,
                parent_external_ids=parent_external_ids,
                root_ids=root_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                metadata=metadata,
                source=source,
                created_time=created_time,
                last_updated_time=last_updated_time,
                root=root,
                external_id_prefix=external_id_prefix,
                labels=labels,
                geo_location=geo_location,
                advanced_filter=advanced_filter,
                sort=sort,
                partition=partition,
            ),
        )

    @staticmethod
    def _list_body(
        *,
        aggregated_properties: bool | Sequence[AggregatedAssetProperty] = False,
        filter: ClassicFilter | dict[str, Any] | None = None,
        name: str | None = None,
        parent_ids: int | Sequence[int] | None = None,
        parent_external_ids: str | Sequence[str] | None = None,
        root_ids: ObjectIds | None = None,
        asset_subtree_ids: ObjectIds | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: ObjectIds | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: TimeRange | None = None,
        last_updated_time: TimeRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | dict[str, Any] | None = None,
        geo_location: GeoLocationFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        sort: Sort | None = None,
        partition: str | None = None,
    ) -> dict[str, Any]:
        aggregated = _aggregated_properties(aggregated_properties)
        return classic_list_body(
            filter=filter,
            fields={
                "name": name,
                "parentIds": _int_ids(parent_ids),
                "parentExternalIds": _str_ids(parent_external_ids),
                "rootIds": _object_ids(root_ids),
                "assetSubtreeIds": _object_ids(asset_subtree_ids, asset_subtree_external_ids),
                "dataSetIds": _object_ids(data_set_ids, data_set_external_ids),
                "metadata": metadata,
                "source": source,
                "createdTime": _time_range(created_time),
                "lastUpdatedTime": _time_range(last_updated_time),
                "root": root,
                "externalIdPrefix": external_id_prefix,
                "labels": _dump_model(labels),
                "geoLocation": _dump_model(geo_location),
            },
            advanced_filter=advanced_filter,
            sort=sort,
            partition=partition,
            extra={"aggregatedProperties": aggregated} if aggregated is not None else None,
        )

    def count(
        self,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        property: AssetPropertyPath | None = None,
    ) -> int:
        """Count assets matching optional filters.

        When ``property`` is set, count assets where that property is present.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/aggregateAssets>`_.
        """
        return aggregate_count(self, filter=filter, advanced_filter=advanced_filter, property=property)

    def cardinality(
        self,
        property: AssetPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> int:
        """Approximate number of distinct values for ``property``.

        Uses ``cardinalityProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``cardinalityValues`` for every other path.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/aggregateAssets>`_.
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
        property: AssetPropertyPath,
        *,
        filter: ClassicFilter | dict[str, Any] | None = None,
        advanced_filter: dict[str, JsonValue] | None = None,
        aggregate_filter: dict[str, JsonValue] | None = None,
    ) -> builtins.list[ClassicAggregateUniqueBucket]:
        """Distinct values for ``property``, each with a count.

        Uses ``uniqueProperties`` when ``property`` is exactly ``("metadata",)``, and
        ``uniqueValues`` for every other path. Text values are aggregated case-insensitively.

        See `API docs <https://api-docs.cognite.com/20230101/tag/Assets/operation/aggregateAssets>`_.
        """
        return aggregate_unique(
            self,
            property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )
