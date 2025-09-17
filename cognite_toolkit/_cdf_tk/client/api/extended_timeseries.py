from collections.abc import Sequence
from typing import Any, cast, overload

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.time_series import SortSpec, TimeSeriesAPI
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.time_series import TimeSeriesFilter, TimeSeriesSort
from cognite.client.utils._auxiliary import split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries, ExtendedTimeSeriesList
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId


class ExtendedTimeSeriesAPI(TimeSeriesAPI):
    """Extended TimeSeriesAPI to include pending ID methods."""

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._PENDING_IDS_LIMIT = 1000
        self._UNLINK_LIMIT = 1000

    @overload
    def set_pending_ids(
        self, instance_id: NodeId | tuple[str, str], id: int | None = None, external_id: str | None = None
    ) -> ExtendedTimeSeries: ...

    @overload
    def set_pending_ids(self, instance_id: Sequence[PendingInstanceId]) -> ExtendedTimeSeriesList: ...

    def set_pending_ids(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[PendingInstanceId],
        id: int | None = None,
        external_id: str | None = None,
    ) -> ExtendedTimeSeries | ExtendedTimeSeriesList:
        """Set a pending identifier for a time series.

        Args:
            instance_id: The pending instance ID to set.
            id: The ID of the time series.
            external_id: The external ID of the time series.

        Returns:
            ExtendedTimeSeries: If a single instance ID is provided, returns the updated ExtendedTimeSeries object.
        """
        if isinstance(instance_id, NodeId) or (
            isinstance(instance_id, tuple)
            and len(instance_id) == 2
            and isinstance(instance_id[0], str)
            and isinstance(instance_id[1], str)
        ):
            return self._set_pending_ids([PendingInstanceId(instance_id, id=id, external_id=external_id)])[0]  # type: ignore[return-value, arg-type]
        elif isinstance(instance_id, Sequence) and all(isinstance(item, PendingInstanceId) for item in instance_id):
            return self._set_pending_ids(instance_id)  # type: ignore[arg-type]
        else:
            raise TypeError(
                "instance_id must be a NodeId, a tuple of (str, str), or a sequence of PendingIdentifier objects."
            )

    def _set_pending_ids(self, identifiers: Sequence[PendingInstanceId]) -> ExtendedTimeSeriesList:
        """Set multiple pending identifiers for time series.

        Args:
            identifiers: A sequence of PendingIdentifier objects containing the pending instance IDs and optional IDs or external IDs.

        Returns:
            ExtendedTimeSeriesList: A list of ExtendedTimeSeries objects with the updated pending identifiers.
        """
        tasks = [
            {
                "url_path": f"{self._RESOURCE_PATH}/set-pending-instance-ids",
                "json": {
                    "items": [identifier.dump(camel_case=True) for identifier in id_chunk],
                },
                "api_subversion": "alpha",
            }
            for id_chunk in split_into_chunks(list(identifiers), self._PENDING_IDS_LIMIT)
        ]
        tasks_summary = execute_tasks(
            self._post,
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
        )

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        return ExtendedTimeSeriesList._load(retrieved_items, cognite_client=self._cognite_client)

    @overload
    def unlink_instance_ids(
        self,
        id: int | None = None,
        external_id: str | None = None,
    ) -> ExtendedTimeSeries | None: ...

    @overload
    def unlink_instance_ids(
        self,
        id: Sequence[int] | None = None,
        external_id: SequenceNotStr[str] | None = None,
    ) -> ExtendedTimeSeriesList: ...

    def unlink_instance_ids(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
    ) -> ExtendedTimeSeries | ExtendedTimeSeriesList | None:
        """Unlink pending instance IDs from time series.

        Args:
            id (int | Sequence[int] | None): The ID(s) of the time series.
            external_id (str | SequenceNotStr[str] | None): The external ID(s) of the time series.

        """
        if id is None and external_id is None:
            return None
        if isinstance(id, int) and isinstance(external_id, str):
            raise ValueError("Cannot specify both id and external_id as single values. Use one or the other.")
        is_single = isinstance(id, int) or isinstance(external_id, str)
        identifiers = IdentifierSequence.load(id, external_id)

        tasks = [
            {
                "url_path": f"{self._RESOURCE_PATH}/unlink-instance-ids",
                "json": {"items": id_chunk},
                "api_subversion": "alpha",
            }
            for id_chunk in split_into_chunks(identifiers.as_dicts(), self._UNLINK_LIMIT)
        ]
        tasks_summary = execute_tasks(
            self._post,
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
        )

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        result = ExtendedTimeSeriesList._load(retrieved_items, cognite_client=self._cognite_client)
        if is_single:
            if len(result) == 0:
                return None
            if len(result) > 1:
                raise ValueError("Expected a single time series, but multiple were returned.")
            return cast(ExtendedTimeSeries, result[0])
        return result

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> ExtendedTimeSeries | None:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            TimeSeries | None: Requested time series or None if it does not exist.

        Examples:

            Get time series by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve(id=1)

            Get time series by external id:

                >>> res = client.time_series.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtendedTimeSeriesList,
            resource_cls=ExtendedTimeSeries,
            identifiers=identifiers,
            api_subversion="alpha",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtendedTimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            Get time series by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve_multiple(ids=[1, 2, 3])

            Get time series by external id:

                >>> res = client.time_series.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=ExtendedTimeSeriesList,
            resource_cls=ExtendedTimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            api_subversion="alpha",
        )

    def list(
        self,
        name: str | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        unit_quantity: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, Any] | None = None,
        external_id_prefix: str | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> ExtendedTimeSeriesList:
        """`List time series <https://developer.cognite.com/api#tag/Time-series/operation/listTimeSeries>`_

        Args:
            name (str | None): Name of the time series. Often referred to as tag.
            unit (str | None): Unit of the time series.
            unit_external_id (str | None): Filter on unit external ID.
            unit_quantity (str | None): Filter on unit quantity.
            is_string (bool | None): Whether the time series is a string time series.
            is_step (bool | None): Whether the time series is a step (piecewise constant) time series.
            asset_ids (Sequence[int] | None): List time series related to these assets.
            asset_external_ids (SequenceNotStr[str] | None): List time series related to these assets.
            asset_subtree_ids (int | Sequence[int] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include time series that are related to an asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only time series in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only time series in the specified data set(s) with this external id / these external ids.
            metadata (dict[str, Any] | None): Custom, application specific metadata. String key -> String value
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            limit (int | None): Maximum number of time series to return.  Defaults to 25. Set to -1, float("inf") or None to return all items.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not. See examples below for usage.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Returns:
            TimeSeriesList: The requested time series.

        .. note::
            When using `partitions`, there are few considerations to keep in mind:
                * `limit` has to be set to `None` (or `-1`).
                * API may reject requests if you specify more than 10 partitions. When Cognite enforces this behavior, the requests result in a 400 Bad Request status.
                * Partitions are done independently of sorting: there's no guarantee of the sort order between elements from different partitions. For this reason providing a `sort` parameter when using `partitions` is not allowed.

        Examples:

            List time series:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.list(limit=5)

            Iterate over time series:

                >>> for ts in client.time_series:
                ...     ts # do something with the time series

            Iterate over chunks of time series to reduce memory load:

                >>> for ts_list in client.time_series(chunk_size=2500):
                ...     ts_list # do something with the time series

            Using advanced filter, find all time series that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client.data_classes import filters
                >>> in_timezone = filters.Prefix(["metadata", "timezone"], "Europe")
                >>> res = client.time_series.list(advanced_filter=in_timezone, sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `TimeSeriesProperty` and `SortableTimeSeriesProperty` Enums.

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.time_series import TimeSeriesProperty, SortableTimeSeriesProperty
                >>> in_timezone = filters.Prefix(TimeSeriesProperty.metadata_key("timezone"), "Europe")
                >>> res = client.time_series.list(
                ...     advanced_filter=in_timezone,
                ...     sort=(SortableTimeSeriesProperty.external_id, "asc"))

            Combine filter and advanced filter:

                >>> from cognite.client.data_classes import filters
                >>> not_instrument_lvl5 = filters.And(
                ...    filters.ContainsAny("labels", ["Level5"]),
                ...    filters.Not(filters.ContainsAny("labels", ["Instrument"]))
                ... )
                >>> res = client.time_series.list(asset_subtree_ids=[123456], advanced_filter=not_instrument_lvl5)
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = TimeSeriesFilter(
            name=name,
            unit=unit,
            unit_external_id=unit_external_id,
            unit_quantity=unit_quantity,
            is_step=is_step,
            is_string=is_string,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            metadata=metadata,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, TimeSeriesSort)
        self._validate_filter(advanced_filter)

        return self._list(
            list_cls=ExtendedTimeSeriesList,
            resource_cls=ExtendedTimeSeries,
            method="POST",
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            limit=limit,
            partitions=partitions,
            api_subversion="alpha",
        )
