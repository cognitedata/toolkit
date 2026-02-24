from collections.abc import Sequence
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api.time_series import SortSpec, TimeSeriesAPI
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.time_series import TimeSeries, TimeSeriesFilter, TimeSeriesList, TimeSeriesSort
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class ExtendedTimeSeriesAPI(TimeSeriesAPI):
    """Extended TimeSeriesAPI that uses the alpha API version for retrieve/list
    to include pending instance ID information in responses."""

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> TimeSeries | None:
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            identifiers=identifiers,
            api_subversion="alpha",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TimeSeriesList:
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
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
    ) -> TimeSeriesList:
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
            list_cls=TimeSeriesList,
            resource_cls=TimeSeries,
            method="POST",
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            limit=limit,
            partitions=partitions,
            api_subversion="alpha",
        )
