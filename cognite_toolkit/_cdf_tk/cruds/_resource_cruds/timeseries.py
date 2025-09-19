import json
from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from itertools import zip_longest
from pathlib import Path
from typing import Any, Literal, cast, final

from cognite.client.data_classes import (
    DatapointSubscription,
    DatapointSubscriptionList,
    DataPointSubscriptionUpdate,
    DataPointSubscriptionWrite,
    DatapointSubscriptionWriteList,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    TimeSeriesAcl,
    TimeSeriesSubscriptionsAcl,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoints_subscriptions import TimeSeriesIDList
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.constants import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.resource_classes import DatapointSubscriptionYAML, TimeSeriesYAML
from cognite_toolkit._cdf_tk.utils import calculate_hash
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier
from cognite_toolkit._cdf_tk.utils.text import suffix_description

from .auth import GroupAllScopedCRUD, SecurityCategoryCRUD
from .classic import AssetCRUD
from .data_organization import DataSetsCRUD


@final
class TimeSeriesCRUD(ResourceContainerCRUD[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    item_name = "datapoints"
    folder_name = "timeseries"
    filename_pattern = r"^(?!.*DatapointSubscription$).*"
    resource_cls = TimeSeries
    resource_write_cls = TimeSeriesWrite
    list_cls = TimeSeriesList
    list_write_cls = TimeSeriesWriteList
    yaml_cls = TimeSeriesYAML
    kind = "TimeSeries"
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD, AssetCRUD})
    _doc_url = "Time-series/operation/postTimeSeries"

    @property
    def display_name(self) -> str:
        return "time series"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[TimeSeriesWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [TimeSeriesAcl.Action.Read] if read_only else [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write]

        scope: TimeSeriesAcl.Scope.All | TimeSeriesAcl.Scope.DataSet = TimeSeriesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if dataset_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = TimeSeriesAcl.Scope.DataSet(list(dataset_ids))

        return TimeSeriesAcl(actions, scope)

    @classmethod
    def get_id(cls, item: TimeSeries | TimeSeriesWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TimeSeries must have external_id set.")
        return item.external_id

    @classmethod
    def get_internal_id(cls, item: TimeSeries | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        return item.id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryCRUD, security_category
        if "assetExternalId" in item:
            yield AssetCRUD, item["assetExternalId"]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> TimeSeriesWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if security_categories_names := resource.pop("securityCategoryNames", []):
            resource["securityCategories"] = self.client.lookup.security_categories.id(
                security_categories_names, is_dry_run
            )
        if asset_external_id := resource.pop("assetExternalId", None):
            resource["assetId"] = self.client.lookup.assets.id(asset_external_id, is_dry_run)
        return TimeSeriesWrite._load(resource)

    def dump_resource(self, resource: TimeSeries, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if security_categories := dumped.pop("securityCategories", []):
            dumped["securityCategoryNames"] = self.client.lookup.security_categories.external_id(security_categories)
        if asset_id := dumped.pop("assetId", None):
            dumped["assetExternalId"] = self.client.lookup.assets.external_id(asset_id)
        return dumped

    def create(self, items: TimeSeriesWriteList) -> TimeSeriesList:
        return self.client.time_series.create(items)

    def retrieve(self, ids: SequenceNotStr[str | int]) -> TimeSeriesList:
        internal_ids, external_ids = self._split_ids(ids)
        return self.client.time_series.retrieve_multiple(
            ids=internal_ids, external_ids=external_ids, ignore_unknown_ids=True
        )

    def update(self, items: TimeSeriesWriteList) -> TimeSeriesList:
        return self.client.time_series.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str | int]) -> int:
        existing = self.retrieve(ids)
        if existing:
            self.client.time_series.delete(id=existing.as_ids(), ignore_unknown_ids=True)
        return len(existing)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[TimeSeries]:
        return iter(
            self.client.time_series(data_set_external_ids=[data_set_external_id] if data_set_external_id else None)
        )

    def count(self, ids: str | dict[str, Any] | SequenceNotStr[str | dict[str, Any]] | None) -> int:
        datapoints = self.client.time_series.data.retrieve(
            external_id=ids,  # type: ignore[arg-type]
            start=MIN_TIMESTAMP_MS,
            end=MAX_TIMESTAMP_MS + 1,
            aggregates="count",
            granularity="1000d",
            ignore_unknown_ids=True,
        )
        return sum(sum(data.count or []) for data in datapoints)  # type: ignore[union-attr, misc, arg-type]

    def drop_data(self, ids: SequenceNotStr[str] | None) -> int:
        count = self.count(ids)
        existing = self.client.time_series.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        ).as_external_ids()
        for external_id in existing:
            self.client.time_series.data.delete_range(
                external_id=external_id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1
            )
        return count

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))

        spec.add(ParameterSpec(("securityCategoryNames",), frozenset({"list"}), is_required=False, _is_nullable=False))

        spec.add(
            ParameterSpec(("securityCategoryNames", ANY_STR), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        spec.add(ParameterSpec(("assetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec


@final
class DatapointSubscriptionCRUD(
    ResourceCRUD[
        str,
        DataPointSubscriptionWrite,
        DatapointSubscription,
        DatapointSubscriptionWriteList,
        DatapointSubscriptionList,
    ]
):
    folder_name = "timeseries"
    filename_pattern = r"^.*DatapointSubscription$"  # Matches all yaml files who end with *DatapointSubscription.
    resource_cls = DatapointSubscription
    resource_write_cls = DataPointSubscriptionWrite
    list_cls = DatapointSubscriptionList
    list_write_cls = DatapointSubscriptionWriteList
    kind = "DatapointSubscription"
    _doc_url = "Data-point-subscriptions/operation/postSubscriptions"
    dependencies = frozenset(
        {
            TimeSeriesCRUD,
            GroupAllScopedCRUD,
        }
    )
    yaml_cls = DatapointSubscriptionYAML

    _hash_key = "cdf-hash"
    _description_character_limit = 1000
    # A datapoint subscription can hold 10,000 timeseries, but the API
    # only supports 100 timeseries per request. Thus, if a subscription
    # has more than 100 timeseries, we need to split it into multiple requests.
    _TIMESERIES_ID_REQUEST_LIMIT = 100
    _MAX_TIMESERIES_IDS = 10_000

    @property
    def display_name(self) -> str:
        return "timeseries subscriptions"

    @classmethod
    def get_id(cls, item: DataPointSubscriptionWrite | DatapointSubscription | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # The Filter class in the SDK class View implementation is deviating from the API.
        # So we need to modify the spec to match the API.
        parameter_path = ("filter",)
        length = len(parameter_path)
        for item in spec:
            if len(item.path) >= length + 1 and item.path[:length] == parameter_path[:length]:
                # Add extra ANY_STR layer
                # The spec class is immutable, so we use this trick to modify it.
                object.__setattr__(item, "path", (*item.path[:length], ANY_STR, *item.path[length:]))
        spec.add(ParameterSpec(("filter", ANYTHING), frozenset({"dict"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        for timeseries_id in item.get("timeSeriesIds", []):
            yield TimeSeriesCRUD, timeseries_id

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataPointSubscriptionWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [TimeSeriesSubscriptionsAcl.Action.Read]
            if read_only
            else [TimeSeriesSubscriptionsAcl.Action.Read, TimeSeriesSubscriptionsAcl.Action.Write]
        )

        scope: TimeSeriesSubscriptionsAcl.Scope.All | TimeSeriesSubscriptionsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            TimeSeriesSubscriptionsAcl.Scope.All()
        )
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = TimeSeriesSubscriptionsAcl.Scope.DataSet(list(data_set_ids))

        return TimeSeriesSubscriptionsAcl(actions, scope)

    def create(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        created_list = DatapointSubscriptionList([])
        for item in items:
            to_create, batches = self.create_split_timeseries_ids(item)
            created = self.client.time_series.subscriptions.create(to_create)
            for batch_item in batches:
                created = self.client.time_series.subscriptions.update(batch_item)
            created_list.append(created)
        return created_list

    def retrieve(self, ids: SequenceNotStr[str]) -> DatapointSubscriptionList:
        items = DatapointSubscriptionList([])
        for id_ in ids:
            retrieved = self.client.time_series.subscriptions.retrieve(id_)
            if retrieved:
                items.append(retrieved)
        return items

    def update(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        updated_list = DatapointSubscriptionList([])
        for item in items:
            current = self.client.time_series.subscriptions.list_member_time_series(item.external_id, limit=-1)
            to_update, batches = self.update_split_timeseries_ids(item, current)
            # There are two versions of a TimeSeries Subscription, one selects timeseries based filter
            # and the other selects timeseries based on timeSeriesIds. If we use mode='replace', we try
            # to set timeSeriesIds to an empty list, while the filter is set. This will result in an error.
            updated = self.client.time_series.subscriptions.update(to_update, mode="replace_ignore_null")
            for batch_item in batches:
                updated = self.client.time_series.subscriptions.update(batch_item)
            updated_list.append(updated)

        return updated_list

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.time_series.subscriptions.delete(ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.time_series.subscriptions.delete(existing)
            return len(existing)
        else:
            # All deleted successfully
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[DatapointSubscription]:
        return iter(self.client.time_series.subscriptions)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = super().load_resource_file(filepath, environment_variables)
        for resource in resources:
            if "timeSeriesIds" not in resource and "instanceIds" not in resource:
                continue
            # If the timeSeriesIds or instanceIds is set, we need to add the auth hash to the description.
            # such that we can detect if the subscription has changed.
            content: dict[str, object] = {}
            if "timeSeriesIds" in resource:
                content["timeSeriesIds"] = resource["timeSeriesIds"]
            if "instanceIds" in resource:
                content["instanceIds"] = resource["instanceIds"]
            timeseries_hash = calculate_hash(json.dumps(content), shorten=True)
            extra_str = f"{self._hash_key}: {timeseries_hash}"
            resource["description"] = suffix_description(
                extra_str,
                resource.get("description"),
                self._description_character_limit,
                self.get_id(resource),
                self.display_name,
                self.console,
            )

        return resources

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> DataPointSubscriptionWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return DataPointSubscriptionWrite._load(resource)

    def dump_resource(self, resource: DatapointSubscription, local: dict[str, Any] | None = None) -> dict[str, Any]:
        if resource.filter is not None:
            dumped = resource.as_write().dump()
        else:
            # If filter is not set, the subscription uses explicit timeSeriesIds, which are not returned in the
            # response. Calling .as_write() in this case raises ValueError because either filter or
            # timeSeriesIds must be set.
            dumped = resource.dump()
            for server_prop in ("createdTime", "lastUpdatedTime", "timeSeriesCount"):
                dumped.pop(server_prop, None)
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        # timeSeriesIds and instanceIds are not returned in the response, so we need to add them
        # to the dumped resource if they are set in the local resource. If there is a discrepancy between
        # the local and dumped resource, th hash added to the description will change.
        if "timeSeriesIds" in local:
            dumped["timeSeriesIds"] = local["timeSeriesIds"]
        if "instanceIds" in local:
            dumped["instanceIds"] = local["instanceIds"]
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "filter" or json_path == ("timeSeriesIds",):
            return diff_list_hashable(local, cdf)
        elif json_path == ("instanceIds",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)

    @classmethod
    def create_split_timeseries_ids(
        cls, subscription: DataPointSubscriptionWrite
    ) -> tuple[DataPointSubscriptionWrite, list[DataPointSubscriptionUpdate]]:
        """Split the time series IDs into batches of 100.
        This is needed because the API only supports 100 time series IDs per request.

        Note this is the total of time series IDs and instance IDs.
        """
        total_timeseries = len(subscription.time_series_ids or []) + len(subscription.instance_ids or [])
        cls._validate_total_below_limit(subscription, total_timeseries)
        if total_timeseries <= cls._TIMESERIES_ID_REQUEST_LIMIT:
            # If the subscription has less than or equal to 100 time series IDs, we can return it as is.
            # No need to split into batches.
            return subscription, []

        # Serialization to create a copy of the subscription
        to_create = DataPointSubscriptionWrite.load(subscription.dump())
        all_timeseries_ids = to_create.time_series_ids or []
        all_instance_ids = to_create.instance_ids or []

        # Create the first batch for the create/update call, prioritizing time_series_ids
        to_create.time_series_ids = all_timeseries_ids[: cls._TIMESERIES_ID_REQUEST_LIMIT]
        space_in_first_batch = cls._TIMESERIES_ID_REQUEST_LIMIT - len(to_create.time_series_ids)
        to_create.instance_ids = all_instance_ids[:space_in_first_batch]

        # Prepare remaining IDs for update batches
        remaining_timeseries_ids = all_timeseries_ids[len(to_create.time_series_ids) :]
        remaining_instance_ids = all_instance_ids[len(to_create.instance_ids) :]

        # Using a list of tuples to preserve the type of ID
        all_remaining_ids = [("ts", id) for id in remaining_timeseries_ids] + [
            ("instance", id) for id in remaining_instance_ids
        ]

        batches: list[DataPointSubscriptionUpdate] = []
        for chunk in chunker(all_remaining_ids, cls._TIMESERIES_ID_REQUEST_LIMIT):
            update = DataPointSubscriptionUpdate(external_id=subscription.external_id)
            ts_ids_in_chunk, instance_ids_in_chunk = cls._split_ts_instance_ids(chunk)
            if ts_ids_in_chunk:
                update.time_series_ids.add(ts_ids_in_chunk)
            if instance_ids_in_chunk:
                update.instance_ids.add(instance_ids_in_chunk)
            batches.append(update)
        return to_create, batches

    @classmethod
    def _validate_total_below_limit(cls, subscription: DataPointSubscriptionWrite, total_timeseries: int) -> None:
        if total_timeseries > cls._MAX_TIMESERIES_IDS:
            raise ToolkitValueError(
                f'Subscription "{subscription.external_id}" has {total_timeseries:,} time series, '
                f"which is more than the limit of {cls._MAX_TIMESERIES_IDS:,}."
            )

    @classmethod
    def _split_ts_instance_ids(
        cls, ids: list[tuple[Literal["ts"], str] | tuple[Literal["instance"], NodeId]]
    ) -> tuple[list[str], list[NodeId]]:
        ts_ids, instance_ids = [], []
        for id_type, identifier in ids:
            if id_type == "ts":
                ts_ids.append(identifier)
            else:
                instance_ids.append(identifier)
        # MyPy fails to understand the logic above ensures that ts_ids is a list of str
        # and instance_ids is a list of NodeId.
        return ts_ids, instance_ids  # type: ignore[return-value]

    @classmethod
    def update_split_timeseries_ids(
        cls, subscription: DataPointSubscriptionWrite, current_ts: TimeSeriesIDList
    ) -> tuple[DataPointSubscriptionWrite, list[DataPointSubscriptionUpdate]]:
        """Split the time series IDs into batches of 100.
        This is needed because the API only supports 100 time series IDs per request.

        Note this is the total of time series IDs and instance IDs.

        In addition, this method will compare to the current times series IDs and
        update the subscription with adding and removing time series IDs as needed.
        """
        if subscription.time_series_ids is None and subscription.instance_ids is None:
            # The subscription is using a filter, so we can return it as is.
            return subscription, []
        total_timeseries = len(subscription.time_series_ids or []) + len(subscription.instance_ids or [])
        cls._validate_total_below_limit(subscription, total_timeseries)

        # Serialization to create a copy of the subscription
        to_update = DataPointSubscriptionWrite.load(subscription.dump())

        # Get desired time series IDs
        desired_timeseries_ids = set(to_update.time_series_ids or [])
        desired_instance_ids = set(to_update.instance_ids or [])

        # Get current time series IDs from the subscription
        current_timeseries_ids: set[str] = set()
        current_instance_ids: set[NodeId] = set()
        for ts in current_ts:
            if ts.external_id and ts.instance_id is None:
                current_timeseries_ids.add(ts.external_id)
            elif ts.instance_id and ts.external_id is None:
                current_instance_ids.add(ts.instance_id)
            elif ts.external_id and ts.instance_id:
                # Migrated time series with both external_id and instance_id
                if ts.external_id in desired_timeseries_ids and ts.instance_id not in desired_instance_ids:
                    current_timeseries_ids.add(ts.external_id)
                elif ts.external_id not in desired_timeseries_ids and ts.instance_id in desired_instance_ids:
                    current_instance_ids.add(ts.instance_id)
                elif ts.external_id in desired_timeseries_ids and ts.instance_id in desired_instance_ids:
                    current_timeseries_ids.add(ts.external_id)
                    current_instance_ids.add(ts.instance_id)
                else:
                    # It is in neither of the desired sets, so it will be removed.
                    # We use instanceId as a preference to avoid duplicates.
                    current_instance_ids.add(ts.instance_id)

        # Calculate what needs to be added and removed
        ts_to_add = desired_timeseries_ids - current_timeseries_ids
        ts_to_remove = current_timeseries_ids - desired_timeseries_ids
        instance_to_add = desired_instance_ids - current_instance_ids
        instance_to_remove = current_instance_ids - desired_instance_ids

        # Clear the time series IDs from the main update to avoid conflicts
        # The to_update object is used to update all other properties of the subscription.
        to_update.time_series_ids = None
        to_update.instance_ids = None

        # Create update batches for changes
        batches: list[DataPointSubscriptionUpdate] = []
        all_removals = [("ts", id_) for id_ in ts_to_remove] + [("instance", id_) for id_ in instance_to_remove]
        all_additions = [("ts", id_) for id_ in ts_to_add] + [("instance", id_) for id_ in instance_to_add]
        for removals, additions in zip_longest(
            chunker(all_removals, cls._TIMESERIES_ID_REQUEST_LIMIT),
            chunker(all_additions, cls._TIMESERIES_ID_REQUEST_LIMIT),
            fillvalue=None,
        ):
            update = DataPointSubscriptionUpdate(external_id=subscription.external_id)
            ts_ids_to_remove, instance_ids_to_remove = cls._split_ts_instance_ids(removals or [])
            if ts_ids_to_remove:
                update.time_series_ids.remove(ts_ids_to_remove)
            if instance_ids_to_remove:
                update.instance_ids.remove(instance_ids_to_remove)

            ts_ids_to_add, instance_ids_to_add = cls._split_ts_instance_ids(additions or [])
            if ts_ids_to_add:
                update.time_series_ids.add(ts_ids_to_add)
            if instance_ids_to_add:
                update.instance_ids.add(instance_ids_to_add)
            batches.append(update)

        return to_update, batches
