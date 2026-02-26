import json
from collections.abc import Hashable, Iterable, Sequence
from itertools import zip_longest
from pathlib import Path
from typing import Any, Literal, final

from cognite.client.data_classes.capabilities import (
    Capability,
    TimeSeriesAcl,
    TimeSeriesSubscriptionsAcl,
)

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalOrExternalId, NameId
from cognite_toolkit._cdf_tk.client.identifiers._references import DatapointSubscriptionTimeSeriesId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import (
    AddRemove,
    DatapointSubscriptionRequest,
    DatapointSubscriptionResponse,
    DataPointSubscriptionUpdate,
    DatapointSubscriptionUpdateRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
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
class TimeSeriesCRUD(ResourceContainerCRUD[ExternalId, TimeSeriesRequest, TimeSeriesResponse]):
    item_name = "datapoints"
    folder_name = "timeseries"
    resource_cls = TimeSeriesResponse
    resource_write_cls = TimeSeriesRequest
    yaml_cls = TimeSeriesYAML
    kind = "TimeSeries"
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD, AssetCRUD})
    _doc_url = "Time-series/operation/postTimeSeries"

    @property
    def display_name(self) -> str:
        return "time series"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[TimeSeriesRequest] | None, read_only: bool
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
    def get_id(cls, item: TimeSeriesRequest | TimeSeriesResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("TimeSeries must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: TimeSeriesResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryCRUD, NameId(name=security_category)
        if "assetExternalId" in item:
            yield AssetCRUD, ExternalId(external_id=item["assetExternalId"])

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> TimeSeriesRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if security_categories_names := resource.pop("securityCategoryNames", []):
            resource["securityCategories"] = self.client.lookup.security_categories.id(
                security_categories_names, is_dry_run
            )
        if asset_external_id := resource.pop("assetExternalId", None):
            resource["assetId"] = self.client.lookup.assets.id(asset_external_id, is_dry_run)
        return TimeSeriesRequest.model_validate(resource)

    def dump_resource(self, resource: TimeSeriesResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local_dict = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if security_categories := dumped.pop("securityCategories", []):
            dumped["securityCategoryNames"] = self.client.lookup.security_categories.external_id(security_categories)
        if asset_id := dumped.pop("assetId", None):
            dumped["assetExternalId"] = self.client.lookup.assets.external_id(asset_id)
        if local is not None:
            for key, default in [
                ("isStep", False),
                ("isString", False),
                ("metadata", {}),
            ]:
                if dumped.get(key) == default and key not in local_dict:
                    dumped.pop(key)
        return dumped

    def create(self, items: Sequence[TimeSeriesRequest]) -> list[TimeSeriesResponse]:
        return self.client.tool.timeseries.create(items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[TimeSeriesResponse]:
        return self.client.tool.timeseries.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[TimeSeriesRequest]) -> list[TimeSeriesResponse]:
        return self.client.tool.timeseries.update(items, mode="replace")

    def delete(self, ids: Sequence[InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.timeseries.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[TimeSeriesResponse]:
        filter_ = ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=data_set_external_id)
        for timeseries in self.client.tool.timeseries.iterate(filter=filter_, limit=None):
            yield from timeseries

    def count(self, ids: Sequence[ExternalId]) -> int:
        datapoints = self.client.time_series.data.retrieve(
            external_id=[id.external_id for id in ids],
            start=MIN_TIMESTAMP_MS,
            end=MAX_TIMESTAMP_MS + 1,
            aggregates="count",
            granularity="1000d",
            ignore_unknown_ids=True,
        )
        return sum(sum(data.count or []) for data in datapoints)

    def drop_data(self, ids: Sequence[ExternalId]) -> int:
        count = self.count(ids)
        existing = self.client.tool.timeseries.retrieve(list(ids), ignore_unknown_ids=True)
        for ts in existing:
            self.client.time_series.data.delete_range(
                external_id=ts.external_id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS + 1
            )
        return count


@final
class DatapointSubscriptionCRUD(
    ResourceCRUD[
        ExternalId,
        DatapointSubscriptionRequest,
        DatapointSubscriptionResponse,
    ]
):
    folder_name = "timeseries"
    resource_cls = DatapointSubscriptionResponse
    resource_write_cls = DatapointSubscriptionRequest
    kind = "DatapointSubscription"
    _doc_url = "Data-point-subscriptions/operation/postSubscriptions"
    dependencies = frozenset({TimeSeriesCRUD, GroupAllScopedCRUD})
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
    def get_id(cls, item: DatapointSubscriptionRequest | DatapointSubscriptionResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])
        for timeseries_id in item.get("timeSeriesIds", []):
            yield TimeSeriesCRUD, ExternalId(external_id=timeseries_id)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DatapointSubscriptionRequest] | None, read_only: bool
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

    def create(self, items: Sequence[DatapointSubscriptionRequest]) -> list[DatapointSubscriptionResponse]:
        created_list = []
        for item in items:
            to_create, update_batches = self.create_split_timeseries_ids(item)
            created = self.client.tool.datapoint_subscriptions.create([to_create])
            if update_batches:
                created = self.client.tool.datapoint_subscriptions.update(update_batches)
            if created:
                # The last batch contains all the time series IDs, so it represents the fully created subscription.
                created_list.append(created[-1])
        return created_list

    def retrieve(self, ids: Sequence[ExternalId]) -> list[DatapointSubscriptionResponse]:
        return self.client.tool.datapoint_subscriptions.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DatapointSubscriptionRequest]) -> list[DatapointSubscriptionResponse]:
        updated_list = []
        for item in items:
            current = self.client.tool.datapoint_subscriptions.list_members(item.external_id, limit=None)
            to_update, update_batches = self.update_split_timeseries_ids(item, current)
            # There are two versions of a TimeSeries Subscription, one selects timeseries based filter
            # and the other selects timeseries based on timeSeriesIds.
            first_update = to_update.as_update()
            updated: list[DatapointSubscriptionResponse] = []
            if first_update.has_data():
                updated = self.client.tool.datapoint_subscriptions.update([to_update.as_update()])
            if update_batches:
                updated = self.client.tool.datapoint_subscriptions.update(update_batches)
            # The last batch contains all the time series IDs, so it represents the fully updated subscription.
            if updated:
                updated_list.append(updated[-1])
            else:
                updated_list.extend(self.client.tool.datapoint_subscriptions.retrieve([item.as_id()]))
        return updated_list

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.datapoint_subscriptions.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DatapointSubscriptionResponse]:
        for subscriptions in self.client.tool.datapoint_subscriptions.iterate(limit=None):
            yield from subscriptions

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

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> DatapointSubscriptionRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return DatapointSubscriptionRequest.model_validate(resource)

    def dump_resource(
        self, resource: DatapointSubscriptionResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        # timeSeriesIds and instanceIds are not returned in the response, so we need to add them
        # to the dumped resource if they are set in the local resource. If there is a discrepancy between
        # the local and dumped resource, the hash added to the description will change.
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
        cls, subscription: DatapointSubscriptionRequest
    ) -> tuple[DatapointSubscriptionRequest, list[DatapointSubscriptionUpdateRequest]]:
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
        to_create = subscription.model_copy(deep=True)
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
        batches: list[DatapointSubscriptionUpdateRequest] = []
        for chunk in chunker(all_remaining_ids, cls._TIMESERIES_ID_REQUEST_LIMIT):
            ts_ids_in_chunk, instance_ids_in_chunk = cls._split_ts_instance_ids(chunk)
            data: dict[str, Any] = {}
            if ts_ids_in_chunk:
                data["time_series_ids"] = AddRemove(add=ts_ids_in_chunk)
            if instance_ids_in_chunk:
                data["instance_ids"] = AddRemove(add=instance_ids_in_chunk)
            batches.append(
                DatapointSubscriptionUpdateRequest(
                    external_id=subscription.external_id, update=DataPointSubscriptionUpdate(**data)
                )
            )
        return to_create, batches

    @classmethod
    def _validate_total_below_limit(cls, subscription: DatapointSubscriptionRequest, total_timeseries: int) -> None:
        if total_timeseries > cls._MAX_TIMESERIES_IDS:
            raise ToolkitValueError(
                f'Subscription "{subscription.external_id}" has {total_timeseries:,} time series, '
                f"which is more than the limit of {cls._MAX_TIMESERIES_IDS:,}."
            )

    @classmethod
    def _split_ts_instance_ids(
        cls, ids: list[tuple[Literal["ts"], str] | tuple[Literal["instance"], NodeReference]]
    ) -> tuple[list[str], list[NodeReference]]:
        ts_ids, instance_ids = [], []
        for id_type, identifier in ids:
            if id_type == "ts":
                ts_ids.append(identifier)
            else:
                instance_ids.append(identifier)
        return ts_ids, instance_ids

    @classmethod
    def update_split_timeseries_ids(
        cls, subscription: DatapointSubscriptionRequest, current_ts: list[DatapointSubscriptionTimeSeriesId]
    ) -> tuple[DatapointSubscriptionRequest, list[DatapointSubscriptionUpdateRequest]]:
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

        to_update = subscription.model_copy(deep=True)

        # Get desired time series IDs
        desired_timeseries_ids = set(to_update.time_series_ids or [])
        desired_instance_ids = set(to_update.instance_ids or [])

        # Get current time series IDs from the subscription
        current_timeseries_ids: set[str] = set()
        current_instance_ids: set[NodeReference] = set()
        for ts in current_ts:
            if ts.external_id and ts.instance_id is None:
                current_timeseries_ids.add(ts.external_id)
            elif ts.instance_id and ts.external_id is None:
                current_instance_ids.add(ts.instance_id)
            else:
                raise ValueError(f"External ID and instance ID are both set for time series {ts.external_id}.")

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
        batches: list[DatapointSubscriptionUpdateRequest] = []
        all_removals = [("ts", id_) for id_ in ts_to_remove] + [("instance", id_) for id_ in instance_to_remove]
        all_additions = [("ts", id_) for id_ in ts_to_add] + [("instance", id_) for id_ in instance_to_add]
        for removals, additions in zip_longest(
            chunker(all_removals, cls._TIMESERIES_ID_REQUEST_LIMIT),
            chunker(all_additions, cls._TIMESERIES_ID_REQUEST_LIMIT),
            fillvalue=None,
        ):
            ts_ids_to_remove, instance_ids_to_remove = cls._split_ts_instance_ids(removals or [])
            args: dict[str, Any] = {}
            if ts_ids_to_remove:
                args["time_series_ids"] = AddRemove(remove=ts_ids_to_remove)
            if instance_ids_to_remove:
                args["instance_ids"] = AddRemove(remove=instance_ids_to_remove)

            ts_ids_to_add, instance_ids_to_add = cls._split_ts_instance_ids(additions or [])
            if ts_ids_to_add:
                if "time_series_ids" in args:
                    args["time_series_ids"].add = ts_ids_to_add
                else:
                    args["time_series_ids"] = AddRemove(add=ts_ids_to_add)
            if instance_ids_to_add:
                if "instance_ids" in args:
                    args["instance_ids"].add = instance_ids_to_add
                else:
                    args["instance_ids"] = AddRemove(add=instance_ids_to_add)
            batches.append(
                DatapointSubscriptionUpdateRequest(
                    external_id=subscription.external_id,
                    update=DataPointSubscriptionUpdate(**args),
                )
            )

        return to_update, batches
