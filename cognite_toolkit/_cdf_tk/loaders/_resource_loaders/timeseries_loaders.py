from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from typing import Any, cast, final

from cognite.client.data_classes import (
    DatapointsList,
    DatapointSubscription,
    DatapointSubscriptionList,
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
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.constants import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceContainerLoader, ResourceLoader
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, dm_identifier

from .auth_loaders import GroupAllScopedLoader, SecurityCategoryLoader
from .classic_loaders import AssetLoader
from .data_organization_loaders import DataSetsLoader


@final
class TimeSeriesLoader(ResourceContainerLoader[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    item_name = "datapoints"
    folder_name = "timeseries"
    filename_pattern = r"^(?!.*DatapointSubscription$).*"
    resource_cls = TimeSeries
    resource_write_cls = TimeSeriesWrite
    list_cls = TimeSeriesList
    list_write_cls = TimeSeriesWriteList
    kind = "TimeSeries"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader, AssetLoader})
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

        return TimeSeriesAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

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
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "securityCategoryNames" in item:
            for security_category in item["securityCategoryNames"]:
                yield SecurityCategoryLoader, security_category
        if "assetExternalId" in item:
            yield AssetLoader, item["assetExternalId"]

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
        datapoints = cast(
            DatapointsList,
            self.client.time_series.data.retrieve(
                external_id=cast(SequenceNotStr[str], ids),
                start=MIN_TIMESTAMP_MS,
                end=MAX_TIMESTAMP_MS + 1,
                aggregates="count",
                granularity="1000d",
                ignore_unknown_ids=True,
            ),
        )
        return sum(sum(data.count or []) for data in datapoints)

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
class DatapointSubscriptionLoader(
    ResourceLoader[
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
            TimeSeriesLoader,
            GroupAllScopedLoader,
        }
    )

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
                object.__setattr__(item, "path", item.path[:length] + (ANY_STR,) + item.path[length:])
        spec.add(ParameterSpec(("filter", ANYTHING), frozenset({"dict"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        for timeseries_id in item.get("timeSeriesIds", []):
            yield TimeSeriesLoader, timeseries_id

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

        return TimeSeriesSubscriptionsAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        created = DatapointSubscriptionList([])
        for item in items:
            created.append(self.client.time_series.subscriptions.create(item))
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> DatapointSubscriptionList:
        items = DatapointSubscriptionList([])
        for id_ in ids:
            retrieved = self.client.time_series.subscriptions.retrieve(id_)
            if retrieved:
                items.append(retrieved)
        return items

    def update(self, items: DatapointSubscriptionWriteList) -> DatapointSubscriptionList:
        updated = DatapointSubscriptionList([])
        for item in items:
            # There are two versions of a TimeSeries Subscription, one selects timeseries based filter
            # and the other selects timeseries based on timeSeriesIds. If we use mode='replace', we try
            # to set timeSeriesIds to an empty list, while the filter is set. This will result in an error.
            update = self.client.time_series.subscriptions.update(item, mode="replace_ignore_null")
            updated.append(update)

        return updated

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

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> DataPointSubscriptionWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return DataPointSubscriptionWrite._load(resource)

    def dump_resource(self, resource: DatapointSubscription, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if "timeSeriesIds" not in dumped:
            return dumped
        # Sorting the timeSeriesIds in the local order
        # Sorting in the same order as the local file.
        ts_order_by_id = {ts_id: no for no, ts_id in enumerate(local.get("timeSeriesIds", []))}
        end_of_list = len(ts_order_by_id)
        dumped["timeSeriesIds"] = sorted(
            dumped["timeSeriesIds"], key=lambda ts_id: ts_order_by_id.get(ts_id, end_of_list)
        )
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "filter" or json_path == ("timeSeriesIds",):
            return diff_list_hashable(local, cdf)
        elif json_path == ("instanceIds",):
            return diff_list_identifiable(local, cdf, get_identifier=dm_identifier)
        return super().diff_list(local, cdf, json_path)
