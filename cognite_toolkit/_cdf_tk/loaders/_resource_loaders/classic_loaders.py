from __future__ import annotations

import io
from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, final

import pandas as pd
from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWrite,
    AssetWriteList,
    Event,
    EventList,
    EventWrite,
    EventWriteList,
    Sequence,
    SequenceList,
    SequenceWrite,
    SequenceWriteList,
    capabilities,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from .data_organization_loaders import DataSetsLoader, LabelLoader


@final
class AssetLoader(ResourceLoader[str, AssetWrite, Asset, AssetWriteList, AssetList]):
    folder_name = "classic"
    filename_pattern = r"^.*\.Asset$"  # Matches all yaml files whose stem ends with '.Asset'.
    filetypes = frozenset({"yaml", "yml", "csv", "parquet"})
    resource_cls = Asset
    resource_write_cls = AssetWrite
    list_cls = AssetList
    list_write_cls = AssetWriteList
    kind = "Asset"
    dependencies = frozenset({DataSetsLoader, LabelLoader})
    _doc_url = "Assets/operation/createAssets"

    @property
    def display_name(self) -> str:
        return self.kind

    @classmethod
    def get_id(cls, item: Asset | AssetWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Asset must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: AssetWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.AssetsAcl.Scope.All | capabilities.AssetsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.AssetsAcl.Scope.All()
        )

        actions = (
            [capabilities.AssetsAcl.Action.Read]
            if read_only
            else [capabilities.AssetsAcl.Action.Read, capabilities.AssetsAcl.Action.Write]
        )

        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.AssetsAcl.Scope.DataSet(list(data_set_ids))

        return capabilities.AssetsAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: AssetWriteList) -> AssetList:
        return self.client.assets.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> AssetList:
        return self.client.assets.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: AssetWriteList) -> AssetList:
        return self.client.assets.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.assets.delete(external_id=ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.assets.delete(external_id=existing)
            return len(existing)
        else:
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Asset]:
        return iter(self.client.assets(data_set_external_ids=[data_set_external_id] if data_set_external_id else None))

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))

        # Failed to be inferred from the AssetWrite.__init__ method.
        spec.add(
            ParameterSpec(("labels", ANY_INT, "externalId"), frozenset({"str"}), is_required=True, _is_nullable=True)
        )

        # Should not be used, used for parentExternalId instead
        spec.discard(ParameterSpec(("parentId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        for label in item.get("labels", []):
            if isinstance(label, dict):
                yield LabelLoader, label["externalId"]
            elif isinstance(label, str):
                yield LabelLoader, label
        if "parentExternalId" in item:
            yield cls, item["parentExternalId"]

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> AssetWriteList:
        resources: list[dict[str, Any]]
        if filepath.suffix in {".yaml", ".yml"}:
            use_environment_variables = (
                ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
            )
            raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

            resources = [raw_yaml] if isinstance(raw_yaml, dict) else raw_yaml
        elif filepath.suffix == ".csv" or filepath.suffix == ".parquet":
            if filepath.suffix == ".csv":
                # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                file_content = filepath.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                data = pd.read_csv(io.StringIO(file_content))
            else:
                data = pd.read_parquet(filepath)
            data.replace(pd.NA, None, inplace=True)
            data.replace("", None, inplace=True)
            resources = data.to_dict(orient="records")
        else:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")

        for resource in resources:
            # Unpack metadata keys from table formats (e.g. csv, parquet)
            metadata: dict = resource.get("metadata", {})
            for key, value in list(resource.items()):
                if key.startswith("metadata."):
                    if value not in {None, float("nan")} and str(value) not in {"", " ", "nan", "null", "none"}:
                        metadata[key.removeprefix("metadata.")] = str(value)
                    del resource[key]
            if metadata:
                resource["metadata"] = metadata
            if "labels" in resource and isinstance(resource["labels"], str):
                resource["labels"] = [
                    label.strip() for label in resource["labels"].removeprefix("[").removesuffix("]").split(",")
                ]

            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in assets"
                )
        return AssetWriteList.load(resources)

    def _are_equal(
        self, local: AssetWrite, cdf_resource: Asset, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Dry run
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        if (
            all(s == -1 for s in local_dumped.get("securityCategories", []))
            and "securityCategories" in cdf_dumped
            and len(cdf_dumped["securityCategories"]) == len(local_dumped.get("securityCategories", []))
        ):
            local_dumped["securityCategories"] = cdf_dumped["securityCategories"]

        # Remove metadata if it is empty to avoid false negatives
        # as a result of cdf_resource.metadata = {} != local.metadata = None
        if not local_dumped.get("metadata"):
            local_dumped.pop("metadata", None)
        if not cdf_dumped.get("metadata"):
            cdf_dumped.pop("metadata", None)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)


@final
class SequenceLoader(ResourceLoader[str, SequenceWrite, Sequence, SequenceWriteList, SequenceList]):
    folder_name = "classic"
    filename_pattern = r"^.*\.Sequence$"
    resource_cls = Sequence
    resource_write_cls = SequenceWrite
    list_cls = SequenceList
    list_write_cls = SequenceWriteList
    kind = "Sequence"
    dependencies = frozenset({DataSetsLoader, AssetLoader})
    _doc_url = "Sequences/operation/createSequence"

    @property
    def display_name(self) -> str:
        return self.kind

    @classmethod
    def get_id(cls, item: Sequence | SequenceWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Sequence must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: SequenceWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: Any = capabilities.SequencesAcl.Scope.All()
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.SequencesAcl.Scope.DataSet(list(data_set_ids))

        actions = (
            [capabilities.SequencesAcl.Action.Read]
            if read_only
            else [capabilities.SequencesAcl.Action.Read, capabilities.SequencesAcl.Action.Write]
        )

        return capabilities.SequencesAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: SequenceWriteList) -> SequenceList:
        return self.client.sequences.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SequenceList:
        return self.client.sequences.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: SequenceWriteList) -> SequenceList:
        return self.client.sequences.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.sequences.delete(external_id=ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.sequences.delete(external_id=existing)
            return len(existing)
        else:
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Sequence]:
        return iter(
            self.client.sequences(data_set_external_ids=[data_set_external_id] if data_set_external_id else None)
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        spec.add(ParameterSpec(("assetExternalId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "assetExternalId" in item:
            yield AssetLoader, item["assetExternalId"]

    def _are_equal(
        self, local: SequenceWrite, cdf_resource: Sequence, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Dry run
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]

        # Remove metadata if it is empty to avoid false negatives
        # as a result of cdf_resource.metadata = {} != local.metadata = None
        if not local_dumped.get("metadata"):
            local_dumped.pop("metadata", None)
        if not cdf_dumped.get("metadata"):
            cdf_dumped.pop("metadata", None)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)


@final
class EventLoader(ResourceLoader[str, EventWrite, Event, EventWriteList, EventList]):
    folder_name = "classic"
    filename_pattern = r"^.*\.Event$"  # Matches all yaml files whose stem ends with '.Event'.
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = Event
    resource_write_cls = EventWrite
    list_cls = EventList
    list_write_cls = EventWriteList
    kind = "Event"
    dependencies = frozenset({DataSetsLoader, AssetLoader})
    _doc_url = "Events/operation/createEvents"

    @property
    def display_name(self) -> str:
        return self.kind

    @classmethod
    def get_id(cls, item: Event | EventWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Event must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: EventWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.EventsAcl.Scope.All | capabilities.EventsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.EventsAcl.Scope.All()
        )

        actions = (
            [capabilities.EventsAcl.Action.Read]
            if read_only
            else [capabilities.EventsAcl.Action.Read, capabilities.EventsAcl.Action.Write]
        )

        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.EventsAcl.Scope.DataSet(list(data_set_ids))

        return capabilities.EventsAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: EventWriteList) -> EventList:
        return self.client.events.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> EventList:
        return self.client.events.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: EventWriteList) -> EventList:
        return self.client.events.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.events.delete(external_id=ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.events.delete(external_id=existing)
            return len(existing)
        else:
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Event]:
        return iter(self.client.events(data_set_external_ids=[data_set_external_id] if data_set_external_id else None))

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))

        spec.add(ParameterSpec(("assetExternalIds",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.add(
            ParameterSpec(
                ("assetExternalIds", ANY_INT, "externalId"), frozenset({"str"}), is_required=False, _is_nullable=False
            )
        )
        spec.discard(ParameterSpec(("assetIds",), frozenset({"int"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("assetIds", ANY_INT), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        for asset_id in item.get("assetExternalIds", []):
            if isinstance(asset_id, str):
                yield AssetLoader, asset_id

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> EventWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

        resources: list[dict[str, Any]] = [raw_yaml] if isinstance(raw_yaml, dict) else raw_yaml

        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in assets"
                )
            if "assetExternalIds" in resource:
                asset_external_ids = resource.pop("assetExternalIds")
                resource["assetIds"] = [
                    ToolGlobals.verify_asset(
                        asset_external_id, skip_validation, action="replace assetExternalIds with assetIds in events"
                    )
                    for asset_external_id in asset_external_ids
                ]
        return EventWriteList._load(resources)

    def _are_equal(
        self, local: EventWrite, cdf_resource: Event, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Dry run
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        if asset_ids := local_dumped.get("assetIds"):
            if (
                all(s == -1 for s in asset_ids)
                and "assetIds" in cdf_dumped
                and len(cdf_dumped["assetIds"]) == len(asset_ids)
            ):
                local_dumped["assetIds"] = cdf_dumped["assetIds"]

        # Remove metadata if it is empty to avoid false negatives
        # as a result of cdf_resource.metadata = {} != local.metadata = None
        if not local_dumped.get("metadata"):
            local_dumped.pop("metadata", None)
        if not cdf_dumped.get("metadata"):
            cdf_dumped.pop("metadata", None)

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)
