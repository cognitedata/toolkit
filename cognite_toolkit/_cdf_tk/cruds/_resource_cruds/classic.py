import collections.abc
import io
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, final

import pandas as pd
from cognite.client.data_classes import Sequence as CDFSequence
from cognite.client.data_classes import (
    SequenceList,
    SequenceWrite,
    capabilities,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.asset import (
    AssetAggregateItem,
    AssetRequest,
    AssetResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.sequences import (
    ToolkitSequenceRows,
    ToolkitSequenceRowsList,
    ToolkitSequenceRowsWrite,
)
from cognite_toolkit._cdf_tk.constants import TABLE_FORMATS, YAML_SUFFIX
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_classes import AssetYAML, EventYAML, SequenceRowYAML, SequenceYAML
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning, ToolkitDeprecationWarning
from cognite_toolkit._cdf_tk.utils import load_yaml_inject_variables
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable
from cognite_toolkit._cdf_tk.utils.file import read_csv

from .data_organization import DataSetsCRUD, LabelCRUD

_DEPRECATION_WARNING_ISSUED = False


@final
class AssetCRUD(ResourceCRUD[ExternalId, AssetRequest, AssetResponse]):
    folder_name = "classic"
    resource_cls = AssetResponse
    resource_write_cls = AssetRequest
    yaml_cls = AssetYAML
    kind = "Asset"
    dependencies = frozenset({DataSetsCRUD, LabelCRUD})
    _doc_url = "Assets/operation/createAssets"

    @classmethod
    def is_supported_file(cls, file: Path) -> bool:
        if Flags.v08.is_enabled():
            return super().is_supported_file(file)
        global _DEPRECATION_WARNING_ISSUED
        if not file.stem.casefold().endswith(cls.kind.casefold()):
            return False
        if file.suffix in YAML_SUFFIX:
            return True
        if file.suffix in TABLE_FORMATS:
            if not _DEPRECATION_WARNING_ISSUED:
                ToolkitDeprecationWarning(
                    feature="deployment of asset from CSV or Parquet files",
                    alternative="data plugin and cdf data upload commands",
                    removal_version="0.8",
                ).print_warning()
                _DEPRECATION_WARNING_ISSUED = True
            return True
        return False

    @property
    def display_name(self) -> str:
        return "assets"

    @classmethod
    def get_id(cls, item: AssetRequest | AssetResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("Asset must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: AssetResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("Asset must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: collections.abc.Sequence[AssetRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
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

        return capabilities.AssetsAcl(actions, scope)

    def create(self, items: collections.abc.Sequence[AssetRequest]) -> list[AssetResponse]:
        return self.client.tool.assets.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[AssetResponse]:
        return self.client.tool.assets.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: collections.abc.Sequence[AssetRequest]) -> list[AssetResponse]:
        return self.client.tool.assets.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.assets.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[AssetResponse]:
        filter_ = ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=data_set_external_id)
        for assets in self.client.tool.assets.iterate(aggregated_properties=True, filter=filter_, limit=None):
            yield from assets

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        for label in item.get("labels", []):
            if isinstance(label, dict):
                yield LabelCRUD, label["externalId"]
            elif isinstance(label, str):
                yield LabelCRUD, label
        if "parentExternalId" in item:
            yield cls, item["parentExternalId"]

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]]
        if filepath.suffix in {".yaml", ".yml"}:
            raw_yaml = load_yaml_inject_variables(
                self.safe_read(filepath),
                environment_variables or {},
                original_filepath=filepath,
            )
            resources = [raw_yaml] if isinstance(raw_yaml, dict) else raw_yaml
        elif filepath.suffix == ".csv" or filepath.suffix == ".parquet":
            if filepath.suffix == ".csv":
                # The replacement is used to ensure that we read exactly the same file on Windows and Linux
                file_content = filepath.read_bytes().replace(b"\r\n", b"\n").decode("utf-8")
                data = read_csv(io.StringIO(file_content))
            else:
                data = pd.read_parquet(filepath)
            data.replace(pd.NA, None, inplace=True)
            data.replace("", None, inplace=True)
            resources = data.to_dict(orient="records")
        else:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")

        return resources

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> AssetRequest:
        # Unpack metadata keys from table formats (e.g. csv, parquet)
        metadata: dict = resource.get("metadata", {})
        for key, value in list(resource.items()):
            if key.startswith("metadata."):
                if value not in {None, float("nan")} and str(value) not in {"", " ", "nan", "null", "none"}:
                    metadata[key.removeprefix("metadata.")] = str(value)
                del resource[key]
        if metadata:
            resource["metadata"] = metadata
        if isinstance(resource.get("labels"), str):
            resource["labels"] = [
                label.strip() for label in resource["labels"].removeprefix("[").removesuffix("]").split(",")
            ]

        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return AssetRequest.model_validate(resource)

    def dump_resource(self, resource: AssetResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if not dumped.get("metadata") and "metadata" not in local:
            dumped.pop("metadata", None)
        if "parentId" in dumped and "parentId" not in local:
            dumped.pop("parentId")
        if resource.aggregates:
            # This is only included when the asset is downloaded/migrated with aggregated properties
            aggregates = (
                resource.aggregates.dump()
                if isinstance(resource.aggregates, AssetAggregateItem)
                else resource.aggregates
            )
            if "path" in aggregates:
                path = aggregates.pop("path", [])
                if path:
                    aggregates["path"] = self.client.lookup.assets.external_id(
                        [segment["id"] for segment in path if "id" in segment]
                    )
            dumped.update(aggregates)
        return dumped


@final
class SequenceCRUD(ResourceCRUD[str, SequenceWrite, CDFSequence]):
    folder_name = "classic"
    resource_cls = CDFSequence
    resource_write_cls = SequenceWrite
    kind = "Sequence"
    dependencies = frozenset({DataSetsCRUD, AssetCRUD})
    yaml_cls = SequenceYAML
    _doc_url = "Sequences/operation/createSequence"

    @property
    def display_name(self) -> str:
        return "sequences"

    @classmethod
    def get_id(cls, item: CDFSequence | SequenceWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Sequence must have external_id")
        return item.external_id

    @classmethod
    def get_internal_id(cls, item: CDFSequence | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("Sequence must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SequenceWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
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

        return capabilities.SequencesAcl(actions, scope)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SequenceWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if asset_external_id := resource.pop("assetExternalId", None):
            resource["assetId"] = self.client.lookup.assets.id(asset_external_id, is_dry_run)
        return SequenceWrite._load(resource)

    def dump_resource(self, resource: CDFSequence, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if asset_id := dumped.pop("assetId", None):
            dumped["assetExternalId"] = self.client.lookup.assets.external_id(asset_id)
        if not dumped.get("metadata") and "metadata" not in local:
            dumped.pop("metadata", None)
        local_col_by_id = {col["externalId"]: col for col in local.get("columns", []) if "externalId" in col}
        for col in dumped.get("columns", []):
            external_id = col.get("externalId")
            if not external_id:
                continue
            if external_id not in local_col_by_id:
                continue
            local_col = local_col_by_id[external_id]
            if not col.get("metadata") and "metadata" not in local_col:
                col.pop("metadata", None)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path != ("columns",):
            return super().diff_list(local, cdf, json_path)
        return diff_list_identifiable(local, cdf, get_identifier=lambda col: col["externalId"])

    def create(self, items: Sequence[SequenceWrite]) -> SequenceList:
        return self.client.sequences.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SequenceList:
        return self.client.sequences.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: Sequence[SequenceWrite]) -> SequenceList:
        return self.client.sequences.upsert(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str | int]) -> int:
        internal_ids, external_ids = self._split_ids(ids)
        try:
            self.client.sequences.delete(id=internal_ids, external_id=external_ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                internal_ids, external_ids = self._split_ids(existing)
                self.client.sequences.delete(id=internal_ids, external_id=external_ids)
            return len(existing)
        else:
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[CDFSequence]:
        return iter(
            self.client.sequences(data_set_external_ids=[data_set_external_id] if data_set_external_id else None)
        )

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        if "assetExternalId" in item:
            yield AssetCRUD, ExternalId(external_id=item["assetExternalId"])


@final
class SequenceRowCRUD(ResourceCRUD[str, ToolkitSequenceRowsWrite, ToolkitSequenceRows]):
    folder_name = "classic"
    resource_cls = ToolkitSequenceRows
    resource_write_cls = ToolkitSequenceRowsWrite
    kind = "SequenceRow"
    dependencies = frozenset({SequenceCRUD})
    parent_resource = frozenset({SequenceCRUD})
    _doc_url = "Sequences/operation/postSequenceData"
    yaml_cls = SequenceRowYAML
    support_update = False

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None):
        super().__init__(client, build_dir, console)
        # Used in the .diff_list method to keep track of the last column in the local list
        # such that the values in the rows can be matched to the correct column.
        self._last_column: tuple[dict[int, int], list[int]] = {}, []

    @property
    def display_name(self) -> str:
        return "sequence rows"

    @classmethod
    def get_id(cls, item: ToolkitSequenceRows | ToolkitSequenceRowsWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("SequenceRows must have external_id")
        return item.external_id

    @classmethod
    def get_internal_id(cls, item: ToolkitSequenceRows | ToolkitSequenceRowsWrite | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("SequenceRows must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: collections.abc.Sequence[ToolkitSequenceRowsWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # We don't have any capabilities for SequenceRows, that is already handled by the Sequence
        return []

    def create(self, items: Sequence[ToolkitSequenceRowsWrite]) -> Sequence[ToolkitSequenceRowsWrite]:
        for item in items:
            self.client.sequences.rows.insert(item.as_sequence_rows(), external_id=item.external_id)
        return items

    def retrieve(self, ids: SequenceNotStr[str]) -> ToolkitSequenceRowsList:
        retrieved = self.client.sequences.rows.retrieve(external_id=ids)
        return ToolkitSequenceRowsList([ToolkitSequenceRows._load(row.dump(camel_case=True)) for row in retrieved])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        for id_ in ids:
            self.client.sequences.rows.delete_range(start=0, end=None, external_id=id_)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ToolkitSequenceRows]:
        if parent_ids is None:
            sequence_iterable = self.client.sequences(
                data_set_external_ids=[data_set_external_id] if data_set_external_id else None
            )
            parent_ids = [seq.external_id or seq.id for seq in sequence_iterable]
        for sequence_id in parent_ids:
            if isinstance(sequence_id, str):
                res = self.client.sequences.rows.retrieve(external_id=sequence_id)
                if res:
                    yield ToolkitSequenceRows._load(res.dump(camel_case=True))
            elif isinstance(sequence_id, int):
                res = self.client.sequences.rows.retrieve(id=sequence_id)
                if res:
                    yield ToolkitSequenceRows._load(res.dump(camel_case=True))

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        yield SequenceCRUD, item["externalId"]

    def dump_resource(self, resource: ToolkitSequenceRows, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()

        if local is not None and "id" in dumped and "id" not in local:
            dumped.pop("id")
        # Ensure that the rows is the last key in the dumped dictionary,
        # This information is used in the .diff_list method to match the values in the rows to the correct column.
        dumped["rows"] = dumped.pop("rows")
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("rows",):
            return diff_list_identifiable(local, cdf, get_identifier=lambda row: row["rowNumber"])
        elif json_path == ("columns",):
            self._last_column = diff_list_hashable(local, cdf)
            # This is a special case where we need to keep track of the last column in the local list
            # such that the values in the rows can be matched to the correct column.
            return self._last_column
        elif len(json_path) == 3 and json_path[0] == "rows" and json_path[2] == "values":
            local_by_cdf, added = self._last_column
            if len(cdf) == len(local_by_cdf) + len(added):
                return local_by_cdf, added
            else:
                LowSeverityWarning("Number of rows in does not match the number of columns").print_warning()
                # Just assume that the rows are in the correct order
                return {no: no for no in range(len(cdf))}, []
        return super().diff_list(local, cdf, json_path)


@final
class EventCRUD(ResourceCRUD[ExternalId, EventRequest, EventResponse]):
    folder_name = "classic"
    resource_cls = EventResponse
    resource_write_cls = EventRequest
    yaml_cls = EventYAML
    kind = "Event"
    dependencies = frozenset({DataSetsCRUD, AssetCRUD})
    _doc_url = "Events/operation/createEvents"

    @property
    def display_name(self) -> str:
        return "events"

    @classmethod
    def get_id(cls, item: EventRequest | EventResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("Event must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: EventResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("Event must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: collections.abc.Sequence[EventRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
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

        return capabilities.EventsAcl(actions, scope)

    def create(self, items: collections.abc.Sequence[EventRequest]) -> list[EventResponse]:
        return self.client.tool.events.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[EventResponse]:
        return self.client.tool.events.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: collections.abc.Sequence[EventRequest]) -> list[EventResponse]:
        return self.client.tool.events.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.events.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[EventResponse]:
        filter_ = ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=data_set_external_id)
        for events in self.client.tool.events.iterate(filter=filter_, limit=None):
            yield from events

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        for asset_id in item.get("assetExternalIds", []):
            if isinstance(asset_id, str):
                yield AssetCRUD, ExternalId(external_id=asset_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> EventRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if asset_external_ids := resource.pop("assetExternalIds", []):
            resource["assetIds"] = self.client.lookup.assets.id(asset_external_ids, is_dry_run)
        return EventRequest.model_validate(resource)

    def dump_resource(self, resource: EventResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if asset_ids := dumped.pop("assetIds", None):
            local_order = {asset: no for no, asset in enumerate(local.get("assetExternalIds", []))}
            end_of_list = len(local_order)
            dumped["assetExternalIds"] = sorted(
                self.client.lookup.assets.external_id(asset_ids), key=lambda a: local_order.get(a, end_of_list)
            )
        if not dumped.get("metadata") and "metadata" not in local:
            dumped.pop("metadata", None)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path != ("assetExternalIds",):
            return super().diff_list(local, cdf, json_path)
        return diff_list_hashable(local, cdf)
