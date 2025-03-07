from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import ClientConfig
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetsAcl,
    ExtractionPipelinesAcl,
    LocationFiltersAcl,
    SecurityCategoriesAcl,
    TimeSeriesAcl,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.api_client import ToolkitAPI
from cognite_toolkit._cdf_tk.constants import DRY_RUN_ID
from cognite_toolkit._cdf_tk.exceptions import ResourceRetrievalError
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient


class LookUpAPI(ToolkitAPI, ABC):
    dry_run_id: int = DRY_RUN_ID

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient") -> None:
        super().__init__(config, api_version, cognite_client)
        self._cache: dict[str, int] = {}
        self._reverse_cache: dict[int, str] = {}

    @property
    def resource_name(self) -> str:
        return type(self).__name__.removesuffix("LookUpAPI")

    @overload
    def id(self, external_id: str, is_dry_run: bool = False, allow_empty: bool = False) -> int: ...

    @overload
    def id(
        self, external_id: SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
    ) -> list[int]: ...

    def id(
        self, external_id: str | SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
    ) -> int | list[int]:
        ids = [external_id] if isinstance(external_id, str) else external_id
        missing = [id for id in ids if id not in self._cache]
        if allow_empty and "" in missing:
            # Note we do not want to put empty string in the cache. It is a special case that
            # as of 01/02/2025 only applies to LocationFilters
            missing.remove("")
        if missing:
            try:
                lookup = self._id(missing)
            except CogniteAPIError as e:
                if 400 <= e.code < 500:
                    missing_capabilities = self._toolkit_client.verify.authorization(self._read_acl())
                    if missing_capabilities:
                        raise self._toolkit_client.verify.create_error(
                            missing_capabilities,
                            f"lookup {self.resource_name} with external_id {missing}",
                        )
                # Raise the original error if it's not a 400 or the user has access to read the resource.from
                raise
            self._cache.update(lookup)
            self._reverse_cache.update({v: k for k, v in lookup.items()})
            if len(missing) != len(lookup) and not is_dry_run:
                raise ResourceRetrievalError(
                    f"Failed to retrieve {self.resource_name} with external_id {missing}. Have you created it?"
                )
        return (
            self._get_id_from_cache(external_id, is_dry_run, allow_empty)
            if isinstance(external_id, str)
            else [self._get_id_from_cache(id, is_dry_run, allow_empty) for id in ids]
        )

    def _get_id_from_cache(self, external_id: str, is_dry_run: bool = False, allow_empty: bool = False) -> int:
        if allow_empty and external_id == "":
            return 0
        elif is_dry_run:
            return self._cache.get(external_id, self.dry_run_id)
        else:
            return self._cache[external_id]

    @overload
    def external_id(self, id: int) -> str | None: ...

    @overload
    def external_id(self, id: Sequence[int]) -> list[str]: ...

    def external_id(self, id: int | Sequence[int]) -> str | None | list[str]:
        ids = [id] if isinstance(id, int) else id
        missing = [id_ for id_ in ids if id_ not in self._reverse_cache]
        if 0 in missing:
            missing.remove(0)
        if missing:
            try:
                lookup = self._external_id(missing)
            except CogniteAPIError as e:
                if 400 <= e.code < 500:
                    missing_capabilities = self._toolkit_client.verify.authorization(self._read_acl())
                    if missing_capabilities:
                        raise self._toolkit_client.verify.create_error(
                            missing_capabilities,
                            f"lookup {self.resource_name} with id {missing}",
                        )
                # Raise the original error if it's not a 400 or the user has access to read the resource.from
                raise
            self._reverse_cache.update(lookup)
            self._cache.update({v: k for k, v in lookup.items()})
            if len(missing) != len(lookup):
                MediumSeverityWarning(
                    f"Failed to retrieve {self.resource_name} with id {missing}. It does not exist in CDF"
                ).print_warning()
        if isinstance(id, int):
            return self._get_external_id_from_cache(id)
        else:
            external_ids = (self._get_external_id_from_cache(id) for id in ids)
            return [id for id in external_ids if id is not None]

    def _get_external_id_from_cache(self, id: int) -> str | None:
        if id == 0:
            # Reverse of looking up an empty string.
            return ""
        return self._reverse_cache.get(id)

    @abstractmethod
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        raise NotImplementedError

    @abstractmethod
    def _read_acl(self) -> Capability:
        raise NotImplementedError


class DataSetLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            data_set.external_id: data_set.id
            for data_set in self._cognite_client.data_sets.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if data_set.external_id and data_set.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            data_set.id: data_set.external_id
            for data_set in self._cognite_client.data_sets.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if data_set.external_id and data_set.id
        }

    def _read_acl(self) -> Capability:
        return DataSetsAcl(
            [DataSetsAcl.Action.Read],
            scope=DataSetsAcl.Scope.All(),
        )


class AssetLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            asset.external_id: asset.id
            for asset in self._cognite_client.assets.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if asset.external_id and asset.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            asset.id: asset.external_id
            for asset in self._cognite_client.assets.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if asset.external_id and asset.id
        }

    def _read_acl(self) -> Capability:
        return AssetsAcl(
            [AssetsAcl.Action.Read],
            scope=AssetsAcl.Scope.All(),
        )


class TimeSeriesLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            ts.external_id: ts.id
            for ts in self._cognite_client.time_series.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if ts.external_id and ts.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            ts.id: ts.external_id
            for ts in self._cognite_client.time_series.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if ts.external_id and ts.id
        }

    def _read_acl(self) -> Capability:
        return TimeSeriesAcl(
            [TimeSeriesAcl.Action.Read],
            scope=TimeSeriesAcl.Scope.All(),
        )


class ExtractionPipelineLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            pipeline.external_id: pipeline.id
            for pipeline in self._cognite_client.extraction_pipelines.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if pipeline.external_id and pipeline.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            pipeline.id: pipeline.external_id
            for pipeline in self._cognite_client.extraction_pipelines.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if pipeline.external_id and pipeline.id
        }

    def _read_acl(self) -> Capability:
        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read],
            scope=ExtractionPipelinesAcl.Scope.All(),
        )


class AllLookUpAPI(LookUpAPI, ABC):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient") -> None:
        super().__init__(config, api_version, cognite_client)
        self._has_looked_up = False

    @abstractmethod
    def _lookup(self) -> None:
        raise NotImplementedError

    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        if not self._has_looked_up:
            self._lookup()
        return {external_id: self._cache[external_id] for external_id in external_id}

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        if not self._has_looked_up:
            self._lookup()
            self._has_looked_up = True
        return {id: self._reverse_cache[id] for id in id}


class SecurityCategoriesLookUpAPI(AllLookUpAPI):
    def _lookup(self) -> None:
        categories = self._cognite_client.iam.security_categories.list(limit=-1)
        self._cache = {category.name: category.id for category in categories if category.name and category.id}
        self._reverse_cache = {category.id: category.name for category in categories if category.name and category.id}

    def name(self, id: int | Sequence[int]) -> str | list[str] | None:
        return self.external_id(id)

    def _read_acl(self) -> Capability:
        return SecurityCategoriesAcl(
            [SecurityCategoriesAcl.Action.List],
            scope=SecurityCategoriesAcl.Scope.All(),
        )


class LocationFiltersLookUpAPI(AllLookUpAPI):
    def _lookup(self) -> None:
        location_filters = self._toolkit_client.location_filters.list()
        self._cache = {location_filter.external_id: location_filter.id for location_filter in location_filters}
        self._reverse_cache = {location_filter.id: location_filter.external_id for location_filter in location_filters}

    def _read_acl(self) -> Capability:
        return LocationFiltersAcl(
            [LocationFiltersAcl.Action.Read],
            scope=LocationFiltersAcl.Scope.All(),
        )


class LookUpGroup(ToolkitAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient") -> None:
        super().__init__(config, api_version, cognite_client)
        self.data_sets = DataSetLookUpAPI(config, api_version, cognite_client)
        self.assets = AssetLookUpAPI(config, api_version, cognite_client)
        self.time_series = TimeSeriesLookUpAPI(config, api_version, cognite_client)
        self.security_categories = SecurityCategoriesLookUpAPI(config, api_version, cognite_client)
        self.location_filters = LocationFiltersLookUpAPI(config, api_version, cognite_client)
        self.extraction_pipelines = ExtractionPipelineLookUpAPI(config, api_version, cognite_client)
