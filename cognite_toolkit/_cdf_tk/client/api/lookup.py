from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import ClientConfig
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.api_client import ToolkitAPI
from cognite_toolkit._cdf_tk.exceptions import ResourceRetrievalError

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient


class LookUpAPI(ToolkitAPI, ABC):
    dry_run: int = -1

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: ToolkitClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._cache: dict[str, int] = {}
        self._reverse_cache: dict[int, str] = {}

    @property
    def resource_name(self) -> str:
        return type(self).__name__.removesuffix("LookUpAPI")

    @overload
    def id(self, external_id: str) -> int: ...

    @overload
    def id(self, external_id: SequenceNotStr[str]) -> list[int]: ...

    def id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        ids = [external_id] if isinstance(external_id, str) else external_id
        missing = [id for id in ids if id not in self._cache]
        if missing:
            lookup = self._id(missing)
            self._cache.update(lookup)
            self._reverse_cache.update({v: k for k, v in lookup.items()})
            if len(missing) != len(lookup):
                raise ResourceRetrievalError(f"Failed to retrieve {self.resource_name} with external_id {missing}")
        return self._cache[external_id] if isinstance(external_id, str) else [self._cache[id] for id in ids]

    @overload
    def external_id(self, id: int) -> str: ...

    @overload
    def external_id(self, id: Sequence[int]) -> list[str]: ...

    def external_id(self, id: int | Sequence[int]) -> str | list[str]:
        ids = [id] if isinstance(id, int) else id
        missing = [id_ for id_ in ids if id not in self._reverse_cache]
        if missing:
            lookup = self._external_id(missing)
            self._reverse_cache.update(lookup)
            self._cache.update({v: k for k, v in lookup.items()})
            if len(missing) != len(lookup):
                raise ResourceRetrievalError(f"Failed to retrieve {self.resource_name} with id {missing}")
        return self._reverse_cache[id] if isinstance(id, int) else [self._reverse_cache[id] for id in ids]

    @abstractmethod
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
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


class AllLookUpAPI(LookUpAPI, ABC):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: ToolkitClient) -> None:
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

    def name(self, id: int | Sequence[int]) -> str | list[str]:
        return self.external_id(id)


class LocationFiltersLookUpAPI(AllLookUpAPI):
    def _lookup(self) -> None:
        location_filters = self._toolkit_client.location_filters.list()
        self._cache = {location_filter.name: location_filter.id for location_filter in location_filters}
        self._reverse_cache = {location_filter.id: location_filter.name for location_filter in location_filters}


class LookUpGroup(ToolkitAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: ToolkitClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data_sets = DataSetLookUpAPI(config, api_version, cognite_client)
        self.assets = AssetLookUpAPI(config, api_version, cognite_client)
        self.time_series = TimeSeriesLookUpAPI(config, api_version, cognite_client)
        self.security_categories = SecurityCategoriesLookUpAPI(config, api_version, cognite_client)
        self.location_filters = LocationFiltersLookUpAPI(config, api_version, cognite_client)
