from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import overload

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.utils.useful_types import SequenceNotStr


class LookUpAPI(APIClient, ABC):
    dry_run: int = -1

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._cache: dict[str, int] = {}
        self._reverse_cache: dict[int, str] = {}

    @overload
    def id(self, external_id: str) -> int: ...

    @overload
    def id(self, external_id: SequenceNotStr[str]) -> list[int]: ...

    def id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        return self._id(external_id)

    @abstractmethod
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        raise NotImplementedError

    @overload
    def external_id(self, id: int) -> str: ...

    @overload
    def external_id(self, id: Sequence[int]) -> list[str]: ...

    def external_id(self, id: int | Sequence[int]) -> str | list[str]:
        return self._external_id(id)

    @abstractmethod
    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        raise NotImplementedError


class DataSetLookUpAPI(LookUpAPI):
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        return [
            data_set.id
            for data_set in self._cognite_client.data_sets.retrieve_multiple(
                external_id=external_id, ignore_unknown_ids=True
            )
        ]

    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        return [
            data_set.external_id
            for data_set in self._cognite_client.data_sets.retrieve_multiple(id=id, ignore_unknown_ids=True)
        ]


class AssetLookUpAPI(LookUpAPI):
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        return [
            asset.id
            for asset in self._cognite_client.assets.retrieve_multiple(external_id=external_id, ignore_unknown_ids=True)
        ]

    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        return [
            asset.external_id for asset in self._cognite_client.assets.retrieve_multiple(id=id, ignore_unknown_ids=True)
        ]


class TimeSeriesLookUpAPI(LookUpAPI):
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        return [
            time_series.id
            for time_series in self._cognite_client.time_series.retrieve_multiple(
                external_id=external_id, ignore_unknown_ids=True
            )
        ]

    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        return [
            time_series.external_id
            for time_series in self._cognite_client.time_series.retrieve_multiple(id=id, ignore_unknown_ids=True)
        ]


class SecurityCategoriesLookUpAPI(LookUpAPI):
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        raise NotImplementedError

    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        raise NotImplementedError


class LocationFiltersLookUpAPI(LookUpAPI):
    def _id(self, external_id: str | SequenceNotStr[str]) -> int | list[int]:
        raise NotImplementedError

    def _external_id(self, id: int | Sequence[int]) -> str | list[str]:
        raise NotImplementedError


class LookUpGroup(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data_sets = DataSetLookUpAPI(config, api_version, cognite_client)
        self.assets = AssetLookUpAPI(config, api_version, cognite_client)
        self.time_series = TimeSeriesLookUpAPI(config, api_version, cognite_client)
        self.security_categories = SecurityCategoriesLookUpAPI(config, api_version, cognite_client)
        self.location_filters = LocationFiltersLookUpAPI(config, api_version, cognite_client)
