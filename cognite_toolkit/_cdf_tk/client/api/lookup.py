from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import ClientConfig
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataSetsAcl,
    EventsAcl,
    ExtractionPipelinesAcl,
    FilesAcl,
    FunctionsAcl,
    LocationFiltersAcl,
    SecurityCategoriesAcl,
    TimeSeriesAcl,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr
from rich.console import Console

from cognite_toolkit._cdf_tk.client.api_client import ToolkitAPI
from cognite_toolkit._cdf_tk.constants import DRY_RUN_ID
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient


class LookUpAPI(ToolkitAPI, ABC):
    dry_run_id: int = DRY_RUN_ID

    def __init__(
        self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient", console: Console
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._console = console
        self._cache: dict[str, int | None] = {}
        self._reverse_cache: dict[int, str | None] = {}

    @property
    def resource_name(self) -> str:
        return type(self).__name__.removesuffix("LookUpAPI")

    @overload
    def id(self, external_id: str, is_dry_run: bool = False, allow_empty: bool = False) -> int | None: ...

    @overload
    def id(
        self, external_id: SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
    ) -> list[int]: ...

    def id(
        self, external_id: str | SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
    ) -> int | None | list[int]:
        """Lookup internal IDs for given external IDs.

        Args:
            external_id: A string external ID or a sequence of string external IDs to look up.
            is_dry_run: If True, return a predefined dry run ID for missing external IDs instead of raising an error.
            allow_empty: If True, allow empty string external IDs and return 0 for them.

        Returns:
            The corresponding internal ID as an integer if a single external ID is provided,
            or a list of internal IDs if a sequence of external IDs is provided.
        """
        ids = [external_id] if isinstance(external_id, str) else external_id
        need_lookup = [id for id in ids if id not in self._cache]
        if allow_empty and "" in need_lookup:
            # Note we do not want to put empty string in the cache. It is a special case that
            # as of 01/02/2025 only applies to LocationFilters
            need_lookup.remove("")
        if need_lookup:
            self._do_lookup_external_ids(need_lookup, is_dry_run)

        if isinstance(external_id, str):
            return self._get_id_from_cache(external_id, is_dry_run, allow_empty)
        else:
            internal_ids = (
                self._get_id_from_cache(external_id, is_dry_run, allow_empty) for external_id in external_id
            )
            return [id_ for id_ in internal_ids if id_ is not None]

    def _do_lookup_external_ids(self, external_ids: list[str], is_dry_run: bool) -> None:
        try:
            ids_by_external_id = self._id(external_ids)
        except CogniteAPIError as e:
            if 400 <= e.code < 500:
                missing_capabilities = self._toolkit_client.verify.authorization(self._read_acl())
                if missing_capabilities:
                    raise self._toolkit_client.verify.create_error(
                        missing_capabilities,
                        f"lookup {self.resource_name} with external_id {external_ids}",
                    )
            # Raise the original error if it's not a 400 or the user has access to read the resource.from
            raise
        self._cache.update(ids_by_external_id)
        self._reverse_cache.update({v: k for k, v in ids_by_external_id.items()})
        missing_external_ids = [ext_id for ext_id in external_ids if ext_id not in ids_by_external_id]
        if missing_external_ids and not is_dry_run:
            plural = "s" if len(missing_external_ids) > 1 else ""
            plural2 = "They do" if len(missing_external_ids) > 1 else "It does"
            MediumSeverityWarning(
                f"Failed to retrieve {self.resource_name} with external_id{plural} "
                f"{humanize_collection(missing_external_ids)}. {plural2} not exist in CDF"
            ).print_warning(console=self._console)
            self._cache.update({ext_id: None for ext_id in missing_external_ids})

    def _get_id_from_cache(self, external_id: str, is_dry_run: bool = False, allow_empty: bool = False) -> int | None:
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
        """Lookup external IDs for given internal IDs.

        Args:
            id: An integer ID or a sequence of integer IDs to look up. Note that an ID of 0 corresponds
                to an empty string external ID.

        Returns:
            The corresponding external ID as a string if a single ID is provided,
            or a list of external IDs if a sequence of IDs is provided.
            If an ID does not exist, None is returned for that ID.

        """
        ids = [id] if isinstance(id, int) else id
        need_lookup = [id_ for id_ in ids if id_ not in self._reverse_cache if id_ != 0]
        if need_lookup:
            self._do_lookup_internal_ids(need_lookup)

        if isinstance(id, int):
            return self._get_external_id_from_cache(id)
        else:
            external_ids = (self._get_external_id_from_cache(id_) for id_ in ids)
            return [id_ for id_ in external_ids if id_ is not None]

    def _do_lookup_internal_ids(self, ids: list[int]) -> None:
        try:
            found_by_id = self._external_id(ids)
        except CogniteAPIError as e:
            if 400 <= e.code < 500:
                missing_capabilities = self._toolkit_client.verify.authorization(self._read_acl())
                if missing_capabilities:
                    raise self._toolkit_client.verify.create_error(
                        missing_capabilities,
                        f"lookup {self.resource_name} with id {ids}",
                    )
            # Raise the original error if it's not a 400 or the user has access to read the resource.from
            raise
        self._reverse_cache.update(found_by_id)
        self._cache.update({v: k for k, v in found_by_id.items()})
        missing_ids = [id for id in ids if id not in found_by_id]
        if not missing_ids:
            return None
        plural = "s" if len(missing_ids) > 1 else ""
        plural2 = "They do" if len(missing_ids) > 1 else "It does"
        MediumSeverityWarning(
            f"Failed to retrieve {self.resource_name} with id{plural} "
            f"{humanize_collection(missing_ids)}. {plural2} not exist in CDF"
        ).print_warning(console=self._console)
        # Cache the missing IDs with None to avoid repeated lookups
        self._reverse_cache.update({missing_id: None for missing_id in missing_ids})
        return None

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


class FileMetadataLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            file.external_id: file.id
            for file in self._cognite_client.files.retrieve_multiple(external_ids=external_id, ignore_unknown_ids=True)
            if file.external_id and file.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            file.id: file.external_id
            for file in self._cognite_client.files.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if file.external_id and file.id
        }

    def _read_acl(self) -> Capability:
        return FilesAcl(
            [FilesAcl.Action.Read],
            scope=FilesAcl.Scope.All(),
        )


class EventLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            event.external_id: event.id
            for event in self._cognite_client.events.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if event.external_id and event.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            event.id: event.external_id
            for event in self._cognite_client.events.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if event.external_id and event.id
        }

    def _read_acl(self) -> Capability:
        return EventsAcl(
            [EventsAcl.Action.Read],
            scope=EventsAcl.Scope.All(),
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


class FunctionLookUpAPI(LookUpAPI):
    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        return {
            function.external_id: function.id
            for function in self._cognite_client.functions.retrieve_multiple(
                external_ids=external_id, ignore_unknown_ids=True
            )
            if function.external_id and function.id
        }

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        return {
            function.id: function.external_id
            for function in self._cognite_client.functions.retrieve_multiple(ids=id, ignore_unknown_ids=True)
            if function.external_id and function.id
        }

    def _read_acl(self) -> Capability:
        return FunctionsAcl(
            [FunctionsAcl.Action.Read],
            scope=FunctionsAcl.Scope.All(),
        )


class AllLookUpAPI(LookUpAPI, ABC):
    def __init__(
        self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient", console: Console
    ) -> None:
        super().__init__(config, api_version, cognite_client, console)
        self._has_looked_up = False

    @abstractmethod
    def _lookup(self) -> None:
        raise NotImplementedError

    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        if not self._has_looked_up:
            self._lookup()
        found_pairs = ((ext_id, self._cache[ext_id]) for ext_id in external_id if ext_id in self._cache)
        return {k: v for k, v in found_pairs if v is not None}

    def _external_id(self, id: Sequence[int]) -> dict[int, str]:
        if not self._has_looked_up:
            self._lookup()
            self._has_looked_up = True
        found_pairs = ((id_, self._reverse_cache[id_]) for id_ in id if id_ in self._reverse_cache)
        return {k: v for k, v in found_pairs if v is not None}


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
        for location in self._toolkit_client.search.locations.list():
            if location.external_id and location.id:
                self._cache[location.external_id] = location.id
                self._reverse_cache[location.id] = location.external_id

    def _read_acl(self) -> Capability:
        return LocationFiltersAcl(
            [LocationFiltersAcl.Action.Read],
            scope=LocationFiltersAcl.Scope.All(),
        )

    def _id(self, external_id: SequenceNotStr[str]) -> dict[str, int]:
        if not self._has_looked_up:
            self._lookup()
        found_pairs = ((ext_id, self._cache[ext_id]) for ext_id in external_id if ext_id in self._cache)
        return {k: v for k, v in found_pairs if v is not None}


class LookUpGroup(ToolkitAPI):
    def __init__(
        self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient", console: Console
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data_sets = DataSetLookUpAPI(config, api_version, cognite_client, console)
        self.assets = AssetLookUpAPI(config, api_version, cognite_client, console)
        self.time_series = TimeSeriesLookUpAPI(config, api_version, cognite_client, console)
        self.files = FileMetadataLookUpAPI(config, api_version, cognite_client, console)
        self.events = EventLookUpAPI(config, api_version, cognite_client, console)
        self.security_categories = SecurityCategoriesLookUpAPI(config, api_version, cognite_client, console)
        self.location_filters = LocationFiltersLookUpAPI(config, api_version, cognite_client, console)
        self.extraction_pipelines = ExtractionPipelineLookUpAPI(config, api_version, cognite_client, console)
        self.functions = FunctionLookUpAPI(config, api_version, cognite_client, console)
