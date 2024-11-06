from __future__ import annotations

from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any

from cognite.client.data_classes import ClientCredentials
from cognite.client.data_classes.capabilities import Capability, HostedExtractorsAcl
from cognite.client.data_classes.hosted_extractors import (
    Destination,
    DestinationList,
    DestinationWrite,
    DestinationWriteList,
    Job,
    JobList,
    JobWrite,
    JobWriteList,
    Mapping,
    MappingList,
    MappingWrite,
    MappingWriteList,
    Source,
    SourceList,
    SourceWrite,
    SourceWriteList,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from .data_organization_loaders import DataSetsLoader


class HostedExtractorSourceLoader(ResourceLoader[str, SourceWrite, Source, SourceWriteList, SourceList]):
    folder_name = "hosted_extractors"
    filename_pattern = r".*\.Source$"  # Matches all yaml files whose stem ends with '.Source'.
    resource_cls = Source
    resource_write_cls = SourceWrite
    list_cls = SourceList
    list_write_cls = SourceWriteList
    kind = "Source"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Sources/operation/create_sources"
    do_environment_variable_injection = True

    @property
    def display_name(self) -> str:
        return "Hosted Extractor Source"

    @classmethod
    def get_id(cls, item: SourceWrite | Source | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: SourceWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SourceList:
        return self.client.hosted_extractors.sources.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.sources.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Source]:
        return iter(self.client.hosted_extractors.sources)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Used by the SDK to determine the class to load the resource into.
        spec.add(ParameterSpec(("type",), frozenset({"str"}), is_required=True, _is_nullable=False))
        spec.add(ParameterSpec(("authentication", "type"), frozenset({"str"}), is_required=True, _is_nullable=False))
        return spec

    def _are_equal(
        self, local: SourceWrite, cdf_resource: Source, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        HighSeverityWarning(
            "Destinations will always be considered different, and thus will always be redeployed."
        ).print_warning()
        local_dumped = local.dump()
        # Source does not have .as_write method as there are secrets in the write object
        # which are not returned by the API.
        cdf_dumped = cdf_resource.dump()

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)


class HostedExtractorDestinationLoader(
    ResourceLoader[str, DestinationWrite, Destination, DestinationWriteList, DestinationList]
):
    folder_name = "hosted_extractors"
    filename_pattern = r".*\.Destination$"  # Matches all yaml files whose stem ends with '.Destination'.
    resource_cls = Destination
    resource_write_cls = DestinationWrite
    list_cls = DestinationList
    list_write_cls = DestinationWriteList
    dependencies = frozenset({DataSetsLoader})
    kind = "Destination"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Destinations/operation/create_destinations"
    do_environment_variable_injection = True

    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        super().__init__(client, build_dir)
        self._authentication_by_id: dict[str, ClientCredentials] = {}

    @property
    def display_name(self) -> str:
        return "Hosted Extractor Destination"

    @classmethod
    def get_id(cls, item: DestinationWrite | Destination | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: DestinationWriteList | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: DestinationWriteList) -> DestinationList:
        return self.client.hosted_extractors.destinations.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> DestinationList:
        return self.client.hosted_extractors.destinations.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: DestinationWriteList) -> DestinationList:
        return self.client.hosted_extractors.destinations.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.destinations.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Destination]:
        return iter(self.client.hosted_extractors.destinations)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> DestinationWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

        raw_list = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]
        loaded = DestinationWriteList([])
        for item in raw_list:
            if "credentials" in item:
                raw_auth = item.pop("credentials")
                credentials = ClientCredentials._load(raw_auth)
                if skip_validation:
                    item["credentials"] = {"nonce": "dummy_nonce"}
                else:
                    session = self.client.iam.sessions.create(credentials, "CLIENT_CREDENTIALS")
                    item["credentials"] = {"nonce": session.nonce}
            if item.get("targetDataSetExternalId") is not None:
                ds_external_id = item.pop("targetDataSetExternalId")
                item["targetDataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation,
                    action="replace targetDataSetExternalId with targetDataSetId in hosted extractor destination",
                )
            loaded.append(DestinationWrite.load(item))
        return loaded

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Handled by Toolkit
        spec.discard(ParameterSpec(("targetDataSetId",), frozenset({"int"}), is_required=False, _is_nullable=True))
        spec.add(ParameterSpec(("targetDataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=True))
        return spec

    def _are_equal(
        self, local: DestinationWrite, cdf_resource: Destination, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        """This can be overwritten in subclasses that require special comparison logic.

        For example, TransformationWrite has OIDC credentials that will not be returned
        by the retrieve method, and thus needs special handling.
        """
        HighSeverityWarning(
            "Destinations will always be considered different, and thus will always be redeployed."
        ).print_warning()
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.dump()
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "targetDataSetId" in item:
            yield DataSetsLoader, item["targetDataSetId"]


class HostedExtractorJobLoader(ResourceLoader[str, JobWrite, Job, JobWriteList, JobList]):
    folder_name = "hosted_extractors"
    filename_pattern = r".*\.Job$"  # Matches all yaml files whose stem ends with '.Job'.
    resource_cls = Job
    resource_write_cls = JobWrite
    list_cls = JobList
    list_write_cls = JobWriteList
    dependencies = frozenset({HostedExtractorSourceLoader, HostedExtractorDestinationLoader})
    kind = "Job"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Jobs/operation/create_jobs"

    @property
    def display_name(self) -> str:
        return "Hosted Extractor Job"

    @classmethod
    def get_id(cls, item: JobWrite | Job | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: JobWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: JobWriteList) -> JobList:
        return self.client.hosted_extractors.jobs.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> JobList:
        return self.client.hosted_extractors.jobs.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: JobWriteList) -> JobList:
        return self.client.hosted_extractors.jobs.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.jobs.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Job]:
        return iter(self.client.hosted_extractors.jobs)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Used by the SDK to determine the class to load
        spec.add(
            ParameterSpec(
                (
                    "format",
                    "type",
                ),
                frozenset({"str"}),
                is_required=True,
                _is_nullable=False,
            )
        )
        spec.add(
            ParameterSpec(
                ("config", "incrementalLoad", "type"), frozenset({"str"}), is_required=True, _is_nullable=False
            )
        )
        spec.add(
            ParameterSpec(("config", "pagination", "type"), frozenset({"str"}), is_required=True, _is_nullable=False)
        )

        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "sourceId" in item:
            yield HostedExtractorSourceLoader, item["sourceId"]
        if "destinationId" in item:
            yield HostedExtractorDestinationLoader, item["destinationId"]


class HostedExtractorMappingLoader(ResourceLoader[str, MappingWrite, Mapping, MappingWriteList, MappingList]):
    folder_name = "hosted_extractors"
    filename_pattern = r".*\.Mapping$"  # Matches all yaml files whose stem ends with '.Mapping'.
    resource_cls = Mapping
    resource_write_cls = MappingWrite
    list_cls = MappingList
    list_write_cls = MappingWriteList
    # This is not an explicit dependency, however, adding it here as mapping will should be deployed after source.
    dependencies = frozenset({HostedExtractorSourceLoader})
    kind = "Mapping"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Mappings/operation/create_mappings"

    @property
    def display_name(self) -> str:
        return "Hosted Extractor Mapping"

    @classmethod
    def get_id(cls, item: MappingWrite | Mapping | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: MappingWriteList | None, read_only: bool) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: MappingWriteList) -> MappingList:
        return self.client.hosted_extractors.mappings.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> MappingList:
        return self.client.hosted_extractors.mappings.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: MappingWriteList) -> MappingList:
        return self.client.hosted_extractors.mappings.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.mappings.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Mapping]:
        return iter(self.client.hosted_extractors.mappings)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Used by the SDK to determine the class to load
        spec.add(ParameterSpec(("input", "type"), frozenset({"str"}), is_required=True, _is_nullable=False))
        return spec
