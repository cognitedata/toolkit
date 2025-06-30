from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
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
    EventHubSourceWrite,
    Job,
    JobList,
    JobWrite,
    JobWriteList,
    KafkaSourceWrite,
    Mapping,
    MappingList,
    MappingWrite,
    MappingWriteList,
    Source,
    SourceList,
    SourceWrite,
    SourceWriteList,
)
from cognite.client.data_classes.hosted_extractors.sources import (
    AuthenticationWrite,
    BasicAuthenticationWrite,
    RESTClientCredentialsAuthenticationWrite,
    RestSourceWrite,
    _MQTTSourceWrite,
)
from cognite.client.utils.useful_types import SequenceNotStr
from rich.console import Console

from cognite_toolkit._cdf_tk._parameters import ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning

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
    _SupportedSources = (_MQTTSourceWrite, KafkaSourceWrite, RestSourceWrite, EventHubSourceWrite)

    @property
    def display_name(self) -> str:
        return "hosted extractor sources"

    @classmethod
    def get_id(cls, item: SourceWrite | Source | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SourceWrite] | None, read_only: bool
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

    def create(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SourceList:
        return self.client.hosted_extractors.sources.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.sources.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Source]:
        return iter(self.client.hosted_extractors.sources)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        # parameterspec is highly dependent on type of source, so we accept any parameter
        return ParameterSpecSet(
            [ParameterSpec((ANYTHING,), frozenset({"dict"}), is_required=False, _is_nullable=False)]
        )

    def dump_resource(self, resource: Source, local: dict[str, Any] | None = None) -> dict[str, Any]:
        HighSeverityWarning(
            "Sources will always be considered different, and thus will always be redeployed."
        ).print_warning()
        return self.dump_id(resource.external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SourceWrite:
        loaded = super().load_resource(resource, is_dry_run=is_dry_run)
        if not isinstance(loaded, self._SupportedSources):
            # We need to explicitly check for the supported types as we need to ensure that we know
            # what the sensitive strings are for each type of source.
            # Technically, we could support any new source type added to the cognite-sdk, but
            # we would risk printing sensitive strings in the terminal.
            raise ToolkitNotSupported(
                f"Hosted extractor source type {type(loaded).__name__} is not supported."
                f"Please contact support to request support for this source type."
            )
        return loaded

    def sensitive_strings(self, item: SourceWrite) -> Iterable[str]:
        if isinstance(item, _MQTTSourceWrite | KafkaSourceWrite | RestSourceWrite) and item.authentication:
            yield from self._sensitive_auth_strings(item.authentication)
        if (
            isinstance(item, _MQTTSourceWrite | KafkaSourceWrite)
            and item.auth_certificate
            and item.auth_certificate.key_password
        ):
            yield item.auth_certificate.key_password

    @staticmethod
    def _sensitive_auth_strings(auth: AuthenticationWrite) -> Iterable[str]:
        if isinstance(auth, BasicAuthenticationWrite):
            yield auth.password
        elif isinstance(auth, RESTClientCredentialsAuthenticationWrite):
            yield auth.client_secret


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

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None):
        super().__init__(client, build_dir, console)
        self._authentication_by_id: dict[str, ClientCredentials] = {}

    @property
    def display_name(self) -> str:
        return "hosted extractor destinations"

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
        cls, items: Sequence[DestinationWrite] | None, read_only: bool
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

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Destination]:
        return iter(self.client.hosted_extractors.destinations)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> DestinationWrite:
        if raw_auth := resource.pop("credentials", None):
            credentials = ClientCredentials._load(raw_auth)
            self._authentication_by_id[self.get_id(resource)] = credentials
            if is_dry_run:
                resource["credentials"] = {"nonce": "dummy_nonce"}
            else:
                session = self.client.iam.sessions.create(credentials, "CLIENT_CREDENTIALS")
                resource["credentials"] = {"nonce": session.nonce}
        if ds_external_id := resource.pop("targetDataSetExternalId", None):
            resource["targetDataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return DestinationWrite._load(resource)

    def dump_resource(self, resource: Destination, local: dict[str, Any] | None = None) -> dict[str, Any]:
        HighSeverityWarning(
            "Destinations will always be considered different, and thus will always be redeployed."
        ).print_warning()
        return self.dump_id(resource.external_id)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Handled by Toolkit

        spec.add(ParameterSpec(("credentials", "clientId"), frozenset({"str"}), is_required=False, _is_nullable=True))
        spec.add(
            ParameterSpec(("credentials", "clientSecret"), frozenset({"str"}), is_required=False, _is_nullable=True)
        )

        spec.discard(ParameterSpec(("targetDataSetId",), frozenset({"int"}), is_required=False, _is_nullable=True))
        spec.add(ParameterSpec(("targetDataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=True))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "targetDataSetId" in item:
            yield DataSetsLoader, item["targetDataSetId"]

    def sensitive_strings(self, item: DestinationWrite) -> Iterable[str]:
        yield item.credentials.nonce
        id_ = self.get_id(item)
        if id_ in self._authentication_by_id:
            yield self._authentication_by_id[id_].client_secret


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
        return "hosted extractor jobs"

    @classmethod
    def get_id(cls, item: JobWrite | Job | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[JobWrite] | None, read_only: bool
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

    def dump_resource(self, resource: Job, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if not dumped.get("config") and "config" not in local:
            dumped.pop("config", None)
        return dumped

    def create(self, items: JobWriteList) -> JobList:
        return self.client.hosted_extractors.jobs.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> JobList:
        return self.client.hosted_extractors.jobs.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: JobWriteList) -> JobList:
        return self.client.hosted_extractors.jobs.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.jobs.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Job]:
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
        return "hosted extractor mappings"

    @classmethod
    def get_id(cls, item: MappingWrite | Mapping | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[MappingWrite] | None, read_only: bool
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

    def create(self, items: MappingWriteList) -> MappingList:
        return self.client.hosted_extractors.mappings.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> MappingList:
        return self.client.hosted_extractors.mappings.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: MappingWriteList) -> MappingList:
        return self.client.hosted_extractors.mappings.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.mappings.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Mapping]:
        return iter(self.client.hosted_extractors.mappings)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Used by the SDK to determine the class to load
        spec.add(ParameterSpec(("input", "type"), frozenset({"str"}), is_required=True, _is_nullable=False))
        return spec
