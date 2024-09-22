from __future__ import annotations

from collections.abc import Iterable
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
    SessionWrite,
    Source,
    SourceList,
    SourceWrite,
    SourceWriteList,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables


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
    def get_required_capability(cls, items: SourceWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        return HostedExtractorsAcl(
            [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write],
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
    kind = "Destination"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Destinations/operation/create_destinations"

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
    def get_required_capability(cls, items: DestinationWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        return HostedExtractorsAcl(
            [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write],
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: DestinationWriteList) -> DestinationList:
        self._set_credentials(items)

        return self.client.hosted_extractors.destinations.create(items)

    def _set_credentials(self, items: DestinationWriteList) -> None:
        for item in items:
            credentials = self._authentication_by_id.get(self.get_id(item))
            if credentials:
                created = self.client.iam.sessions.create(credentials, "CLIENT_CREDENTIALS")
                item.credentials = SessionWrite(created.nonce)

    def retrieve(self, ids: SequenceNotStr[str]) -> DestinationList:
        return self.client.hosted_extractors.destinations.retrieve(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: DestinationWriteList) -> DestinationList:
        self._set_credentials(items)
        return self.client.hosted_extractors.destinations.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.destinations.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Destination]:
        return iter(self.client.hosted_extractors.destinations)

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> DestinationWriteList:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())

        raw_list = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]
        loaded = DestinationWriteList([])
        for item in raw_list:
            if "authentication" in item:
                raw_auth = item.pop("authentication")
                self._authentication_by_id[self.get_id(item)] = ClientCredentials._load(raw_auth)
            if item.get("targetDataSetExternalId") is not None:
                ds_external_id = item.pop("targetDataSetExternalId")
                item["targetDataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation,
                    action="replace targetDataSetExternalId with targetDataSetId in hosted extractor destination",
                )
            loaded.append(DestinationWrite.load(item))
        return loaded
