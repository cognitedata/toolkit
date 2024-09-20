from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from cognite.client.data_classes.capabilities import Capability, HostedExtractorsAcl
from cognite.client.data_classes.hosted_extractors import Source, SourceList, SourceWrite, SourceWriteList
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader


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
