from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cognite.client.data_classes.capabilities import Capability, HostedExtractorsAcl
from cognite.client.data_classes.hosted_extractors import Source, SourceList, SourceWrite, SourceWriteList
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader


class HostedExtractorSourceLoader(ResourceLoader[str, SourceWrite, Source, SourceWriteList, SourceList]):
    @classmethod
    def get_id(cls, item: SourceWrite | Source | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: SourceWriteList) -> Capability | list[Capability]:
        if len(items) == 0:
            return []

        return HostedExtractorsAcl(
            [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write],
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> SourceList:
        return self.client.hosted_extractors.sources.retrieve(external_ids=ids)

    def update(self, items: SourceWriteList) -> SourceList:
        return self.client.hosted_extractors.sources.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.hosted_extractors.sources.delete(ids, ignore_unknown_ids=True)
        return len(ids)

    def iterate(self) -> Iterable[Source]:
        return iter(self.client.hosted_extractors.sources)
