from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from typing import Any

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.agents import Agent, AgentList, AgentWrite, AgentWriteList
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader


class AgentLoader(ResourceLoader[str, AgentWrite, Agent, AgentWriteList, AgentList]):
    folder_name = "agents"
    filename_pattern = r".*\.Agent$"  # Matches all yaml files whose stem ends with '.Agent'.
    resource_cls = Agent
    resource_write_cls = AgentWrite
    list_cls = AgentList
    list_write_cls = AgentWriteList
    kind = "Agent"
    _doc_base_url = ""
    _doc_url = ""

    @classmethod
    def get_id(cls, item: AgentWrite | Agent | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[AgentWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    def create(self, items: AgentWriteList) -> AgentList:
        return self.client.agents.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> AgentList:
        return self.client.agents.retrieve(ids)

    def update(self, items: AgentWriteList) -> AgentList:
        # The agent API does not support update, but accepts create with the same external ID.
        return self.client.agents.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        return 0

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Agent]:
        return iter([])
