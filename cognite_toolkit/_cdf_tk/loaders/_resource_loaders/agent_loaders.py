from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from typing import Any

from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters.constants import ANY_INT, ANYTHING
from cognite_toolkit._cdf_tk._parameters.data_classes import ParameterSpec, ParameterSpecSet
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
    _doc_url = "https://pr-2829.specs.preview.cogniteapp.com/20230101-alpha.json.html"

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

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Tool configuration is a dict where the accepted keys depend on the tool type.
        spec.add(
            ParameterSpec(
                ("tools", ANY_INT, "configuration", ANYTHING),
                frozenset({"dict"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        return spec

    def create(self, items: AgentWriteList) -> AgentList:
        return self.client.agents.apply(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> AgentList:
        return self.client.agents.retrieve(ids)

    def update(self, items: AgentWriteList) -> AgentList:
        return self.client.agents.apply(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.agents.delete(ids)
        except CogniteAPIError:
            deleted = 0
            for id in ids:
                try:
                    self.client.agents.delete(id)
                    deleted += 1
                except CogniteAPIError:
                    # accepted because the resource may not exist
                    pass
            return deleted
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Agent]:
        return iter([])
