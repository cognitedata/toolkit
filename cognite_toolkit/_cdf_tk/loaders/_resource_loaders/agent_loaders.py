from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk._parameters.constants import ANYTHING
from cognite_toolkit._cdf_tk.client.data_classes.agents import Agent, AgentList, AgentWrite, AgentWriteList
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader


@final
class AgentLoader(ResourceLoader[str, AgentWrite, Agent, AgentWriteList, AgentList]):
    folder_name = "locations"
    filename_pattern = r"^.*Agent$"
    resource_cls = Agent
    resource_write_cls = AgentWrite
    list_cls = AgentList
    list_write_cls = AgentWriteList

    kind = "Agent"
    _doc_url = ""

    @property
    def display_name(self) -> str:
        return "agents"

    @classmethod
    def get_id(cls, item: Agent | AgentWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Asset must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[AgentWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # TODO: add when capability is defined
        return []

    def create(self, items: AgentWriteList) -> AgentList:
        return AgentList([])

    def retrieve(self, ids: SequenceNotStr[str | int]) -> AgentList:
        return AgentList([])

    def update(self, items: Sequence[AgentWrite]) -> AgentList:
        return AgentList([])

    def delete(self, ids: SequenceNotStr[str | int]) -> int:
        return 0

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        # TODO: addd when spec is confirmed
        return ParameterSpecSet(
            [ParameterSpec((ANYTHING,), frozenset({"dict"}), is_required=False, _is_nullable=False)]
        )

    @classmethod
    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Agent]:
        return iter([])
