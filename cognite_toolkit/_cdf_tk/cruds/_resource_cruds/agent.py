from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from typing import Any

from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert, AgentUpsertList
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters.constants import ANY_INT, ANYTHING
from cognite_toolkit._cdf_tk._parameters.data_classes import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable


class AgentCRUD(ResourceCRUD[str, AgentUpsert, Agent, AgentUpsertList, AgentList]):
    folder_name = "agents"
    filename_pattern = r".*\.Agent$"  # Matches all yaml files whose stem ends with '.Agent'.
    resource_cls = Agent
    resource_write_cls = AgentUpsert
    list_cls = AgentList
    list_write_cls = AgentUpsertList
    kind = "Agent"
    _doc_base_url = ""
    _doc_url = "https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/main_ai_agents_post/"

    @classmethod
    def get_id(cls, item: AgentUpsert | Agent | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[AgentUpsert] | None, read_only: bool
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

    def create(self, items: AgentUpsertList) -> AgentList:
        return self.client.agents.upsert(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> AgentList:
        return self.client.agents.retrieve(ids, ignore_unknown_ids=True)

    def update(self, items: AgentUpsertList) -> AgentList:
        return self.client.agents.upsert(items)

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
        return self.client.agents.list()

    def dump_resource(self, resource: Agent, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        # The atlas endpoints are not yet full implemented. There are properties being added and removed that are
        # not part of the official API. For example, as of 31.July 2025, `labels` is not part of the API, however,
        # this is necessary to ensure that the agents are shown as published in the UI, so we cannot ignore it.
        # The below logic ensures that we keep the unknown properties returned by the API, such that when we run
        # `cdf dump agents` we will not lose any properties that are not part of the official API.
        if (unknown_props := getattr(resource, "_unknown_properties", None)) and isinstance(unknown_props, dict):
            dumped.update(unknown_props)
        if local is None:
            return dumped
        if resource.instructions == "" and "instructions" not in local:
            # Instructions are optional, if not set the server set them to an empty string.
            # We remove them from the dumped resource to ensure it will be equal to the local resource.
            dumped.pop("instructions", None)
        for key in ["labels", "exampleQuestions"]:
            if key not in local and not dumped.get(key):
                # If the local resource does not have the key and the server set Agent has it set to an empty list,
                # we remove it from the dumped resource to ensure it will be equal to the local resource.
                dumped.pop(key, None)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        """
        Compare two lists and return a mapping of local indices to CDF indices and a list of CDF indices that are not
        present in the local list.
        """
        if json_path == ("tools",):
            return diff_list_identifiable(
                local, cdf, get_identifier=lambda t: (t.get("name", ""), t.get("description", ""))
            )
        elif json_path == ("labels",):
            return diff_list_hashable(local, cdf)
        elif json_path == ("exampleQuestions",):
            return diff_list_identifiable(
                local, cdf, get_identifier=lambda q: q.get("question", "") if isinstance(q, dict) else str(q)
            )
        return super().diff_list(local, cdf, json_path)
