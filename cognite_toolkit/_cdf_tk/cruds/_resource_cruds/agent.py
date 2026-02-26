from collections.abc import Hashable, Iterable, Sequence
from typing import Any

from cognite.client.data_classes.capabilities import AgentsAcl, Capability

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.function import FunctionCRUD
from cognite_toolkit._cdf_tk.resource_classes import AgentYAML
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename


class AgentCRUD(ResourceCRUD[ExternalId, AgentRequest, AgentResponse]):
    folder_name = "agents"
    resource_cls = AgentResponse
    resource_write_cls = AgentRequest
    kind = "Agent"
    yaml_cls = AgentYAML
    dependencies = frozenset({FunctionCRUD})
    _doc_base_url = ""
    _doc_url = "https://api-docs.cognite.com/20230101-beta/tag/Agents/operation/main_ai_agents_post/"

    @classmethod
    def get_id(cls, item: AgentRequest | AgentResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        for tool in item.get("tools", []):
            if tool.get("type") == "callFunction":
                if ext_id := tool.get("configuration", {}).get("externalId"):
                    yield FunctionCRUD, ExternalId(external_id=ext_id)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[AgentRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [AgentsAcl.Action.READ] if read_only else [AgentsAcl.Action.READ, AgentsAcl.Action.WRITE]

        return AgentsAcl(actions, AgentsAcl.Scope.All())

    def create(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        return self.client.tool.agents.create(items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[AgentResponse]:
        return self.client.tool.agents.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        return self.client.tool.agents.update(items)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        self.client.tool.agents.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[AgentResponse]:
        return self.client.tool.agents.list()

    def dump_resource(self, resource: AgentResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        # The atlas endpoints are not yet full implemented. There are properties being added and removed that are
        # not part of the official API. For example, as of 31.July 2025, `labels` is not part of the API, however,
        # this is necessary to ensure that the agents are shown as published in the UI, so we cannot ignore it.
        # The below logic ensures that we keep the unknown properties returned by the API, such that when we run
        # `cdf dump agents` we will not lose any properties that are not part of the official API.
        if (unknown_props := getattr(resource, "__pydantic_extra__", None)) and isinstance(unknown_props, dict):
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
