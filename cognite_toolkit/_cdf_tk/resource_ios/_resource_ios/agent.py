from collections.abc import Hashable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from typing import Any, Literal, TypeVar, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import Agent, AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AgentsAcl,
    AllScope,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel import DataModelIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.function import FunctionIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import SkillIO
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import AgentYAML
from cognite_toolkit._cdf_tk.yaml_classes.agent import (
    AgentDataModel,
    CallFunction,
    ManualQueryDataModels,
    Query,
    QueryKnowledgeGraph,
)

T_Agent = TypeVar("T_Agent", bound=Agent)


@final
class AgentIO(ResourceIO[ExternalId, AgentRequest, AgentResponse]):
    folder_name = "agents"
    resource_cls = AgentResponse
    resource_write_cls = AgentRequest
    kind = "Agent"
    yaml_cls = AgentYAML
    dependencies = frozenset(
        {FunctionIO, DataModelIO, *({SkillIO} if FeatureFlag.is_enabled(Flags.AGENT_SKILLS) else set())}
    )
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

    @staticmethod
    def _data_model_dependencies(data_models: list[dict[str, Any]]) -> Iterable[tuple[type[ResourceIO], DataModelId]]:
        for data_model in data_models:
            space = data_model.get("space")
            external_id = data_model.get("externalId")
            version = data_model.get("version")
            if space and external_id and version:
                yield DataModelIO, DataModelId(space=space, external_id=external_id, version=str(version))

    @staticmethod
    def _yaml_data_model_dependencies(
        data_models: list[AgentDataModel],
    ) -> Iterable[tuple[type[ResourceIO], DataModelId]]:
        for data_model in data_models:
            yield (
                DataModelIO,
                DataModelId(
                    space=data_model.space,
                    external_id=data_model.external_id,
                    version=data_model.version,
                ),
            )

    @staticmethod
    def _call_function_dependencies(tool: CallFunction) -> Iterable[tuple[type[ResourceIO], ExternalId]]:
        yield FunctionIO, ExternalId(external_id=tool.configuration.external_id)

    @staticmethod
    def _query_knowledge_graph_dependencies(
        tool: QueryKnowledgeGraph,
    ) -> Iterable[tuple[type[ResourceIO], DataModelId]]:
        yield from AgentIO._yaml_data_model_dependencies(tool.configuration.data_models)

    @staticmethod
    def _query_dependencies(tool: Query) -> Iterable[tuple[type[ResourceIO], DataModelId]]:
        dm_scope = tool.configuration.data_models
        if dm_scope.type == "manual" and isinstance(dm_scope, ManualQueryDataModels):
            yield from AgentIO._yaml_data_model_dependencies(dm_scope.data_models)

    @classmethod
    def _query_tool_manual_data_models(cls, configuration: dict[str, Any]) -> list[dict[str, Any]]:
        data_models_scope = configuration.get("dataModels")
        if not isinstance(data_models_scope, dict) or data_models_scope.get("type") != "manual":
            return []
        data_models = data_models_scope.get("dataModels")
        if not isinstance(data_models, list):
            return []
        return data_models

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        for subagent in item.get("subagents") or []:
            if isinstance(subagent, dict) and (agent_external_id := subagent.get("agentExternalId")):
                yield AgentIO, ExternalId(external_id=agent_external_id)
        for tool in item.get("tools", []):
            if tool.get("type") == "callFunction":
                if ext_id := tool.get("configuration", {}).get("externalId"):
                    yield FunctionIO, ExternalId(external_id=ext_id)
            elif tool.get("type") == "queryKnowledgeGraph":
                yield from cls._data_model_dependencies(tool.get("configuration", {}).get("dataModels", []))
            elif tool.get("type") == "query":
                yield from cls._data_model_dependencies(
                    cls._query_tool_manual_data_models(tool.get("configuration", {}))
                )
        if FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
            for skill_external_id in item.get("skills") or []:
                if isinstance(skill_external_id, str):
                    yield SkillIO, ExternalId(external_id=skill_external_id)

    @classmethod
    def get_dependencies(cls, resource: AgentYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        for subagent in resource.subagents or []:
            yield AgentIO, ExternalId(external_id=subagent.agent_external_id)
        for tool in resource.tools or []:
            match tool:
                case CallFunction():
                    yield from cls._call_function_dependencies(tool)
                case QueryKnowledgeGraph():
                    yield from cls._query_knowledge_graph_dependencies(tool)
                case Query():
                    yield from cls._query_dependencies(tool)
        if FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
            for skill_external_id in resource.skills or []:
                yield SkillIO, ExternalId(external_id=skill_external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[AgentRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield AgentsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def topological_sort(cls, items: Sequence[T_Agent]) -> list[T_Agent]:
        """Sorts the agents in topological order based on their subagent references.

        Subagents must exist before the agents that reference them, as the agents service
        validates that subagent references point to existing agents.
        """
        agent_by_id: dict[ExternalId, T_Agent] = {item.as_id(): item for item in items}
        dependencies: dict[ExternalId, set[ExternalId]] = {}
        for item_id, item in agent_by_id.items():
            dependencies[item_id] = {
                subagent_id
                for subagent in item.subagents or []
                if (subagent_id := ExternalId(external_id=subagent.agent_external_id)) in agent_by_id
            }
        try:
            return [
                agent_by_id[item_id]
                for item_id in TopologicalSorter(dependencies).static_order()
                if item_id in agent_by_id
            ]
        except CycleError as e:
            raise ToolkitCycleError(
                f"Cannot deploy agents. Cycle detected {e.args} in the 'subagents' references of the agents.",
                *e.args[1:],
            ) from None

    def create(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        return self.client.tool.agents.create(self.topological_sort(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[AgentResponse]:
        return self.client.tool.agents.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[AgentRequest]) -> list[AgentResponse]:
        return self.client.tool.agents.update(self.topological_sort(items))

    def delete(self, ids: Sequence[ExternalId]) -> int:
        # The agents service rejects deleting an agent that is still referenced as a subagent by
        # another agent, so the referencing agents must be deleted before the subagents they reference,
        # i.e. the reverse of the create/update order.
        retrieved = self.retrieve(ids)
        retrieved_ids = {agent.as_id() for agent in retrieved}
        ordered_ids = [agent.as_id() for agent in reversed(self.topological_sort(retrieved))]
        # Ids that could not be retrieved (e.g. already deleted) are appended at the end.
        ordered_ids.extend(id_ for id_ in ids if id_ not in retrieved_ids)

        self.client.tool.agents.delete(ordered_ids, ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[AgentResponse]:
        return self.client.tool.agents.list(limit=None)

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
        for key in ["labels", "exampleQuestions", "skills", "subagents"]:
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
        elif json_path in {("labels",), ("skills",)}:
            return diff_list_hashable(local, cdf)
        elif json_path == ("exampleQuestions",):
            return diff_list_identifiable(
                local, cdf, get_identifier=lambda q: q.get("question", "") if isinstance(q, dict) else str(q)
            )
        elif json_path == ("subagents",):
            return diff_list_identifiable(
                local,
                cdf,
                get_identifier=lambda ref: ref.get("agentExternalId", "") if isinstance(ref, dict) else "",
            )
        return super().diff_list(local, cdf, json_path)
