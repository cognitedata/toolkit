from collections.abc import Hashable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, Literal, TypeVar, cast, final

from yaml.error import YAMLError

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import Agent, AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AgentsAcl,
    AllScope,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel import DataModelIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.function import FunctionIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import SkillIO
from cognite_toolkit._cdf_tk.utils import (
    calculate_hash,
    read_yaml_content,
    safe_read,
    sanitize_filename,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
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
class AgentIO(ResourceIO[ExternalId, AgentRequest, AgentResponse, AgentYAML]):
    folder_name = "agents"
    resource_cls = AgentResponse
    resource_write_cls = AgentRequest
    kind = "Agent"
    yaml_cls = AgentYAML
    extra_content_property = "instructions"
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
    def get_extra_files(cls, filepath: Path, identifier: ExternalId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        """Get extra files for an Agent resource.

        This includes an optional .md file referenced by instructionsFile, optional YAML files
        referenced by toolFiles, and optional .py files referenced by pythonCodeFile on
        runPythonCode tools.
        """
        yield from cls._get_instructions_extra_file(filepath, item)
        yield from cls._get_tools_extra_files(filepath, item)
        yield from cls._get_python_code_extra_files(filepath, item.get("tools"))

    @classmethod
    def _get_instructions_extra_file(cls, filepath: Path, item: dict[str, Any]) -> Iterable[ReadExtra]:
        instructions_file_name = item.get("instructionsFile")
        if not instructions_file_name or not isinstance(instructions_file_name, str):
            return
        instructions_file = filepath.parent / Path(instructions_file_name)
        if not instructions_file.is_file():
            yield FailedReadExtra(
                source_path=instructions_file,
                code="MISSING",
                error=f"Instructions file {instructions_file.as_posix()} not found or is not a file",
            )
            return

        content = safe_read(instructions_file, encoding=BUILD_FOLDER_ENCODING)
        source_hash = calculate_hash(content, shorten=True)
        yield SuccessExtra(
            source_path=instructions_file,
            source_hash=source_hash,
            suffix=".md",
            content=content,
            description="agent instructions",
            resource_field="instructions",
            remove_fields=["instructionsFile"],
        )

    @classmethod
    def _get_tools_extra_files(cls, filepath: Path, item: dict[str, Any]) -> Iterable[ReadExtra]:
        if not isinstance(tools_files := item.get("toolFiles"), list):
            return

        for tools_file_name in tools_files:
            if not isinstance(tools_file_name, str) or not tools_file_name:
                continue
            tools_file = filepath.parent / Path(tools_file_name)
            if not tools_file.is_file():
                yield FailedReadExtra(
                    source_path=tools_file,
                    code="MISSING",
                    error=f"Tools file {tools_file.as_posix()} not found or is not a file",
                )
                continue

            content = safe_read(tools_file, encoding=BUILD_FOLDER_ENCODING)
            parsed_content = cls._try_parse(content)
            if parsed_content is None:
                yield FailedReadExtra(
                    source_path=tools_file,
                    code="SYNTAX-ERROR",
                    error=f"Tools file {tools_file.as_posix()} is not valid YAML",
                )
                continue
            yield from cls._get_python_code_extra_files(tools_file, parsed_content)
            source_hash = calculate_hash(content, shorten=True)
            suffix = tools_file.suffix if tools_file.suffix else ".yaml"
            yield SuccessExtra(
                source_path=tools_file,
                source_hash=source_hash,
                suffix=suffix,
                content_parsed=parsed_content,
                description="agent tools",
                resource_field="tools",
                is_list=True,
                remove_fields=["toolFiles"],
            )

    @classmethod
    def _try_parse(cls, content: str) -> Any | None:
        try:
            parsed = read_yaml_content(content)
        except (YAMLError, ValueError, TypeError):
            # This is handled when validating the Agent resource, so we can ignore it here.
            return None
        if not isinstance(parsed, dict | list):
            return None
        return parsed

    @classmethod
    def _get_python_code_extra_files(cls, filepath: Path, tools: Any) -> Iterable[ReadExtra]:
        tool_list: list[Any] = []
        if isinstance(tools, list):
            tool_list.extend(tools)
        elif isinstance(tools, dict):
            tool_list.append(tools)
        else:
            return

        for tool in tool_list or []:
            if not isinstance(tool, dict) or tool.get("type") != "runPythonCode":
                continue
            config = tool.get("configuration")
            if not isinstance(config, dict):
                continue
            extra = cls._read_and_inline_python_code_python_tool(filepath, config)
            if extra is not None:
                yield extra

    @classmethod
    def _read_and_inline_python_code_python_tool(cls, filepath: Path, config: dict[str, Any]) -> ReadExtra | None:
        code_file_name = config.get("pythonCodeFile")
        if not code_file_name or not isinstance(code_file_name, str):
            return None
        code_file = filepath.parent / Path(code_file_name)
        if not code_file.is_file():
            return FailedReadExtra(
                source_path=code_file,
                code="MISSING",
                error=f"Python code file {code_file.as_posix()} not found or is not a file",
            )
        content = safe_read(code_file, encoding=BUILD_FOLDER_ENCODING)
        config["pythonCode"] = content
        # Mutating the config dict to remove the pythonCodeFile key, so that it is not included in the final resource.
        config.pop("pythonCodeFile", None)
        source_hash = calculate_hash(content, shorten=True)
        suffix = code_file.suffix if code_file.suffix else ".py"
        return SuccessExtra(
            source_path=code_file,
            source_hash=source_hash,
            suffix=suffix,
            content=content,
            description="agent python code",
            resource_field=None,
            write_to_build=False,
        )

    def split_resource(
        self, base_filepath: Path, resource: dict[str, Any]
    ) -> Iterable[tuple[Path, dict[str, Any] | str]]:
        if not Flags.V09.is_enabled():
            yield from super().split_resource(base_filepath, resource)
            return

        if instructions := resource.pop("instructions", None):
            md_path = base_filepath.with_suffix(".md")
            resource["instructionsFile"] = md_path.name
            yield md_path, cast(str, instructions)

        if tools := resource.pop("tools", None):
            if not isinstance(tools, list):
                resource["tools"] = tools
            else:
                tool_paths: list[str] = []
                for tool_no, tool in enumerate(tools, start=1):
                    tool_name = tool.get("name") or f"{tool.get('type', '')}{tool_no!s}"
                    tool_filename = sanitize_filename(tool_name)
                    stem = base_filepath.stem
                    if stem.lower().endswith(f".{self.kind.lower()}"):
                        stem = stem[: -(len(self.kind) + 1)]

                    tools_dir = base_filepath.parent
                    if tool.get("type") == "runPythonCode":
                        tools_dir = base_filepath.parent / "tools"
                        config = tool.get("configuration")
                        if isinstance(config, dict):
                            python_code = config.pop("pythonCode", None)
                            if python_code and isinstance(python_code, str):
                                py_path = tools_dir / f"{stem}.{tool_filename}.py"
                                config["pythonCodeFile"] = py_path.name
                                yield py_path, python_code
                    tools_path = tools_dir / f"{stem}.{tool_filename}.yaml"

                    tool_paths.append(tools_path.relative_to(base_filepath.parent).as_posix())
                    yield tools_path, yaml_safe_dump(tool)

                resource["toolFiles"] = tool_paths

        yield base_filepath, resource

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
