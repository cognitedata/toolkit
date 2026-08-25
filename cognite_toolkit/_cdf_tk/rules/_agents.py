from collections.abc import Iterable
from functools import cached_property

from cognite_toolkit._cdf_tk.client.resource_classes.agent import ServicesAvailability
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, FailedValidation
from cognite_toolkit._cdf_tk.resource_ios import AgentIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file, read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes.agent import AgentYAML


class AgentRules(ToolkitGlobalRuleSet):
    """Validate agent definitions against the CDF project's AI service availability.

    The availability endpoint is used on a best-effort basis: if it cannot be reached, e.g. because
    no client is available or the endpoint is not yet enabled for the project, any model and runtime
    version is accepted rather than falling back to a hardcoded list that would go stale over time.
    """

    CODE_PREFIX = "AGENT"
    DISPLAY_NAME = "Agents checks"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "Agent model and runtime version validation requires a client. "
                    "Provide client credentials to validate these against the CDF project's AI service availability."
                ),
            )
        if self.service_availability is None:
            return RuleSetStatus(
                code="reduced",
                message="Could not fetch AI service availability for the CDF project. Will allow any model and runtime version.",
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate agent models and runtime versions against the CDF project's AI service availability.",
        )

    def validate(self) -> Iterable[ConsistencyError | FailedValidation]:
        agent_type = ResourceType(resource_folder=AgentIO.folder_name, kind=AgentIO.kind)
        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    # We do not do further validation if there are syntax errors.
                    continue
                if resource.type == agent_type:
                    try:
                        yield from self._validate_agent(resource)
                    except Exception as e:
                        yield FailedValidation(
                            message=f"Agent validation failed for agent definition {resource.build_path.name!r}: {e}",
                            source=str(resource.identifier),
                            source_file=format_insight_source_file(resource.source_path),
                        )

    def _validate_agent(self, resource: BuiltResource) -> Iterable[ConsistencyError]:
        """Validate an agent definition against the CDF project's AI service availability.

        Args:
            resource: The built agent resource to validate.

        Yields:
            ConsistencyError for any violations found.
        """
        source_file = format_insight_source_file(resource.source_path)
        raw_data = read_yaml_file(resource.build_path, expected_output="dict")
        agent_def = AgentYAML.model_validate(raw_data)

        availability = self.service_availability
        if availability is None:
            return

        supported_models = availability.supported_agent_models
        if supported_models is not None and agent_def.model not in supported_models:
            yield ConsistencyError(
                message=(
                    f"Agent '{agent_def.external_id}' model {agent_def.model!r} is not available in this "
                    f"CDF project. Available models: {sorted(supported_models)}."
                ),
                code=f"{self.CODE_PREFIX}-MODEL",
                fix="Use one of the available models for this CDF project.",
                source_file=source_file,
            )

        if agent_def.subagents and agent_def.runtime_version:
            supports_subagents = availability.runtime_version_supports_subagents(agent_def.runtime_version)
            if supports_subagents is False:
                yield ConsistencyError(
                    message=(
                        f"Agent '{agent_def.external_id}' runtime version {agent_def.runtime_version!r} "
                        "does not support subagents."
                    ),
                    code=f"{self.CODE_PREFIX}-SUBAGENTS-RUNTIME-VERSION",
                    fix="Use a runtime version that supports subagents, or remove the 'subagents' field.",
                    source_file=source_file,
                )

        max_tools = availability.max_tools_per_agent
        if agent_def.tools is not None and max_tools is not None and len(agent_def.tools) > max_tools:
            yield ConsistencyError(
                message=(
                    f"Agent '{agent_def.external_id}' has {len(agent_def.tools)} tools, "
                    f"which exceeds the maximum of {max_tools} tools per agent for this CDF project."
                ),
                code=f"{self.CODE_PREFIX}-TOOLS-LIMIT",
                fix=f"Reduce the number of tools to at most {max_tools}.",
                source_file=source_file,
            )

    @cached_property
    def service_availability(self) -> ServicesAvailability | None:
        if not self.client:
            return None
        try:
            return self.client.tool.agents.service_availability()
        except Exception:
            return None
