from collections.abc import Iterable
from functools import cached_property
from typing import NamedTuple

from cognite_toolkit._cdf_tk.client.resource_classes.agent import ServicesAvailability
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, FailedValidation
from cognite_toolkit._cdf_tk.resource_ios import AgentIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file, read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes.agent import AgentYAML


class RuntimeCapabilityRequirement(NamedTuple):
    """An AgentYAML field that is gated behind a runtime version capability."""

    field_name: str
    capability: str

    def is_used(self, agent_def: AgentYAML) -> bool:
        return bool(getattr(agent_def, self.field_name))


# Fields whose usage requires the runtime version to advertise the matching capability. Add
# to this list as the API exposes more per-field capabilities under agentRuntimeVersions[].capabilities.
RUNTIME_CAPABILITY_REQUIREMENTS: tuple[RuntimeCapabilityRequirement, ...] = (
    RuntimeCapabilityRequirement(field_name="subagents", capability="SUBAGENTS"),
    RuntimeCapabilityRequirement(field_name="skills", capability="SKILLS"),
)


class AgentRules(ToolkitGlobalRuleSet):
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
                    f"CDF project. Available models: {humanize_collection(supported_models)}."
                ),
                code=f"{self.CODE_PREFIX}-MODEL",
                fix="Use one of the available models for this CDF project.",
                source_file=source_file,
            )

        if agent_def.runtime_version:
            supported_runtime_versions = availability.supported_agent_runtime_versions
            if supported_runtime_versions is not None and agent_def.runtime_version not in supported_runtime_versions:
                yield ConsistencyError(
                    message=(
                        f"Agent '{agent_def.external_id}' runtime version {agent_def.runtime_version!r} is not "
                        f"available in this CDF project. "
                        f"Available runtime versions: {humanize_collection(supported_runtime_versions)}."
                    ),
                    code=f"{self.CODE_PREFIX}-RUNTIME-VERSION",
                    fix="Use one of the available runtime versions for this CDF project.",
                    source_file=source_file,
                )

            for requirement in RUNTIME_CAPABILITY_REQUIREMENTS:
                if not requirement.is_used(agent_def):
                    continue
                has_capability = availability.runtime_version_has_capability(
                    agent_def.runtime_version, requirement.capability
                )
                if has_capability is False:
                    yield ConsistencyError(
                        message=(
                            f"Agent '{agent_def.external_id}' runtime version {agent_def.runtime_version!r} "
                            f"does not support the '{requirement.field_name}' field."
                        ),
                        code=f"{self.CODE_PREFIX}-RUNTIME-UNSUPPORTED-CAPABILITY",
                        fix=(
                            f"Use a runtime version that supports '{requirement.field_name}', "
                            f"or remove the '{requirement.field_name}' field."
                        ),
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
