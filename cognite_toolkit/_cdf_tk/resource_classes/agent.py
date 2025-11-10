from typing import Literal

from pydantic import Field

from .agent_tools import AgentTool
from .base import ToolkitResource

Model = Literal[
    "azure/o3",
    "azure/o4-mini",
    "azure/gpt-4o",
    "azure/gpt-4o-mini",
    "azure/gpt-4.1",
    "azure/gpt-4.1-nano",
    "azure/gpt-4.1-mini",
    "azure/gpt-5",
    "azure/gpt-5-mini",
    "azure/gpt-5-nano",
    "gcp/gemini-2.5-pro",
    "gcp/gemini-2.5-flash",
    "aws/claude-4-sonnet",
    "aws/claude-4-opus",
    "aws/claude-4.1-opus",
    "aws/claude-3.5-sonnet",
]


class AgentYAML(ToolkitResource):
    """Atlas AI Agent"""

    external_id: str = Field(
        description="An external ID that uniquely identifies the agent.",
        min_length=1,
        max_length=128,
        pattern=r"^[^\x00]{1,128}$",
    )
    name: str = Field(
        description="A descriptive name intended for use in user interfaces.",
        min_length=1,
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="A human-readable description of what the agent does, used for documentation only. "
        "This description is not used by the language model.",
        max_length=1024,
    )
    instructions: str | None = Field(
        default=None,
        description="The instructions for the agent prompt the language model to understand "
        "the agent's goals and how to achieve them.",
        max_length=32000,
    )
    model: Model = Field(
        "azure/gpt-4o-mini", description="The name of the model to use. Defaults to your CDF project's default model."
    )
    tools: list[AgentTool] | None = Field(None, description="A list of tools available to the agent.", max_length=20)
    runtime_version: str | None = Field(None, description="The runtime version")
