from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    InternalIdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

from cognite_toolkit._cdf_tk.client.data_classes.agent_tools import AgentTool


class AgentCore(WriteableCogniteResource["AgentWrite"], ABC):
    ## https://github.com/cognitedata/cog-ai/blob/main/cog_ai/agents/core/types/agent.py

    """Representation of an AI Agent in CDF.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent.
        description (str): The description of the agent.
        owner_id (str): The owner ID of the agent.
        instructions (str): Instructions for the agent.
        model (str): (str | None): Name of the language model to use.
        labels (list[str]): list[str] | None: List of labels for the agent.
        example_questions (list[dict[str, Any]]): list[dict[str, Any]] | None: List of example questions for the agent.
        tools (list[AgentTool]): list[AgentTool] | None: List of tools for the agent.
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        description: str,
        owner_id: str,
        instructions: str,
        model: str,
        labels: list[str] | None = None,
        example_questions: list[AgentExampleQuestion] | None = None,
        tools: list[AgentTool] | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.name = name
        self.description = description
        self.owner_id = owner_id
        self.instructions = instructions
        self.model = model
        self.labels = labels
        self.example_questions = example_questions
        self.tools = tools


class Agent(AgentCore):
    """Representation of an AI Agent in CDF.
    This is the read format of an agent.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the agent.
        description (str): The description of the agent.
        owner_id (str): The owner ID of the agent.
        instructions (str): Instructions for the agent.
        model (str): (str | None): Name of the language model to use.
        labels (list[str]): list[str] | None: List of labels for the agent.
        example_questions (list[dict[str, Any]]): list[dict[str, Any]] | None: List of example questions for the agent.
        tools (list[AgentTool]): list[AgentTool] | None: List of tools for the agent.
    """

    def __init__(
        self,
        id: int,
        external_id: str,
        name: str,
        description: str,
        owner_id: str,
        instructions: str,
        model: str,
        labels: list[str] | None = None,
        example_questions: list[AgentExampleQuestion] | None = None,
        tools: list[AgentTool] | None = None,
    ) -> None:
        super().__init__(
            id, external_id, name, description, owner_id, instructions, model, labels, example_questions, tools
        )

    def as_write(self) -> AgentWrite:
        return AgentWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            owner_id=self.owner_id,
            instructions=self.instructions,
            model=self.model,
            labels=self.labels,
            example_questions=self.example_questions,
            tools=self.tools,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Agent:
        example_questions = (
            [AgentExampleQuestion(question=k, answer=v) for k, v in resource["exampleQuestions"].items()]
            if resource.get("exampleQuestions")
            else None
        )

        tools = (
            [AgentTool._load(item) for item in resource["tools"]] if isinstance(resource.get("tools"), list) else None
        )

        return cls(
            id=int(resource["id"]),
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource["description"],
            owner_id=resource["ownerId"],
            instructions=resource["instructions"],
            model=resource["model"],
            labels=resource.get("labels"),
            example_questions=example_questions,
            tools=tools,
        )


class AgentWrite(AgentCore):
    def __init__(
        self,
        external_id: str,
        name: str,
        description: str,
        owner_id: str,
        instructions: str,
        model: str,
        labels: list[str] | None = None,
        example_questions: list[AgentExampleQuestion] | None = None,
        tools: list[AgentTool] | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.description = description
        self.owner_id = owner_id
        self.instructions = instructions
        self.model = model
        self.labels = labels
        self.example_questions = example_questions
        self.tools = tools

    def as_write(self) -> AgentWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> AgentWrite:
        example_questions = (
            [AgentExampleQuestion(question=k, answer=v) for k, v in resource["exampleQuestions"].items()]
            if resource.get("exampleQuestions")
            else None
        )

        tools = (
            [AgentTool._load(item) for item in resource["tools"]] if isinstance(resource.get("tools"), list) else None
        )

        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            description=resource["description"],
            owner_id=resource["owner"],
            instructions=resource["instructions"],
            model=resource["model"],
            labels=resource["labels"],
            example_questions=example_questions,
            tools=tools,
        )


class AgentWriteList(CogniteResourceList[AgentWrite]):
    _RESOURCE = AgentWrite


class AgentList(
    WriteableCogniteResourceList[AgentWrite, Agent],
    InternalIdTransformerMixin,
):
    _RESOURCE = Agent

    def as_write(self) -> AgentWriteList:
        """Returns this AgentList as writeableinstance"""
        return AgentWriteList([item.as_write() for item in self.data], cognite_client=self._get_cognite_client())


# from cog_ai.agents.core.tools import (
#     AnalyzeTimeSeriesTool,
#     CogniteFunctionTool,
#     DeprecatedTool,
#     DocumentAskTool,
#     DocumentSummaryTool,
#     PythonCodeExecutionTool,
#     QueryAndAnalyzeImagesTool,
#     QueryDataModelTool,
#     QueryTimeSeriesDatapointsTool,
#     RestAPITool,
#     WebhookTool,
# )


@dataclass(frozen=True)
class AgentExampleQuestion:
    question: str
    answer: Any
