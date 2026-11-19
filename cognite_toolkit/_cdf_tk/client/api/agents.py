"""Agents API for managing CDF AI agents.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/main_ai_agents_post

Note: This is an alpha API and may change in future releases.
"""

import builtins
from collections.abc import Iterable, Sequence
from typing import Literal, overload

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse, ServicesAvailability
from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatClientAction,
    AgentChatMessage,
    AgentChatRequest,
    AgentChatResponse,
    AgentChatStreamOptions,
)


class AgentsAPI(CDFResourceAPI[AgentResponse]):
    """API for managing CDF AI agents.

    Note: This is an alpha API and may change in future releases.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/ai/agents", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/ai/agents/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/ai/agents/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/ai/agents", item_limit=1000),
            },
            api_version="beta",
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[AgentResponse]:
        return PagedResponse[AgentResponse].model_validate_json(response.body)

    def create(self, items: Sequence[AgentRequest]) -> builtins.list[AgentResponse]:
        """Apply (create or update) agents in CDF.

        Args:
            items: List of AgentRequest objects to apply.

        Returns:
            List of applied AgentResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[AgentRequest]) -> builtins.list[AgentResponse]:
        """Update agents in CDF.

        Args:
            items: List of AgentRequest objects to update.
        Returns:
            List of updated AgentResponse objects.
        """
        # Implemented as an alias to create (upsert) to have a standardized interface.
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> builtins.list[AgentResponse]:
        """Retrieve agents from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved AgentResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete agents from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[builtins.list[AgentResponse]]:
        """Iterate over all agents in CDF.

        Args:
            limit: Maximum total number of agents to return.

        Returns:
            Iterable of lists of AgentResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> builtins.list[AgentResponse]:
        """List all agents in CDF.

        Args:
            limit: Maximum total number of agents to return.

        Returns:
            List of AgentResponse objects.
        """
        return self._list(limit=limit)

    @overload
    def chat(
        self,
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None = None,
        actions: Sequence[AgentChatClientAction] | None = None,
        retention_policy: Literal["temporary", "persisted"] | None = None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None,
        stream: Literal[False] = False,
        stream_options: AgentChatStreamOptions | None = None,
        content_type: str = "application/json",
    ) -> AgentChatResponse: ...

    @overload
    def chat(
        self,
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None = None,
        actions: Sequence[AgentChatClientAction] | None = None,
        retention_policy: Literal["temporary", "persisted"] | None = None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None,
        *,
        stream: Literal[True],
        stream_options: AgentChatStreamOptions | None = None,
        content_type: str = "application/json",
    ) -> builtins.list[AgentChatResponse]: ...

    def chat(
        self,
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None = None,
        actions: Sequence[AgentChatClientAction] | None = None,
        retention_policy: Literal["temporary", "persisted"] | None = None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None,
        stream: bool = False,
        stream_options: AgentChatStreamOptions | None = None,
        content_type: str = "application/json",
    ) -> AgentChatResponse | builtins.list[AgentChatResponse]:
        """Send messages to an agent and wait for the reply.

        Args:
            agent_external_id: The external ID of the agent to chat with.
            messages: The user messages, or action messages answering the agent's previous actions.
            cursor: The cursor from the previous reply, to continue the conversation.
            actions: Client tools the agent may call, at most 20.
            retention_policy: Whether the conversation is kept after the session.
            tool_confirmation_mode: Whether tools that require confirmation run without asking.
            stream: Whether the agent streams its reply as server-sent events. The events are returned once the
                stream is complete, ending with the result.
            stream_options: Options for the stream.
            content_type: The content-type header of the request.

        Returns:
            The agent's reply, or all events of the stream if stream is True.
        """
        body = AgentChatRequest(agent_external_id=agent_external_id, messages=list(messages))
        if cursor is not None:
            body.cursor = cursor
        if actions is not None:
            body.actions = list(actions)
        if retention_policy is not None:
            body.retention_policy = retention_policy
        if tool_confirmation_mode is not None:
            body.tool_confirmation_mode = tool_confirmation_mode
        if stream:
            body.stream = stream
        if stream_options is not None:
            body.stream_options = stream_options
        request = RequestMessage(
            endpoint_url=self._make_url("/ai/agents/chat"),
            method="POST",
            body_content=body.dump(),
            content_type=content_type,
            accept="text/event-stream" if stream else "application/json",
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        if stream:
            return self._parse_server_sent_events(response.body)
        return AgentChatResponse.model_validate_json(response.body)

    @staticmethod
    def _parse_server_sent_events(body: str) -> builtins.list[AgentChatResponse]:
        events: builtins.list[AgentChatResponse] = []
        for block in body.replace("\r\n", "\n").split("\n\n"):
            data = "\n".join(
                line.removeprefix("data:").removeprefix(" ") for line in block.splitlines() if line.startswith("data:")
            )
            if data:
                events.append(AgentChatResponse.model_validate_json(data))
        return events

    def service_availability(self) -> ServicesAvailability:
        """Get the availability of AI services in the CDF project.

        This includes which language models and agent runtime version capabilities are available.

        Returns:
            ServicesAvailability object describing the AI services available in the project.
        """
        request = RequestMessage(
            endpoint_url=self._make_url("/ai/services/availability"),
            method="GET",
            api_version="20230101-alpha",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return ServicesAvailability.model_validate_json(response.body)
