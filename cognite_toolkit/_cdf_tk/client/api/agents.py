"""Agents API for managing CDF AI agents.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Agents/operation/main_ai_agents_post

Note: This is an alpha API and may change in future releases.
"""

import builtins
from collections.abc import Iterable, Iterator, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    ErrorDetails,
    FailedResponse,
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
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
        self._chat_path = "/ai/agents/chat"

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

    def chat(
        self,
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None = None,
        actions: Sequence[AgentChatClientAction] | None = None,
        retention_policy: Literal["temporary", "persisted"] | None = None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None,
        content_type: str = "application/json",
    ) -> AgentChatResponse:
        """Send messages to an agent and wait for the reply.

        Args:
            agent_external_id: The external ID of the agent to chat with.
            messages: The user messages, or action messages answering the agent's previous actions.
            cursor: The cursor from the previous reply, to continue the conversation.
            actions: Client tools the agent may call, at most 20.
            retention_policy: Whether the conversation is kept after the session.
            tool_confirmation_mode: Whether tools that require confirmation run without asking.
            content_type: The content-type header of the request.

        Returns:
            The agent's reply.
        """
        body = self._create_chat_body(
            agent_external_id, messages, cursor, actions, retention_policy, tool_confirmation_mode
        )
        request = RequestMessage(
            endpoint_url=self._make_url(self._chat_path),
            method="POST",
            body_content=body.dump(),
            content_type=content_type,
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return AgentChatResponse.model_validate_json(response.body)

    def chat_stream(
        self,
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None = None,
        actions: Sequence[AgentChatClientAction] | None = None,
        retention_policy: Literal["temporary", "persisted"] | None = None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None = None,
        stream_options: AgentChatStreamOptions | None = None,
        content_type: str = "application/json",
    ) -> Iterator[AgentChatResponse]:
        """Send messages to an agent and yield the events of its reply as they arrive.

        The request is sent when iteration starts, and it is not retried, as that could repeat events.
        To resume an interrupted stream, set ``return_cursor`` in the stream options and pass the cursor
        from the first event to a new call.

        Args:
            agent_external_id: The external ID of the agent to chat with.
            messages: The user messages, or action messages answering the agent's previous actions.
            cursor: The cursor from the previous reply, to continue the conversation.
            actions: Client tools the agent may call, at most 20.
            retention_policy: Whether the conversation is kept after the session.
            tool_confirmation_mode: Whether tools that require confirmation run without asking.
            stream_options: Options for the stream.
            content_type: The content-type header of the request.

        Yields:
            The events of the agent's reply: progress, response chunks, the cursor if requested, and lastly the result.
        """
        body = self._create_chat_body(
            agent_external_id, messages, cursor, actions, retention_policy, tool_confirmation_mode
        )
        body.stream = True
        if stream_options is not None:
            body.stream_options = stream_options
        request = RequestMessage(
            endpoint_url=self._make_url(self._chat_path),
            method="POST",
            body_content=body.dump(),
            content_type=content_type,
            accept="text/event-stream",
            api_version=self._api_version,
        )
        http_client = self._http_client
        headers = http_client.create_headers(
            request.api_version, request.content_type, request.accept, disable_gzip=request.disable_gzip
        )
        with http_client.session.stream(
            request.method,
            request.endpoint_url,
            content=request.content,
            headers=headers,
            timeout=http_client.config.timeout,
            follow_redirects=False,
        ) as response:
            if not 200 <= response.status_code < 300:
                response.read()
                FailedResponse(
                    status_code=response.status_code,
                    body=response.text,
                    error=ErrorDetails.from_response(response),
                ).get_success_or_raise(request)
            data_lines: builtins.list[str] = []
            for line in response.iter_lines():
                if line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").removeprefix(" "))
                elif not line and data_lines:
                    yield AgentChatResponse.model_validate_json("\n".join(data_lines))
                    data_lines = []
            if data_lines:
                yield AgentChatResponse.model_validate_json("\n".join(data_lines))

    @staticmethod
    def _create_chat_body(
        agent_external_id: str,
        messages: Sequence[AgentChatMessage],
        cursor: str | None,
        actions: Sequence[AgentChatClientAction] | None,
        retention_policy: Literal["temporary", "persisted"] | None,
        tool_confirmation_mode: Literal["default", "dangerouslyAllowAll"] | None,
    ) -> AgentChatRequest:
        body = AgentChatRequest(agent_external_id=agent_external_id, messages=list(messages))
        if cursor is not None:
            body.cursor = cursor
        if actions is not None:
            body.actions = list(actions)
        if retention_policy is not None:
            body.retention_policy = retention_policy
        if tool_confirmation_mode is not None:
            body.tool_confirmation_mode = tool_confirmation_mode
        return body

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
