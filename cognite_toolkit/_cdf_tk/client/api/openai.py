"""OpenAI-compatible API for chat completions and language models.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Chat-Completions/operation/chat_completions_ai_openai_chat_completions_post
https://api-docs.cognite.com/20230101-beta/tag/Models/operation/list_models_ai_openai_models_get

Note: Chat completions is an alpha API and models is a beta API. Both may change in future releases.
"""

from collections.abc import Iterator

from cognite_toolkit._cdf_tk.client.http_client import ErrorDetails, FailedResponse, HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.openai import (
    ChatCompletionChunk,
    ChatCompletionRequest,
    ChatCompletionResponse,
    OpenAIModel,
    OpenAIModelList,
)

_STREAM_DONE = "[DONE]"


class OpenAIAPI:
    """API for the OpenAI-compatible endpoints in CDF."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client
        self._chat_completions_path = "/ai/openai/chat/completions"

    def _make_url(self, path: str) -> str:
        return self._http_client.config.create_api_url(path)

    def chat_completions(self, request: ChatCompletionRequest) -> ChatCompletionResponse:
        """Generate a chat completion from a list of messages.

        Args:
            request: The chat completion request.

        Returns:
            The generated chat completion.
        """
        if request.stream:
            raise ValueError("Use chat_completion_stream for streaming chat completions.")
        message = RequestMessage(
            endpoint_url=self._make_url(self._chat_completions_path),
            method="POST",
            body_content=request.dump(),
            api_version="alpha",
        )
        response = self._http_client.request_single_retries(message).get_success_or_raise(message)
        return ChatCompletionResponse.model_validate_json(response.body)

    def chat_completion_stream(self, request: ChatCompletionRequest) -> Iterator[ChatCompletionChunk]:
        """Generate a chat completion and yield its chunks as they arrive.

        The request is sent when iteration starts, and it is not retried, as that could repeat chunks.

        Args:
            request: The chat completion request. The stream field is set automatically.

        Yields:
            The chunks of the completion. Concatenate the delta content of each choice to get the full message.
        """
        message = RequestMessage(
            endpoint_url=self._make_url(self._chat_completions_path),
            method="POST",
            body_content={**request.dump(), "stream": True},
            accept="text/event-stream",
            api_version="alpha",
        )
        http_client = self._http_client
        headers = http_client.create_headers(
            message.api_version, message.content_type, message.accept, disable_gzip=message.disable_gzip
        )
        with http_client.session.stream(
            message.method,
            message.endpoint_url,
            content=message.content,
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
                ).get_success_or_raise(message)
            data_lines: list[str] = []
            for line in response.iter_lines():
                if line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").removeprefix(" "))
                    continue
                if line or not data_lines:
                    continue
                data = "\n".join(data_lines)
                data_lines = []
                if data == _STREAM_DONE:
                    return
                yield ChatCompletionChunk.model_validate_json(data)
            if data_lines and (data := "\n".join(data_lines)) != _STREAM_DONE:
                yield ChatCompletionChunk.model_validate_json(data)

    def list_models(self) -> list[OpenAIModel]:
        """List the language models available in the CDF project.

        Returns:
            The available models. Their IDs are valid values for ``ChatCompletionRequest.model``.
        """
        message = RequestMessage(
            endpoint_url=self._make_url("/ai/openai/models"),
            method="GET",
            api_version="beta",
        )
        response = self._http_client.request_single_retries(message).get_success_or_raise(message)
        return OpenAIModelList.model_validate_json(response.body).data
