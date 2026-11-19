"""OpenAI-compatible API for chat completions and language models.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Chat-Completions/operation/chat_completions_ai_openai_chat_completions_post
https://api-docs.cognite.com/20230101-beta/tag/Models/operation/list_models_ai_openai_models_get

Note: Chat completions is an alpha API and models is a beta API. Both may change in future releases.
"""

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.openai import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    OpenAIModel,
    OpenAIModelList,
)


class OpenAIAPI:
    """API for the OpenAI-compatible endpoints in CDF."""

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _make_url(self, path: str) -> str:
        return self._http_client.config.create_api_url(path)

    def chat_completions(self, request: ChatCompletionRequest) -> ChatCompletionResponse:
        """Generate a chat completion from a list of messages.

        Only non-streaming responses are supported, so setting ``stream=True`` raises a ValueError.

        Args:
            request: The chat completion request.

        Returns:
            The generated chat completion.
        """
        if (request.model_extra or {}).get("stream"):
            raise ValueError("Streaming chat completions are not supported.")
        message = RequestMessage(
            endpoint_url=self._make_url("/ai/openai/chat/completions"),
            method="POST",
            body_content=request.dump(),
            api_version="alpha",
        )
        response = self._http_client.request_single_retries(message).get_success_or_raise(message)
        return ChatCompletionResponse.model_validate_json(response.body)

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
