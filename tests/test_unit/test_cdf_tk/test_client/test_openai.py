import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.openai import OpenAIAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.openai import (
    ChatCompletionRequest,
    OpenAIFunction,
    OpenAIMessage,
    OpenAITool,
)

_COMPLETION = {
    "id": "chatcmpl-def456",
    "object": "chat.completion",
    "created": 1715000000,
    "model": "azure/gpt-4.1",
    "choices": [
        {
            "index": 0,
            "finish_reason": "tool_calls",
            "message": {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_abc123",
                        "type": "function",
                        "function": {"name": "get_current_weather", "arguments": '{"location": "Paris, France"}'},
                    }
                ],
            },
        }
    ],
    "usage": {"prompt_tokens": 57, "completion_tokens": 18, "total_tokens": 75},
}

_MODEL = {
    "id": "azure/gpt-4.1",
    "object": "model",
    "created": 0,
    "owned_by": "Cognite",
    "context_length": 1047576,
    "max_completion_tokens": 32768,
    "earliest_retirement_date": 1775000000,
}


@pytest.mark.usefixtures("disable_gzip")
class TestOpenAIAPI:
    def test_chat_completions(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = OpenAIAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/ai/openai/chat/completions")).mock(
            return_value=httpx2.Response(status_code=200, json=_COMPLETION)
        )
        request = ChatCompletionRequest(
            model="azure/gpt-4.1",
            messages=[OpenAIMessage(role="user", content="What is the weather in Paris?")],
            max_completion_tokens=100,
            tool_choice="auto",
            tools=[
                OpenAITool(
                    function=OpenAIFunction(
                        name="get_current_weather",
                        parameters={"type": "object", "properties": {"location": {"type": "string"}}},
                    )
                )
            ],
        )

        response = api.chat_completions(request)

        assert {
            "version": respx_mock.calls[0].request.headers["cdf-version"],
            "body": json.loads(respx_mock.calls[0].request.content),
            "response": response.dump(),
        } == {
            "version": "alpha",
            "body": {
                "model": "azure/gpt-4.1",
                "messages": [{"role": "user", "content": "What is the weather in Paris?"}],
                "max_completion_tokens": 100,
                "tool_choice": "auto",
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "get_current_weather",
                            "parameters": {"type": "object", "properties": {"location": {"type": "string"}}},
                        },
                    }
                ],
            },
            "response": _COMPLETION,
        }

    def test_chat_completions_rejects_stream(self, toolkit_config: ToolkitClientConfig) -> None:
        api = OpenAIAPI(HTTPClient(toolkit_config))
        request = ChatCompletionRequest(messages=[OpenAIMessage(role="user", content="Hi")], stream=True)

        with pytest.raises(ValueError, match="chat_completion_stream"):
            api.chat_completions(request)

    def test_chat_completion_stream(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = OpenAIAPI(HTTPClient(toolkit_config))
        base = {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1715000000, "model": "azure/gpt-4.1"}
        chunks = [
            {**base, "choices": [{"index": 0, "delta": {"role": "assistant", "content": "Hel"}}]},
            {**base, "choices": [{"index": 0, "delta": {"content": "lo"}, "finish_reason": "stop"}]},
        ]
        route = respx_mock.post(api._make_url("/ai/openai/chat/completions")).mock(
            return_value=httpx2.Response(
                200,
                text=": keep-alive\n\n"
                + "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks)
                + "data: [DONE]\n\n",
                headers={"content-type": "text/event-stream"},
            )
        )

        responses = list(
            api.chat_completion_stream(ChatCompletionRequest(messages=[OpenAIMessage(role="user", content="Hi")]))
        )

        assert {
            "accept": route.calls[0].request.headers["accept"],
            "body": json.loads(route.calls[0].request.content),
            "chunks": [response.dump() for response in responses],
        } == {
            "accept": "text/event-stream",
            "body": {"messages": [{"role": "user", "content": "Hi"}], "stream": True},
            "chunks": chunks,
        }

    def test_chat_completion_stream_raises_on_error(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        api = OpenAIAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/ai/openai/chat/completions")).mock(
            return_value=httpx2.Response(400, json={"error": {"code": 400, "message": "Unknown model"}})
        )
        request = ChatCompletionRequest(messages=[OpenAIMessage(role="user", content="Hi")], model="unknown")

        with pytest.raises(ToolkitAPIError, match="Unknown model"):
            list(api.chat_completion_stream(request))

    def test_json_schema_response_format_requires_schema(self) -> None:
        with pytest.raises(ValueError, match="json_schema"):
            ChatCompletionRequest.model_validate(
                {"messages": [{"role": "user", "content": "Hi"}], "response_format": {"type": "json_schema"}}
            )

    def test_list_models(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = OpenAIAPI(HTTPClient(toolkit_config))
        respx_mock.get(api._make_url("/ai/openai/models")).mock(
            return_value=httpx2.Response(status_code=200, json={"object": "list", "data": [_MODEL]})
        )

        models = api.list_models()

        assert {
            "version": respx_mock.calls[0].request.headers["cdf-version"],
            "models": [model.dump() for model in models],
        } == {"version": "beta", "models": [_MODEL]}
