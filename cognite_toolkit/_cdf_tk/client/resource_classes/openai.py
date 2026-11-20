"""Request and response classes for the OpenAI-compatible endpoints.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Chat-Completions/operation/chat_completions_ai_openai_chat_completions_post
https://api-docs.cognite.com/20230101-beta/tag/Models/operation/list_models_ai_openai_models_get

Note: These are alpha and beta APIs and may change in future releases.
"""

from typing import Annotated, Literal, TypeAlias

from pydantic import ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class OpenAIObject(BaseModelObject):
    """The OpenAI-compatible endpoints use snake_case keys, so the camelCase alias generator is disabled."""

    model_config = ConfigDict(alias_generator=None)


class OpenAIImageURL(OpenAIObject):
    url: str


class OpenAIContentPart(OpenAIObject):
    type: Literal["text", "image_url"] | str
    text: str | None = None
    image_url: OpenAIImageURL | None = None


class OpenAIFunctionCall(OpenAIObject):
    name: str | None = None
    arguments: str | None = Field(default=None, description="The arguments of the function call, as a JSON string.")


class OpenAIToolCall(OpenAIObject):
    id: str
    type: Literal["function"] = "function"
    function: OpenAIFunctionCall


class OpenAIMessage(OpenAIObject):
    role: Literal["user", "system", "assistant", "function", "tool", "developer"] | str
    content: str | list[OpenAIContentPart] | None = None
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: list[OpenAIToolCall] | None = None


class OpenAIFunction(OpenAIObject):
    name: str = Field(min_length=1, max_length=64)
    description: str | None = None
    parameters: dict[str, JsonValue] = Field(description="The parameters the function accepts, as a JSON schema.")


class OpenAITool(OpenAIObject):
    type: Literal["function"] = "function"
    function: OpenAIFunction


class OpenAIFunctionName(OpenAIObject):
    name: str = Field(min_length=1, max_length=64)


class OpenAIToolChoice(OpenAIObject):
    type: Literal["function"] = "function"
    function: OpenAIFunctionName


class OpenAIJSONSchema(OpenAIObject):
    name: str
    description: str | None = None
    schema_: dict[str, JsonValue] = Field(alias="schema")
    strict: bool | None = None


class OpenAIResponseFormatDefinition(OpenAIObject):
    type: str


class OpenAITextResponseFormat(OpenAIResponseFormatDefinition):
    type: Literal["text"] = "text"


class OpenAIJSONObjectResponseFormat(OpenAIResponseFormatDefinition):
    type: Literal["json_object"] = "json_object"


class OpenAIJSONSchemaResponseFormat(OpenAIResponseFormatDefinition):
    type: Literal["json_schema"] = "json_schema"
    json_schema: OpenAIJSONSchema


OpenAIResponseFormat: TypeAlias = Annotated[
    OpenAITextResponseFormat | OpenAIJSONObjectResponseFormat | OpenAIJSONSchemaResponseFormat,
    Field(discriminator="type"),
]


class ChatCompletionRequest(OpenAIObject):
    """Request for the chat completions endpoint.

    Use ``chat_completions`` for a single response and ``chat_completion_stream`` for a streamed response.
    """

    messages: list[OpenAIMessage] = Field(min_length=1)
    model: str | None = Field(
        default=None, description="The model ID, as returned by list models. Uses the project default if not set."
    )
    max_completion_tokens: int | None = Field(default=None, gt=0)
    temperature: float | None = None
    response_format: OpenAIResponseFormat | None = None
    tool_choice: Literal["none", "auto", "required"] | OpenAIToolChoice | None = None
    tools: list[OpenAITool] | None = Field(default=None, min_length=1, max_length=128)
    stream: bool | None = Field(
        default=None, description="Set by chat_completion_stream. Leave unset when calling chat_completions."
    )


class OpenAIResponseMessage(OpenAIObject):
    role: Literal["assistant"] | str = "assistant"
    content: str | None = None
    tool_calls: list[OpenAIToolCall] | None = None


class OpenAIChoice(OpenAIObject):
    index: int
    message: OpenAIResponseMessage
    finish_reason: Literal["stop", "length", "tool_calls", "content_filter"] | str


class OpenAIUsage(OpenAIObject):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class ChatCompletionResponse(OpenAIObject):
    id: str
    object: Literal["chat.completion"] | str = "chat.completion"
    created: int = Field(description="Unix timestamp in seconds of when the completion was created.")
    model: str
    choices: list[OpenAIChoice]
    usage: OpenAIUsage


class OpenAIToolCallDelta(OpenAIObject):
    """A fragment of a tool call. Fragments with the same index belong to the same tool call."""

    index: int
    id: str | None = None
    type: Literal["function"] | str | None = None
    function: OpenAIFunctionCall | None = None


class OpenAIDelta(OpenAIObject):
    role: Literal["assistant"] | str | None = None
    content: str | None = None
    tool_calls: list[OpenAIToolCallDelta] | None = None


class OpenAIChunkChoice(OpenAIObject):
    index: int
    delta: OpenAIDelta
    finish_reason: Literal["stop", "length", "tool_calls", "content_filter"] | str | None = None


class ChatCompletionChunk(OpenAIObject):
    """A fragment of a streamed chat completion."""

    id: str
    object: Literal["chat.completion.chunk"] | str = "chat.completion.chunk"
    created: int = Field(description="Unix timestamp in seconds of when the completion was created.")
    model: str
    choices: list[OpenAIChunkChoice]
    usage: OpenAIUsage | None = None


class OpenAIModel(OpenAIObject):
    id: str
    object: Literal["model"] | str = "model"
    created: int = 0
    owned_by: str = "Cognite"
    context_length: int
    max_completion_tokens: int
    earliest_retirement_date: int = Field(
        description="Earliest date the model may be retired, as seconds since Unix epoch."
    )


class OpenAIModelList(OpenAIObject):
    object: Literal["list"] | str = "list"
    data: list[OpenAIModel]
