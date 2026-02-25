from typing import Annotated, Any, TypeAlias

from pydantic import BeforeValidator


def _keys_and_values_as_string(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    return {str(k): str(v) for k, v in value.items()}


Metadata: TypeAlias = Annotated[dict[str, str], BeforeValidator(_keys_and_values_as_string)]
