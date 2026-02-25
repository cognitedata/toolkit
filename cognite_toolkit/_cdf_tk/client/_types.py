from typing import Annotated, TypeAlias

from pydantic import BeforeValidator


def _keys_and_values_as_string(value: dict) -> dict[str, str]:
    return {str(k): str(v) for k, v in value.items()}


Metadata: TypeAlias = Annotated[dict[str, str], BeforeValidator(_keys_and_values_as_string)]
