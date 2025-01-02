from cognite.client import CogniteClient
from other_module import to_camel, to_pascal, to_snake


def handle(data: dict, client: CogniteClient, function_call_info: dict) -> dict:
    """Convert string to different cases."""
    output: dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, str):
            output[f"{key}_camel"] = to_camel(value)
            output[f"{key}_snake"] = to_snake(value)
            output[f"{key}_pascal"] = to_pascal(value)
    return output
