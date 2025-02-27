from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
