from cognite.client import CogniteClient


def handle(client: CogniteClient, data: dict, secrets: dict, info: dict) -> dict:
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
