from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    # This will fail unless the function has the specified capabilities.
    print("Print statements will be shown in the logs.")
    print("Running with the following configuration:\n")
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
