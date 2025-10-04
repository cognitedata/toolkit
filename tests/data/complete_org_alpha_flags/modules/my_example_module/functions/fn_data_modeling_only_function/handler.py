from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    print("Function 'fn_data_modeling_only_function' called with data:", data)
    return {
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
