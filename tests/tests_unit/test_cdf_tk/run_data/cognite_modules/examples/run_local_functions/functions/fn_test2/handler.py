from cognite.client import CogniteClient

# You can import from common.tool and get a CDFClientTool instance
# that can be used to run the function locally and verify capabilities.
from common.tool import CDFClientTool


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    tool = CDFClientTool(client=client)
    # This will fail unless the function has the specified capabilities.
    print("Print statements will be shown in the logs.")
    print("Running with the following configuration:\n")
    print(tool)
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
