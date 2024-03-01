from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import FunctionsAcl

# You can import from common.tool and get a CDFClientTool instance
# that can be used to run the function locally and verify capabilities.
from common.tool import CDFClientTool


def handle(data: dict, client: CogniteClient, secrets: dict, function_call_info: dict) -> dict:
    tool = CDFClientTool(client=client)
    # This will fail unless the function has the specified capabilities.
    tool.verify_capabilities(
        [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
        ]
    )
    print(tool)
    return {
        "data": data,
        "secrets": mask_secrets(secrets),
        "functionInfo": function_call_info,
    }


def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
