import time

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataWrite, Function, FunctionWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient


@pytest.fixture()
def function_code_file_id(toolkit_client: ToolkitClient) -> int:
    """
    This fixture uploads a simple function code to CDF and returns the file ID.
    The function code is a simple Python function that prints the input data and function call info.
    """

    def handle(client: CogniteClient, data: dict, function_call_info: dict) -> dict:
        """
        [requirements]
        cognite-sdk>=7.78.0
        [/requirements]
        """
        print("Print statements will be shown in the logs.")
        print("Running with the following configuration:\n")
        return {
            "data": data,
            "functionInfo": function_call_info,
            "project": client.config.project,
        }

    name = "test_create_function_429"
    file = FileMetadataWrite(name=name, external_id="test_create_function_429")
    retrieved = toolkit_client.files.retrieve(external_id=file.external_id)
    if retrieved:
        return retrieved.id

    file_id = toolkit_client.functions._zip_and_upload_handle(handle, name=name, external_id=file.external_id)
    # Wait for the file to be available
    time.sleep(10)
    return file_id


class TestExtendedFunctions:
    def test_create_with_429_retry(self, toolkit_client: ToolkitClient, function_code_file_id: int) -> None:
        my_function = FunctionWrite(
            name="Test Function with 429 Retry",
            external_id="test_function_retry_429",
            file_id=function_code_file_id,
        )
        created: Function | None = None
        try:
            created = toolkit_client.functions.create_with_429_retry(my_function)
            assert created is not None
        finally:
            if created:
                toolkit_client.functions.delete(id=created.id)
