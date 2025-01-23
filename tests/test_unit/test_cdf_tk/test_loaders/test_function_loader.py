from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes import Function, FunctionWrite

from cognite_toolkit._cdf_tk.loaders import FunctionLoader, ResourceWorker
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, calculate_directory_hash, calculate_secure_hash
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestFunctionLoader:
    def test_load_functions(self, cdf_tool_mock: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_mock, LOAD_DATA)

        raw_list = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_functions.yaml", cdf_tool_mock.environment_variables()
        )

        assert len(raw_list) == 2

    def test_load_function(self, cdf_tool_mock: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_mock, LOAD_DATA)

        raw_list = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_function.yaml", cdf_tool_mock.environment_variables()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, FunctionWrite)

    def test_update_secrets(
        self, cdf_tool_mock: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        local_yaml = """name: my_function
externalId: my_function
secrets:
    secret1: value1
    secret2: value2
        """
        cdf_function = Function(
            name="my_function",
            external_id="my_function",
            file_id=123,
            status="Ready",
            metadata={
                FunctionLoader._MetadataKey.function_hash: calculate_directory_hash(tmp_path / "my_function"),
                FunctionLoader._MetadataKey.secret_hash: calculate_secure_hash(
                    {
                        "secret1": "value1",
                        "secret2": "value2",
                    }
                ),
            },
            secrets={
                # The API returns secrets masked
                "secret1": "***",
                "secret2": "***",
            },
        )
        toolkit_client_approval.append(Function, cdf_function)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_yaml
        filepath.parent.name = FunctionLoader.folder_name

        worker = ResourceWorker(FunctionLoader.create_loader(cdf_tool_mock, tmp_path))
        to_create, to_update, to_delete, unchanged, _ = worker.load_resources([filepath])

        assert {
            "create": len(to_create),
            "update": len(to_update),
            "delete": len(to_delete),
            "unchanged": len(unchanged),
        } == {"create": 0, "update": 0, "delete": 0, "unchanged": 1}

        toolkit_client_approval.clear_cdf_resources(Function)
        cdf_function.metadata[FunctionLoader._MetadataKey.secret_hash] = calculate_secure_hash(
            {
                "secret1": "value1",
                "secret2": "updated_value2",
            }
        )
        toolkit_client_approval.append(Function, cdf_function)
        to_create, to_update, to_delete, unchanged, _ = worker.load_resources([filepath])

        assert {
            "create": len(to_create),
            "update": len(to_update),
            "delete": len(to_delete),
            "unchanged": len(unchanged),
        } == {"create": 1, "update": 0, "delete": 1, "unchanged": 0}

    def test_dump_index_url_set(self, cdf_tool_mock: CDFToolConfig, tmp_path: Path) -> None:
        local_dict = FunctionWrite(
            name="my_function",
            file_id=123,
            external_id="my_function",
            index_url="http://my-index-url",
        ).dump()
        cdf_function = Function(
            name="my_function",
            file_id=123,
            external_id="my_function",
            metadata={
                FunctionLoader._MetadataKey.function_hash: calculate_directory_hash(tmp_path / "my_function"),
            },
        )
        loader = FunctionLoader.create_loader(cdf_tool_mock, tmp_path)

        dumped = loader.dump_resource(cdf_function, local_dict)

        assert "indexUrl" in dumped
        assert dumped["indexUrl"] == "http://my-index-url"
