from pathlib import Path

from cognite.client.data_classes import Function, FunctionWrite

from cognite_toolkit._cdf_tk.loaders import FunctionLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, calculate_directory_hash, calculate_secure_hash
from tests.data import LOAD_DATA


class TestFunctionLoader:
    def test_load_functions(self, cdf_tool_mock: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_mock, None)

        loaded = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_functions.yaml",
            cdf_tool_mock,
        )

        assert len(loaded) == 2

    def test_load_function(self, cdf_tool_mock: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_mock, None)

        loaded = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_function.yaml",
            cdf_tool_mock,
        )

        assert isinstance(loaded, FunctionWrite)

    def test_are_equals_secret_changing(self, cdf_tool_mock: CDFToolConfig, tmp_path: Path) -> None:
        local_function = FunctionWrite(
            name="my_function",
            file_id=123,
            external_id="my_function",
            secrets={
                "secret1": "value1",
                "secret2": "value2",
            },
        )
        cdf_function = Function(
            name="my_function",
            file_id=123,
            external_id="my_function",
            metadata={
                FunctionLoader._MetadataKey.function_hash: calculate_directory_hash(tmp_path / "my_function"),
                FunctionLoader._MetadataKey.secret_hash: calculate_secure_hash(
                    {
                        "secret1": "value1",
                        "secret2": "updated_value2",
                    }
                ),
            },
            secrets={
                # The API returns secrets masked
                "secret1": "***",
                "secret2": "***",
            },
        )
        loader = FunctionLoader.create_loader(cdf_tool_mock, tmp_path)

        _, local_dumped, cdf_dumped = loader.are_equal(local_function, cdf_function, return_dumped=True)

        assert (
            local_dumped["metadata"][FunctionLoader._MetadataKey.secret_hash]
            != cdf_dumped["metadata"][FunctionLoader._MetadataKey.secret_hash]
        )
        local_dumped["metadata"].pop(FunctionLoader._MetadataKey.secret_hash)
        cdf_dumped["metadata"].pop(FunctionLoader._MetadataKey.secret_hash)
        assert local_dumped == cdf_dumped
        assert local_function.secrets == {
            "secret1": "value1",
            "secret2": "value2",
        }, "Original object should not be modified"

    def test_are_equals_index_url_set(self, cdf_tool_mock: CDFToolConfig, tmp_path: Path) -> None:
        local_function = FunctionWrite(
            name="my_function",
            file_id=123,
            external_id="my_function",
            index_url="http://my-index-url",
        )
        cdf_function = Function(
            name="my_function",
            file_id=123,
            external_id="my_function",
            metadata={
                FunctionLoader._MetadataKey.function_hash: calculate_directory_hash(tmp_path / "my_function"),
            },
        )
        loader = FunctionLoader.create_loader(cdf_tool_mock, tmp_path)

        are_equal = loader.are_equal(local_function, cdf_function, return_dumped=False)

        assert are_equal is True
