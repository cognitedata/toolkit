from pathlib import Path

from cognite.client.data_classes import Function, FunctionWrite

from cognite_toolkit._cdf_tk.loaders import FunctionLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, calculate_directory_hash, calculate_secure_hash
from tests.data import LOAD_DATA


class TestFunctionLoader:
    def test_load_functions(self, cdf_tool_config: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_config, None)

        loaded = loader.load_resource(
            LOAD_DATA / "functions" / "1.my_functions.yaml", cdf_tool_config, skip_validation=False
        )

        assert len(loaded) == 2

    def test_load_function(self, cdf_tool_config: CDFToolConfig) -> None:
        loader = FunctionLoader.create_loader(cdf_tool_config, None)

        loaded = loader.load_resource(
            LOAD_DATA / "functions" / "1.my_function.yaml", cdf_tool_config, skip_validation=False
        )

        assert isinstance(loaded, FunctionWrite)

    def test_are_equals_secret_changing(self, cdf_tool_config: CDFToolConfig, tmp_path: Path) -> None:
        local_function = FunctionWrite(
            name="my_function",
            file_id=123,
            external_id="my_function",
            metadata={FunctionLoader._MetadataKey.function_hash: calculate_directory_hash(tmp_path / "my_function")},
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
                FunctionLoader._MetadataKey.function_hash: "hash1",
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
        loader = FunctionLoader.create_loader(cdf_tool_config, tmp_path)

        _, local_dumped, cdf_dumped = loader.are_equal(local_function, cdf_function, return_dumped=True)

        assert local_dumped["secrets"] != cdf_dumped["secrets"]
        local_dumped.pop("secrets")
        cdf_dumped.pop("secrets")
        assert local_dumped == cdf_dumped
