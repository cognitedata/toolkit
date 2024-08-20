from cognite.client.data_classes import FunctionWrite

from cognite_toolkit._cdf_tk.loaders import FunctionLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
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

    def test_are_equals_secret_changing(self, cdf_tool_config: CDFToolConfig) -> None: ...
