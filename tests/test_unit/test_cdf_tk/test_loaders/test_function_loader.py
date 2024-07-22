from unittest.mock import MagicMock

from cognite.client.data_classes import FunctionWrite

from cognite_toolkit._cdf_tk.loaders import FunctionLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalCogniteClient


class TestFunctionLoader:
    def test_load_functions(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = cognite_client_approval.mock_client

        loader = FunctionLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(LOAD_DATA / "functions" / "1.my_functions.yaml", cdf_tool, skip_validation=False)
        assert len(loaded) == 2

    def test_load_function(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = cognite_client_approval.mock_client

        loader = FunctionLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource(LOAD_DATA / "functions" / "1.my_function.yaml", cdf_tool, skip_validation=False)
        assert isinstance(loaded, FunctionWrite)
