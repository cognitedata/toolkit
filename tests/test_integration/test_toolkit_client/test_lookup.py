import pytest
from cognite.client.data_classes import Function

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ResourceRetrievalError


class TestLookupFunctionsIds:
    def test_lookup_functions_id(self, toolkit_client: ToolkitClient, dummy_function: Function) -> None:
        function_id = toolkit_client.lookup.functions.id(dummy_function.external_id)

        assert function_id == dummy_function.id

    def test_lookup_functions_external_id(self, toolkit_client: ToolkitClient, dummy_function: Function) -> None:
        external_id = toolkit_client.lookup.functions.external_id(dummy_function.id)

        assert external_id == dummy_function.external_id

    def test_lookup_functions_external_id_not_found(self, toolkit_client: ToolkitClient) -> None:
        result = toolkit_client.lookup.functions.external_id(999999)

        assert result is None

    def test_lookup_functions_id_not_found_raise(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ResourceRetrievalError) as e:
            _ = toolkit_client.lookup.functions.id("non_existent_function")

        assert "Failed to retrieve Function with external_id ['non_existent_function']" in str(e.value)

    def test_lookup_functions_id_not_found(self, toolkit_client: ToolkitClient) -> None:
        result = toolkit_client.lookup.functions.id("", allow_empty=True)

        assert result == 0
