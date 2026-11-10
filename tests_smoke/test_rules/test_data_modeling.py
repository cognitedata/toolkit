from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.rules._data_modeling import DataModelingLimits


def test_data_modeling_limits(toolkit_client: ToolkitClient) -> None:
    limit_responses = toolkit_client.limits.list(prefix="data_modeling.")
    try:
        _ = DataModelingLimits.from_responses(limit_responses)
    except ValidationError as e:
        raise AssertionError(
            f"DataModelingLimits validation failed. Please check the retrieved limit values and ensure they match the expected structure. {e}"
        )
