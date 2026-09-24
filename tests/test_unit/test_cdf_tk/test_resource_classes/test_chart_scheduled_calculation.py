import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    ChartScheduledCalculationRequest,
    ChartScheduledCalculationResponse,
)
from tests.test_unit.test_cdf_tk.test_client.data import get_example_minimum_responses


@pytest.mark.parametrize(
    "input_data, error_type",
    [
        pytest.param({"type": "ts", "value": "ts_001"}, "missing", id="missing-param"),
        pytest.param({"type": "ts", "value": "ts_001", "param": None}, "string_type", id="null-param"),
        pytest.param({"type": "ts", "value": "ts_001", "param": 1}, "string_type", id="non-string-param"),
    ],
)
def test_scheduled_calculation_requires_input_param(input_data: dict[str, object], error_type: str) -> None:
    resource = get_example_minimum_responses(ChartScheduledCalculationResponse)
    resource["nonce"] = "test-nonce"
    resource["graph"]["steps"][0]["inputs"] = [input_data]

    with pytest.raises(ValidationError) as exc_info:
        ChartScheduledCalculationRequest.model_validate(resource, extra="ignore")

    assert [(error["loc"], error["type"]) for error in exc_info.value.errors()] == [
        (("graph", "steps", 0, "inputs", 0, "param"), error_type)
    ]
