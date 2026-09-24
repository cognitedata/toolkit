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


@pytest.mark.parametrize(
    "target",
    [
        pytest.param({"targetTimeseriesExternalId": "output-ts"}, id="external-id"),
        pytest.param(
            {"targetTimeseriesInstanceId": {"space": "plant", "externalId": "output-ts"}},
            id="instance-id",
        ),
    ],
)
def test_scheduled_calculation_update_omits_immutable_fields(target: dict[str, object]) -> None:
    resource = get_example_minimum_responses(ChartScheduledCalculationResponse)
    resource.pop("targetTimeseriesExternalId", None)
    resource.pop("targetTimeseriesInstanceId", None)
    resource.update(target)
    resource.update(
        nonce="test-nonce", offset=0, windowSize=300000, name="updated-name", description="updated-description"
    )
    request = ChartScheduledCalculationRequest.model_validate(resource, extra="ignore")

    assert request.as_update("replace") == {
        "externalId": request.external_id,
        "name": "updated-name",
        "description": "updated-description",
        "graph": request.graph.model_dump(exclude_none=True, by_alias=True),
    }
    create_payload = request.dump()
    for field, value in target.items():
        assert create_payload[field] == value
