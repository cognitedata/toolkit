from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import SimulatorModelYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_simulator_model_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Model 1"},
        {
            "Missing required field: 'externalId'",
            "Missing required field: 'simulatorExternalId'",
            "Missing required field: 'dataSetExternalId'",
            "Missing required field: 'type'",
        },
        id="Missing required fields",
    )
    yield pytest.param(
        {
            "externalId": "model_1",
            "simulatorExternalId": "simulator_1",
            "dataSetExternalId": "dataset_1",
            "type": "steady_state",
        },
        {"Missing required field: 'name'"},
        id="Missing required field: name",
    )
    yield pytest.param(
        {
            "externalId": "model_1",
            "simulatorExternalId": "simulator_1",
            "name": "Model 1",
            "dataSetExternalId": "dataset_1",
            "type": "steady_state",
            "id": 123,
        },
        {"Unused field: 'id'"},
        id="Unused field: id",
    )
    yield pytest.param(
        {
            "externalId": "model_1",
            "simulatorExternalId": "simulator_1",
            "name": "Model 1",
            "dataSetExternalId": "dataset_1",
            "type": "steady_state",
            "dataSetId": 123,
        },
        {"Unused field: 'dataSetId'"},
        id="Unused field: dataSetId (should use dataSetExternalId)",
    )


class TestSimulatorModelYAML:
    @pytest.mark.parametrize("data", list(find_resources("SimulatorModel")))
    def test_load_valid_simulator_model(self, data: dict[str, object]) -> None:
        loaded = SimulatorModelYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_simulator_model_test_cases()))
    def test_invalid_simulator_model_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SimulatorModelYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
