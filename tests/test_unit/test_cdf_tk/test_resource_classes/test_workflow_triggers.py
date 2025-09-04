from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.workflow_trigger import WorkflowTriggerYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_workflow_trigger_test_cases() -> Iterable:
    # Missing required field: externalId, extra field 'name'
    yield pytest.param(
        {"name": "MyWorkflowTrigger"},
        {
            "Missing required field: 'authentication'",
            "Missing required field: 'externalId'",
            "Missing required field: 'triggerRule'",
            "Missing required field: 'workflowExternalId'",
            "Missing required field: 'workflowVersion'",
            "Unused field: 'name'",
        },
        id="Missing required fields ",
    )
    # Extra/unknown field
    yield pytest.param(
        {
            "externalId": "abc",
            "triggerRule": {},
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {},
            "foo": 123,
        },
        {
            "In authentication missing required field: 'clientId'",
            "In authentication missing required field: 'clientSecret'",
            "In field triggerRule invalid trigger rule data missing 'triggerType' key",
            "Unused field: 'foo'",
        },
        id="Extra field and missing triggerType in triggerRule",
    )
    # Invalid trigger type
    yield pytest.param(
        {
            "externalId": "abc",
            "triggerRule": {"triggerType": "notAType"},
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
        {"In field triggerRule invalid trigger type 'notAType'. Expected one of dataModeling or schedule"},
        id="Invalid triggerType value",
    )
    # Missing required triggerRule fields (e.g. for schedule)
    yield pytest.param(
        {
            "externalId": "abc",
            "triggerRule": {"triggerType": "schedule"},
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
        {"In triggerRule missing required field: 'cronExpression'"},
        id="Missing required field in schedule triggerRule",
    )
    # Wrong type for batch_size in dataModeling trigger
    yield pytest.param(
        {
            "externalId": "abc",
            "triggerRule": {
                "triggerType": "dataModeling",
                "dataModelingQuery": {},
                "batchSize": "notAnInt",
                "batchTimeout": 100,
            },
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
        {"In triggerRule.batchSize input should be a valid integer, unable to parse string as an integer"},
        id="Wrong type for batchSize in dataModeling trigger",
    )
    # Invalid value for batch_timeout (too low)
    yield pytest.param(
        {
            "externalId": "abc",
            "triggerRule": {
                "triggerType": "dataModeling",
                "dataModelingQuery": {},
                "batchSize": 100,
                "batchTimeout": 10,
            },
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
        {"In triggerRule.batchTimeout input should be greater than or equal to 60"},
        id="Invalid value for batchTimeout in dataModeling trigger",
    )
    # Invalid Json input
    yield pytest.param(
        {
            "externalId": "abc",
            "input": {"not_json": datetime(2025, 9, 1)},
            "triggerRule": {"triggerType": "schedule", "cronExpression": "* * * * *"},
            "workflowExternalId": "wf",
            "workflowVersion": "v1",
            "authentication": {"clientId": "id", "clientSecret": "secret"},
        },
        {"In input.dict.not_json input was not a valid JSON value"},
        id="Invalid Json input in dataModelingQuery",
    )


class TestWorkflowYAML:
    @pytest.mark.parametrize("data", list(find_resources("WorkflowTrigger")))
    def test_load_valid_workflow_file(self, data: dict[str, object]) -> None:
        loaded = WorkflowTriggerYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert "authentication" in dumped
        # Secret is not dumped as per design, so we add it back for comparison
        dumped["authentication"]["clientSecret"] = data["authentication"]["clientSecret"]
        assert dumped == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_workflow_trigger_test_cases()))
    def test_invalid_workflow_trigger_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, WorkflowTriggerYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
