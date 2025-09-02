from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.workflow_version import WorkflowVersionYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_workflow_version_test_cases() -> Iterable:
    # Missing required top-level fields
    yield pytest.param(
        {},
        {
            "Missing required field: 'workflowExternalId'",
            "Missing required field: 'version'",
            "Missing required field: 'workflowDefinition'",
        },
        id="Missing all required top-level fields",
    )
    # Extra/unknown field at top level
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {"description": "desc", "tasks": []},
            "foo": 123,
        },
        {"Unused field: 'foo'"},
        id="Extra field at top level",
    )
    # Missing required fields in task definition
    yield pytest.param(
        {"workflowExternalId": "wf1", "version": "v1", "workflowDefinition": {"description": "desc", "tasks": [{}]}},
        {
            "In workflowDefinition.tasks[1] missing required field: 'externalId'",
            "In workflowDefinition.tasks[1] missing required field: 'parameters'",
            "In workflowDefinition.tasks[1] missing required field: 'type'",
        },
        id="Missing required fields in task definition",
    )
    # Wrong type for retries in task definition
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "description": "desc",
                "tasks": [{"externalId": "t1", "type": "function", "parameters": {}, "retries": "notAnInt"}],
            },
        },
        {"In workflowDefinition.tasks[1].retries input should be a valid integer. Got 'notAnInt' of type str."},
        id="Wrong type for retries",
    )
    # Invalid enum value for on_failure
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "description": "desc",
                "tasks": [{"externalId": "t1", "type": "function", "parameters": {}, "onFailure": "notAValidValue"}],
            },
        },
        {
            "In workflowDefinition.tasks[1].onFailure input should be 'abortWorkflow' or "
            "'skipTask'. Got 'notAValidValue'."
        },
        id="Invalid enum value for onFailure",
    )


class TestWorkflowVersionYAML:
    @pytest.mark.parametrize("data", list(find_resources("WorkflowVersion")))
    def test_load_valid_workflow_file(self, data: dict[str, object]) -> None:
        loaded = WorkflowVersionYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_workflow_version_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, WorkflowVersionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
