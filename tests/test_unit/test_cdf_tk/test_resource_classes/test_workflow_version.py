from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest

from cognite_toolkit._cdf_tk.resource_classes.workflow_version import Task, TaskDefinition, WorkflowVersionYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_workflow_version_test_cases() -> Iterable:
    yield pytest.param(
        {},
        {
            "Missing required field: 'workflowExternalId'",
            "Missing required field: 'version'",
            "Missing required field: 'workflowDefinition'",
        },
        id="Missing all required top-level fields",
    )
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {"description": "desc", "tasks": []},
            "foo": 123,
        },
        {"Unknown field: 'foo'"},
        id="Extra field at top level",
    )
    yield pytest.param(
        {"workflowExternalId": "wf1", "version": "v1", "workflowDefinition": {"description": "desc", "tasks": [{}]}},
        # Note that when we do not have 'type', we cannot determine which task type it is, so we only get the error about missing 'type'.
        {
            "In workflowDefinition.tasks[1] missing required field: 'type'",
        },
        id="Missing required type in task definition",
    )
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "description": "desc",
                "tasks": [{"externalId": "t1", "type": "function", "parameters": {}, "retries": "notAnInt"}],
            },
        },
        {
            "In workflowDefinition.tasks[1].function.parameters missing required field: 'function'",
            "In workflowDefinition.tasks[1].function.retries input should be a valid "
            "integer. Got 'notAnInt' of type str.",
        },
        id="Wrong type for retries",
    )
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
            "In workflowDefinition.tasks[1].function.onFailure input should be "
            "'abortWorkflow' or 'skipTask'. Got 'notAValidValue'.",
            "In workflowDefinition.tasks[1].function.parameters missing required field: 'function'",
        },
        id="Invalid enum value for onFailure",
    )
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "description": "desc",
                "tasks": [{"externalId": "t1", "type": "unknownType", "parameters": {}}],
            },
        },
        {
            "In workflowDefinition.tasks[1] input tag 'unknownType' found using 'type' "
            "does not match any of the expected tags: 'function', 'transformation', "
            "'cdfRequest', 'dynamic', 'subworkflow', 'simulation', 'functionApp'"
        },
        id="Invalid task type",
    )


def valid_subworkflow_test_cases() -> Iterable:
    """Test cases for CDF-26812: subworkflow reference validation."""
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "subworkflowTask",
                        "type": "subworkflow",
                        "parameters": {
                            "subworkflow": {
                                "workflowExternalId": "mySubWorkflow",
                                "version": "v1",
                            }
                        },
                    }
                ]
            },
        },
        id="Subworkflow task with external workflow reference",
    )
    yield pytest.param(
        {
            "workflowExternalId": "wf1",
            "version": "v1",
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "subworkflowTask",
                        "type": "subworkflow",
                        "parameters": {
                            "subworkflow": {
                                "tasks": [
                                    {
                                        "externalId": "nestedTask1",
                                        "type": "transformation",
                                        "parameters": {
                                            "transformation": {"externalId": "myTransformation"},
                                        },
                                    }
                                ]
                            }
                        },
                    }
                ]
            },
        },
        id="Subworkflow task with inline tasks",
    )


class TestWorkflowVersionYAML:
    @pytest.mark.parametrize("data", list(find_resources("WorkflowVersion")))
    def test_load_valid_workflow_file(self, data: dict[str, object]) -> None:
        loaded = WorkflowVersionYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_workflow_version_test_cases()))
    def test_invalid_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, WorkflowVersionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    @pytest.mark.parametrize("data", list(valid_subworkflow_test_cases()))
    def test_valid_subworkflow_formats(self, data: dict[str, object]) -> None:
        """Test that both subworkflow reference and inline tasks formats are valid (CDF-26812)."""
        warning_list = validate_resource_yaml_pydantic(data, WorkflowVersionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 0, f"Expected no warnings, got: {warning_list}"

        loaded = WorkflowVersionYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_tasks_are_in_union(self) -> None:
        all_tasks = get_concrete_subclasses(TaskDefinition)
        all_union_tasks = get_args(Task.__args__[0])
        missing = set(all_tasks) - set(all_union_tasks)
        assert not missing, (
            f"The following TaskDefinition subclasses are "
            f"missing from the Tasks union: {humanize_collection([cls.__name__ for cls in missing])}"
        )
