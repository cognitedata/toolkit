from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.workflow import WorkflowYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_workflow_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "MyWorkflow"},
        {"Missing required field: 'externalId'", "Unused field: 'name'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {"externalId": "MyWorkflow", "description": "way too long" * 500},
        {"In field description string should have at most 500 characters"},
        id="Too long description",
    )


class TestWorkflowYAML:
    @pytest.mark.parametrize("data", list(find_resources("Workflow")))
    def test_load_valid_workflow_file(self, data: dict[str, object]) -> None:
        loaded = WorkflowYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_workflow_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, WorkflowYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
