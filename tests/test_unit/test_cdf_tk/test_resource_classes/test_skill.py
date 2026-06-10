from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.skill import SkillIO
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.skill import SkillYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {
            "name": "toolkit-demo-skill",
            "description": "Skill without external id",
        },
        {"Missing required field: 'externalId'"},
        id="missing-external-id",
    )
    yield pytest.param(
        {
            "externalId": "toolkit_demo_skill",
            "name": "Invalid Skill Name",
            "description": "Skill with invalid name pattern",
            "content": "not markdown frontmatter format",
        },
        {
            "In field name string should match pattern '^[a-z0-9]+(?:-[a-z0-9]+)*$'",
            "In field content string should match pattern '^(?:\\ufeff)?---\\s*\\n([\\s\\S]*?)\\n---\\s*\\n([\\s\\S]*\\S[\\s\\S]*)$'",
        },
        id="name-and-content-pattern-errors",
    )
    yield pytest.param(
        {
            "externalId": "",
            "name": "",
            "description": "",
        },
        {
            "In field externalId string should have at least 1 character",
            "In field name string should have at least 1 character",
            "In field description string should have at least 1 character",
        },
        id="empty-strings",
    )


class TestSkillYAML:
    def test_alpha_skill_files_have_valid_yaml_structure(self) -> None:

        path = next(path for path in (COMPLETE_ORG_ALPHA_FLAGS / MODULES).rglob("*Skill.yaml"))
        data = SkillIO(ToolkitClientMock(), None).load_resource_file(path)[0]
        loaded = SkillYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_model_error_messages(self, data: dict, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SkillYAML, Path("some_skill.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
