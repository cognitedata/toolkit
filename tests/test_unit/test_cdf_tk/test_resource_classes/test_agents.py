from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_classes.agent import AgentYAML
from cognite_toolkit._cdf_tk.resource_classes.agent_tools import (
    AgentInstanceSpaces,
    AgentInstanceSpacesDefinition,
    AgentTool,
    AgentToolDefinition,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {
            "externalId": "my_space",
        },
        {"Missing required field: 'name'"},
        id="missing-required-field",
    )


class TestAgentYAML:
    @pytest.mark.parametrize("data", list(find_resources("Agent", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid(self, data: dict[str, object]) -> None:
        loaded = AgentYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_model_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    def test_tools_are_in_union(self) -> None:
        all_agent_tools = get_concrete_subclasses(AgentToolDefinition)
        all_union_agent_tools = get_args(AgentTool.__args__[0])
        missing = set(all_agent_tools) - set(all_union_agent_tools)
        assert not missing, (
            f"The following AgentTools subclasses are "
            f"missing from the AgentTool union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_instance_spaces_are_in_union(self) -> None:
        all_instance_spaces = get_concrete_subclasses(AgentInstanceSpacesDefinition)
        all_union_instance_spaces = get_args(AgentInstanceSpaces.__args__[0])
        missing = set(all_instance_spaces) - set(all_union_instance_spaces)
        assert not missing, (
            f"The following AgentInstanceSpaces subclasses are "
            f"missing from the AgentInstanceSpaces union: {humanize_collection([cls.__name__ for cls in missing])}"
        )
