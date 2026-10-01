from collections.abc import Iterable
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_content
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import GroupYAML
from tests.test_unit.utils import find_resources


def invalid_group_test_cases() -> Iterable:
    yield pytest.param(
        """name: gp_table_scoped_complete_org
sourceId: '1234567890123456789'
metadata:
  origin: cognite-toolkit
  governed: true
  groupNo: 0123
  maxUsers: .inf
  description: foo:bar
capabilities:
- rawAcl:
    actions:
    - READ
    - WRITE
    scope:
      tableScope:
        dbsToTables:
          db_complete_org: {}
""",
        {
            "Invalid value at capabilities[1].scope.dbsToTables.db_complete_org: Input should be a valid list. Got {}.",
        },
        id="Naughty metadata and non-standard rawAcl tableScope",
    )

    yield pytest.param(
        """- name: group1
  sourceId: '1234567890123456789'
  capabilities:
  - labelsAcl:
     actions:
     - READ
     scope:
       all: {}
- name: group2
  sourceId: '1234567890123456789'
  capabilities:
  - labelsAcl:
     actions:
     - WRITE-KING
     scope:
       all: {}
""",
        {
            "Unrecognized value for item [2].capabilities[1].actions[1]: Expected one of 'READ' or 'WRITE'. "
            "Got 'WRITE-KING'."
        },
        id="Error in second group",
    )
    yield pytest.param(
        """name: group1
sourceId: '1234567890123456789'
members: allUserAccounts
""",
        {"Invalid group definition: Cannot have both 'sourceId' and 'members'. Please specify only one."},
        id="Both sourceId and members present",
    )


@pytest.fixture(scope="session")
def group_adapter() -> TypeAdapter[GroupYAML]:
    return TypeAdapter(GroupYAML)


class TestGroupYAML:
    @pytest.mark.parametrize("data", list(find_resources("Group")))
    def test_load_valid_groups(self, data: dict[str, object], group_adapter: TypeAdapter[GroupYAML]) -> None:
        loaded = group_adapter.validate_python(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_load_group_with_attributes(self, group_adapter: TypeAdapter[GroupYAML]) -> None:
        data = {
            "name": "group-with-app-ids",
            "sourceId": "1234567890123456789",
            "attributes": {
                "token": {
                    "appIds": ["my-app-id"],
                },
            },
        }
        loaded = group_adapter.validate_python(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("content, expected_errors", list(invalid_group_test_cases()))
    def test_invalid_group_error_messages(
        self, content: str, expected_errors: set[str], group_adapter: TypeAdapter[GroupYAML]
    ) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        data = read_yaml_content(content)

        warning_list = validate_resource_yaml_pydantic(data, GroupYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
