from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_content
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import GroupYAML
from cognite_toolkit._cdf_tk.yaml_classes.base import validate_ignoring_unknown_fields
from cognite_toolkit._cdf_tk.yaml_classes.groups import CDFGroupYAML
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


class TestTimeSeriesTK:
    @pytest.mark.parametrize("data", list(find_resources("Group")))
    def test_load_valid_timeseries(self, data: dict[str, object]) -> None:
        loaded = GroupYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_load_group_with_attributes(self) -> None:
        data = {
            "name": "group-with-app-ids",
            "sourceId": "1234567890123456789",
            "attributes": {
                "token": {
                    "appIds": ["my-app-id"],
                },
            },
        }
        loaded = GroupYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_unknown_field_in_capability_scope_is_ignored(self) -> None:
        """The unknown field sits in a capability scope, two class selections below the group itself."""
        data = {
            "name": "my-group",
            "members": ["my-user"],
            "capabilities": [
                {"timeSeriesAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["1"], "unknown": "x"}}}}
            ],
        }

        loaded = validate_ignoring_unknown_fields(GroupYAML, data)

        assert isinstance(loaded, CDFGroupYAML)
        assert loaded.as_id().name == "my-group"

    @pytest.mark.parametrize("content, expected_errors", list(invalid_group_test_cases()))
    def test_invalid_group_error_messages(self, content: str, expected_errors: set[str]) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        data = read_yaml_content(content)

        warning_list = validate_resource_yaml_pydantic(data, GroupYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
