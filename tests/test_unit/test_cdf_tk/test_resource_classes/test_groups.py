from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import GroupYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import read_yaml_content
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
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
            "In metadata the key 'groupNo' should be a valid string. Got 83 of type int. Hint: Use double quotes to force string.",
            "In metadata the key 'governed' should be a valid string. Got True of type bool. Hint: Use double quotes to force string.",
            "In metadata the key 'maxUsers' should be a valid string. Got inf of type float. Hint: Use double quotes to force string.",
            "In capabilities[1].scope.dbsToTables input should be a valid list. Got {}.",
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
        {"In item [2].capabilities[1].actions input should be 'READ' or 'WRITE'. Got 'WRITE-KING'."},
        id="Error in second group",
    )


class TestTimeSeriesTK:
    @pytest.mark.parametrize("data", list(find_resources("Group")))
    def test_load_valid_timeseries(self, data: dict[str, object]) -> None:
        loaded = GroupYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("content, expected_errors", list(invalid_group_test_cases()))
    def test_invalid_group_error_messages(self, content: str, expected_errors: set[str]) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        data = read_yaml_content(content)

        warning_list = validate_resource_yaml_pydantic(data, GroupYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
