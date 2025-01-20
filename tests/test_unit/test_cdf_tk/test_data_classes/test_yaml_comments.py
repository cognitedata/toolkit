from collections.abc import Iterable

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes import YAMLComments
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


def load_dump_test_cases() -> Iterable:
    yield pytest.param("""key: value # comment""", 2, id="simple")
    yield pytest.param(
        """- item: 23 # comment
  # other comment
- item: 24""",
        2,
        id="list",
    )
    yield pytest.param(
        """first: value
second:
    key: value
    # above comment
    third:
    -   item: 23
    -   item: 24 # with comment""",
        4,
        id="nested with indent 4",
    )
    yield pytest.param(
        """- name: daily-8am-utc
  functionExternalId: fn_first_function
  description: Run every day at 8am UTC
  cronExpression: 0 8 * * *
  data:
    breakfast: 'today: peanut butter sandwich and coffee'
    lunch: 'today: greek salad and water'
    dinner: 'today: steak and red wine'
  authentication:
    # Credentials to use to run the function in this schedule.
    # In this example, we just use the main deploy credentials, so the result is the same, but use a different set of
    # credentials (env variables) if you want to run the function with different permissions.
    clientId: some_id
    clientSecret: some_secret""",
        2,
        id="realistic",
    )


class TestYAMLComments:
    @pytest.mark.parametrize("yaml_str, indent", list(load_dump_test_cases()))
    def test_load_dump(self, yaml_str: str, indent: int) -> None:
        comments = YAMLComments.load(yaml_str)
        data = yaml.safe_load(yaml_str)
        dumped = yaml_safe_dump(data, indent=indent)
        assert comments.dump(dumped) == yaml_str
