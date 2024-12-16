from collections.abc import Iterable

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes import YAMLComments


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


class TestYAMLComments:
    @pytest.mark.parametrize("yaml_str, indent", list(load_dump_test_cases()))
    def test_load_dump(self, yaml_str: str, indent: int) -> None:
        comments = YAMLComments.load(yaml_str)
        data = yaml.safe_load(yaml_str)
        dumped = yaml.safe_dump(data, sort_keys=False, indent=indent)
        assert comments.dump(dumped) == yaml_str
