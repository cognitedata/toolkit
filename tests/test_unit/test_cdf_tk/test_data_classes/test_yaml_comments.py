import textwrap
from collections.abc import Iterable

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes import YAMLComments


def load_dump_test_cases() -> Iterable:
    yield pytest.param("""key: value # comment""", id="simple")
    yield pytest.param(
        """- item: 23 # comment
  # comment
  item: 24""",
        id="list",
    )
    yield pytest.param(
        textwrap.dedent("""
            first: value
            second:
                key: value
                # comment
                third: value
        """),
        id="nested with indent 4",
    )


class TestYAMLComments:
    @pytest.mark.parametrize("yaml_str", list(load_dump_test_cases()))
    def test_load_dump(self, yaml_str: str) -> None:
        comments = YAMLComments.load(yaml_str)
        data = yaml.safe_load(yaml_str)
        dumped = yaml.safe_dump(data, sort_keys=False)
        assert comments.dump(dumped) == yaml_str
