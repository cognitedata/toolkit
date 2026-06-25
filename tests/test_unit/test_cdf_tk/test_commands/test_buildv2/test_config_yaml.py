from datetime import date, datetime

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._config import ConfigYAML


class TestConfigYAML:
    def test_from_yaml_accepts_unquoted_datetime_variables(self) -> None:
        config = ConfigYAML.from_yaml(
            """\
environment:
  project: test-project
variables:
  modules:
    custom:
      2024-date: 2024-01-01 00:00:00
      2025-date: 2025-01-01 00:00:00
"""
        )

        assert config.variables == {
            "modules": {
                "custom": {
                    "2024-date": "2024-01-01 00:00:00",
                    "2025-date": "2025-01-01 00:00:00",
                }
            }
        }

    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(datetime(2024, 1, 1, 0, 0, 0), "2024-01-01 00:00:00", id="datetime"),
            pytest.param(date(2024, 1, 1), "2024-01-01", id="date"),
        ],
    )
    def test_normalizes_date_and_datetime_in_nested_variables(self, value: date | datetime, expected: str) -> None:
        config = ConfigYAML.model_validate({"variables": {"modules": {"custom": {"timestamp": value}}}})

        assert config.variables == {"modules": {"custom": {"timestamp": expected}}}

    def test_leaves_quoted_datetime_strings_unchanged(self) -> None:
        config = ConfigYAML.model_validate({"variables": {"modules": {"custom": {"2024-date": "2024-01-01 00:00:00"}}}})

        assert config.variables == {"modules": {"custom": {"2024-date": "2024-01-01 00:00:00"}}}
