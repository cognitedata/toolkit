from datetime import date, datetime
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._config import ConfigYAML
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


@pytest.fixture
def config_dependencies_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    original = FeatureFlag.is_enabled
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.CONFIG_DEPENDENCIES or original(flag))


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

    def test_dependencies_must_be_yaml_files(self) -> None:
        with pytest.raises(ValueError, match=r"Dependencies must be YAML files"):
            ConfigYAML.model_validate({"dependencies": ["shared.toml"]})


@pytest.mark.usefixtures("config_dependencies_enabled")
class TestConfigYAMLDependencies:
    def test_merges_nested_dependencies(self, tmp_path: Path) -> None:
        main = tmp_path / "config.dev.yaml"
        shared = tmp_path / "shared" / "config.shared.yaml"
        base = tmp_path / "config.base.yaml"
        main.write_text(
            """\
dependencies: [shared/config.shared.yaml]
environment:
  project: my-project
  selected: [modules/a]
variables:
  modules:
    a:
      var_a: main
"""
        )
        shared.parent.mkdir()
        shared.write_text(
            """\
dependencies: [../config.base.yaml]
environment:
  project: my-project
  selected: [modules/a, modules/b]
variables:
  modules:
    b:
      var_b: shared
"""
        )
        # base is depended on twice, directly by shared and through a cycle back to main.
        base.write_text(
            """\
dependencies: [config.dev.yaml, shared/config.shared.yaml]
environment:
  selected: [modules/c]
variables:
  modules:
    a:
      var_base: base
"""
        )

        config = ConfigYAML.from_yaml_file(main)

        assert {
            "selected": config.environment.selected,
            "variables": config.variables,
            "source_files": {path.as_posix(): file.name for path, file in config.variable_source_files.items()},
        } == {
            "selected": ["modules/a", "modules/b", "modules/c"],
            "variables": {"modules": {"a": {"var_a": "main", "var_base": "base"}, "b": {"var_b": "shared"}}},
            "source_files": {
                "modules": "config.dev.yaml",
                "modules/a": "config.dev.yaml",
                "modules/a/var_a": "config.dev.yaml",
                "modules/a/var_base": "config.base.yaml",
                "modules/b": "config.shared.yaml",
                "modules/b/var_b": "config.shared.yaml",
            },
        }

    def test_raises_with_all_conflicts(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config.dev.yaml").write_text(
            """\
dependencies: [config.first.yaml, config.second.yaml]
environment:
  project: my-project
  validation-type: dev
variables:
  modules:
    a:
      var1: main
      section:
        nested: main
"""
        )
        (tmp_path / "config.first.yaml").write_text(
            """\
environment:
  project: other-project
  validation-type: dev
variables:
  modules:
    a:
      var1: first
      var2: first
"""
        )
        (tmp_path / "config.second.yaml").write_text(
            """\
environment:
  validation-type: prod
variables:
  modules:
    a:
      var2: second
      section: second
"""
        )

        with pytest.raises(ToolkitValueError) as exc_info:
            ConfigYAML.from_yaml_file(tmp_path / "config.dev.yaml")

        assert str(exc_info.value) == (
            "Cannot merge the dependencies of config YAML file 'config.dev.yaml':\n"
            "- environment.project is 'other-project' in 'config.first.yaml', but 'my-project' in 'config.dev.yaml'\n"
            "- variables.modules.a.var1 is defined in config.dev.yaml and config.first.yaml\n"
            "- environment.validation-type is 'prod' in 'config.second.yaml', but 'dev' in 'config.dev.yaml'\n"
            "- variables.modules.a.var2 is defined in config.first.yaml and config.second.yaml\n"
            "- variables.modules.a.section is defined in config.dev.yaml and config.second.yaml"
        )

    def test_raises_on_missing_dependency(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config.dev.yaml").write_text("dependencies: [config.missing.yaml]\n")

        with pytest.raises(ToolkitValueError, match=r"has dependencies that do not exist: config.missing.yaml"):
            ConfigYAML.from_yaml_file(tmp_path / "config.dev.yaml")


def test_dependencies_require_feature_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda _flag: False)
    (tmp_path / "config.dev.yaml").write_text("dependencies: [config.shared.yaml]\n")

    with pytest.raises(ToolkitValueError, match=r"config-dependencies = true"):
        ConfigYAML.from_yaml_file(tmp_path / "config.dev.yaml")
