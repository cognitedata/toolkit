from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2._module_source_parser import ModuleSourceParser
from cognite_toolkit._cdf_tk.constants import DEFAULT_CONFIG_FILE


class TestModuleSourceParser:
    @pytest.mark.parametrize(
        "yaml_files, expected_modules, expected_orphans",
        [
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                ],
                {"modules/moduleA": ["modules/moduleA/data_modeling/my.Space.yaml"]},
                [],
                id="Single module with one YAML file",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/another_file.Space.yaml",
                ],
                {"modules/moduleA": ["modules/moduleA/data_modeling/my.Space.yaml"]},
                ["modules/moduleA/another_file.Space.yaml"],
                id="Single module with one valid YAML file and one orphan",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/data_modeling/another.Space.yaml",
                    f"modules/moduleA/{DEFAULT_CONFIG_FILE}",
                ],
                {
                    "modules/moduleA": [
                        "modules/moduleA/data_modeling/my.Space.yaml",
                        "modules/moduleA/data_modeling/another.Space.yaml",
                    ],
                },
                [],
                id="Single module with multiple valid YAML files and one excluded file",
            ),
        ],
    )
    def test_find_modules(
        self, yaml_files: list[str], expected_modules: dict[str, list[str]], expected_orphans: list[str]
    ) -> None:
        found_modules, orphans = ModuleSourceParser._find_modules([Path(yaml_file) for yaml_file in yaml_files])
        actual_modules = {
            module.as_posix(): [file.as_posix() for file in files] for module, files in found_modules.items()
        }
        actual_orphans = [file.as_posix() for file in orphans]
        assert actual_modules == expected_modules
        assert actual_orphans == expected_orphans
