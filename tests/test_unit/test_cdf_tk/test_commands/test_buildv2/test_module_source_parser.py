from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.build_v2._module_source_parser import ModuleSourceParser


class TestModuleSourceParser:
    @pytest.mark.parametrize(
        "yaml_files, expected_modules",
        [
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                ],
                {"modules/moduleA": ["modules/moduleA/data_modeling/my.Space.yaml"]},
                id="Single module with one YAML file",
            )
        ],
    )
    def test_find_modules(self, yaml_files: list[str], expected_modules: dict[str, list[str]]):
        found_modules = ModuleSourceParser._find_modules([Path(yaml_file) for yaml_file in yaml_files])
        actual_modules = {
            module.as_posix(): [file.as_posix() for file in files] for module, files in found_modules.items()
        }
        assert actual_modules == expected_modules
