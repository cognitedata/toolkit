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
                {"modules/moduleA": {"data_modeling": ["modules/moduleA/data_modeling/my.Space.yaml"]}},
                [],
                id="Single module with one YAML file",
            ),
            pytest.param(
                [
                    "modules/moduleA/data_modeling/my.Space.yaml",
                    "modules/moduleA/another_file.Space.yaml",
                ],
                {"modules/moduleA": {"data_modeling": ["modules/moduleA/data_modeling/my.Space.yaml"]}},
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
                    "modules/moduleA": {
                        "data_modeling": [
                            "modules/moduleA/data_modeling/my.Space.yaml",
                            "modules/moduleA/data_modeling/another.Space.yaml",
                        ]
                    },
                },
                [],
                id="Single module with multiple valid YAML files and one excluded file",
            ),
        ],
    )
    def test_find_modules(
        self,
        yaml_files: list[str],
        expected_modules: dict[str, dict[str, list[str]]],
        expected_orphans: list[str],
        tmp_path: Path,
    ) -> None:
        org = tmp_path
        for yaml_file in yaml_files:
            file_path = org / yaml_file
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()

        found_modules, orphans = ModuleSourceParser._find_modules([Path(yaml_file) for yaml_file in yaml_files], org)
        actual_modules = {
            module.as_posix(): {
                resource_folder: [file.relative_to(org).as_posix() for file in files]
                for resource_folder, files in source.resource_files_by_folder.items()
            }
            for module, source in found_modules.items()
        }
        actual_orphans = [file.as_posix() for file in orphans]
        assert actual_modules == expected_modules
        assert actual_orphans == expected_orphans
