from __future__ import annotations

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import BuildCommand
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML
from cognite_toolkit._cdf_tk.utils import iterate_modules
from tests.data import PROJECT_FOR_TEST
from tests.test_unit.test_cdf_tk.constants import BUILD_DIR


class TestBuildConfigYAML:
    def test_build_config_create_valid_build_folder(self, project_for_test_config_dev_yaml: str) -> None:
        build_env_name = "dev"
        cdf_toml = CDFToml.load(PROJECT_FOR_TEST)
        config = BuildConfigYAML.load_from_directory(PROJECT_FOR_TEST, build_env_name)
        available_modules = {module.name for module, _ in iterate_modules(PROJECT_FOR_TEST)}
        config.environment.selected = list(available_modules)

        BuildCommand(silent=True).build_config(
            BUILD_DIR, PROJECT_FOR_TEST, config=config, packages=cdf_toml.modules.packages, clean=True, verbose=False
        )

        # The resulting build folder should only have subfolders that are matching the folder name
        # used by the loaders.
        invalid_resource_folders = [
            dir_.name for dir_ in BUILD_DIR.iterdir() if dir_.is_dir() and dir_.name not in CRUDS_BY_FOLDER_NAME
        ]
        assert not invalid_resource_folders, f"Invalid resource folders after build: {invalid_resource_folders}"
