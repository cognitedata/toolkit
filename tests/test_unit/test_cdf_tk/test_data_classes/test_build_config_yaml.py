from __future__ import annotations

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_ios import CRUDS_BY_FOLDER_NAME
from tests.data import PROJECT_FOR_TEST
from tests.test_unit.test_cdf_tk.constants import BUILD_DIR


class TestBuildConfigYAML:
    def test_build_config_create_valid_build_folder(self) -> None:
        BuildV2Command(silent=True, skip_tracking=True).build(
            parameters=BuildParameters(
                organization_dir=PROJECT_FOR_TEST,
                build_dir=BUILD_DIR,
                config_yaml=PROJECT_FOR_TEST / "config.dev.yaml",
                user_selected_modules=[f"{MODULES}/"],
            ),
        )

        # The resulting build folder should only have subfolders that are matching the folder name
        # used by the loaders.
        invalid_resource_folders = [
            dir_.name for dir_ in BUILD_DIR.iterdir() if dir_.is_dir() and dir_.name not in CRUDS_BY_FOLDER_NAME
        ]
        assert not invalid_resource_folders, f"Invalid resource folders after build: {invalid_resource_folders}"
