import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.prototypes.commands import ModulesCommand
from cognite_toolkit._cdf_tk.prototypes.commands.modules import CLICommands
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests_migrations.constants import PROJECT_INIT_DIR, SUPPORTED_TOOLKIT_VERSIONS, TEST_DIR_ROOT, chdir


@pytest.fixture(scope="function")
def tool_globals() -> CDFToolConfig:
    with chdir(TEST_DIR_ROOT.parent):
        load_dotenv(".env")
        return CDFToolConfig()


@pytest.fixture(scope="function")
def local_tmp_project_path() -> Path:
    project_path = TEST_DIR_ROOT / "tmp-project"
    if project_path.exists():
        shutil.rmtree(project_path)
    project_path.mkdir(exist_ok=True)
    yield project_path


@pytest.fixture(scope="function")
def local_build_path() -> Path:
    build_path = TEST_DIR_ROOT / "build"
    if build_path.exists():
        shutil.rmtree(build_path)

    build_path.mkdir(exist_ok=True)
    # This is a small hack to get 0.1.0b1-4 working
    (build_path / "file.txt").touch(exist_ok=True)
    yield build_path


# This test is not part of the ordinary test suite, as it requires test data that is in the order of 100MB
# and thus is not suitable for running on every test run or committing to the repository.
@pytest.mark.parametrize("previous_version", list(SUPPORTED_TOOLKIT_VERSIONS))
# This is to allow running the test with having uncommitted changes in the repository.
@patch.object(CLICommands, "has_uncommitted_changes", lambda: False)
def tests_modules_upgrade_(
    previous_version: Path, local_tmp_project_path: Path, local_build_path: Path, tool_globals: CDFToolConfig
) -> None:
    project_init = PROJECT_INIT_DIR / f"project_{previous_version}"
    if not project_init.exists():
        pytest.skip(f"Project init for version {previous_version} does not exist.")
    # Copy the project to a temporary location as the upgrade command modifies the project.
    shutil.copytree(project_init, local_tmp_project_path, dirs_exist_ok=True)

    with chdir(TEST_DIR_ROOT):
        modules = ModulesCommand()
        modules.upgrade(local_tmp_project_path)

        build = BuildCommand(print_warning=False)
        config_yaml = local_tmp_project_path / "config.dev.yaml"
        assert config_yaml.exists()
        yaml_data = yaml.safe_load(config_yaml.read_text())
        yaml_data["environment"]["selected"] = ["cognite_modules/", "custom_modules/"]
        config_yaml.write_text(yaml.dump(yaml_data))
        build.execute(False, local_tmp_project_path, local_build_path, build_env_name="dev", no_clean=False)

        deploy = DeployCommand(print_warning=False)
        deploy.execute(
            tool_globals,
            str(local_build_path),
            build_env_name="dev",
            dry_run=True,
            drop=True,
            drop_data=True,
            include=list(LOADER_BY_FOLDER_NAME),
            verbose=True,
        )

    assert True
