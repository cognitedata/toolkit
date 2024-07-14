from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests import data


def test_deploy_core_model(cdf_tool_config: CDFToolConfig) -> None:
    deploy_command = DeployCommand(print_warning=False, skip_tracking=True)

    deploy_command.execute(
        cdf_tool_config,
        build_dir_raw=str(data.BUILD_CORE_MODEL),
        build_env_name="env",
        dry_run=False,
        drop=True,
        drop_data=True,
        include=list(LOADER_BY_FOLDER_NAME.keys()),
        verbose=False,
    )
