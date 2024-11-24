import tempfile
from pathlib import Path
from typing import Literal

import yaml
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands import BuildCommand, DeployCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH, MODULES
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment, InitConfigYAML, Packages
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils.auth import CDFToolConfig
from cognite_toolkit._cdf_tk.utils.modules import module_directory_from_path


class CogniteToolkitDemo:
    def __init__(self) -> None:
        self._cdf_tool_config = CDFToolConfig()

    @property
    def _build_dir(self) -> Path:
        build_path = Path(tempfile.gettempdir()).resolve() / "cognite-toolkit-build"
        build_path.mkdir(exist_ok=True)
        return build_path

    def quickstart(self) -> None:
        print(Panel("Running Toolkit QuickStart..."))
        # Lookup user ID to add user ID to the group to run the workflow
        user = self._cdf_tool_config.toolkit_client.iam.user_profiles.me()

        # Build directly from _builtin_modules
        build = BuildCommand()
        environment: Literal["dev"] = "dev"
        packages = Packages().load(BUILTIN_MODULES_PATH)
        quickstart = packages["quickstart"]
        selected_paths: set[Path] = set()
        for module in quickstart.modules:
            selected_paths.add(module.relative_path)
            selected_paths.update(module.parent_relative_paths)
            if module.definition:
                for extra in module.definition.extra_resources:
                    module_dir = module_directory_from_path(extra)
                    selected_paths.add(module_dir)
                    selected_paths.update(module_dir.parents)

        ignore_variable_patterns: list[tuple[str, ...]] = []
        for to_ignore in ["sourcesystem", "contextualization"]:
            ignore_variable_patterns.extend(
                [
                    ("modules", to_ignore, "*", "workflow"),
                    ("modules", to_ignore, "*", "workflowClientId"),
                    ("modules", to_ignore, "*", "workflowClientSecret"),
                    ("modules", to_ignore, "*", "groupSourceId"),
                ]
            )
        config_init = InitConfigYAML(
            Environment(
                name=environment,
                project=self._cdf_tool_config.project,
                build_type=environment,
                selected=[f"{MODULES}/"],
            )
        ).load_defaults(BUILTIN_MODULES_PATH, selected_paths, ignore_variable_patterns)
        config_init.lift()
        config_raw = config_init.dump_yaml_with_comments()
        # Ensure the user can execute the workflow
        config_raw = config_raw.replace("<your user id>", user.user_identifier)
        # To avoid warnings about not set
        config_raw = config_raw.replace("<not set>", "123456")

        config = BuildConfigYAML.load(yaml.safe_load(config_raw), environment, Path("memory"))

        build.run(
            lambda: build.build_config(
                build_dir=self._build_dir,
                organization_dir=BUILTIN_MODULES_PATH,
                config=config,
                packages={},
                clean=True,
                verbose=False,
                ToolGlobals=self._cdf_tool_config,
                on_error="raise",
            )
        )

        deploy = DeployCommand()

        deploy.run(
            lambda: deploy.execute(
                ToolGlobals=self._cdf_tool_config,
                build_dir=self._build_dir,
                build_env_name="dev",
                dry_run=False,
                drop_data=False,
                drop=False,
                force_update=False,
                include=list(LOADER_BY_FOLDER_NAME.keys()),
                verbose=False,
            )
        )
