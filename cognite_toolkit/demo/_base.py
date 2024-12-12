import re
import tempfile
import textwrap
from pathlib import Path

from cognite.client.data_classes import UserProfile
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.commands import AuthCommand, BuildCommand, DeployCommand, ModulesCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils.auth import AuthVariables, CDFToolConfig


class CogniteToolkitDemo:
    def __init__(self) -> None:
        self._cdf_tool_config = CDFToolConfig()
        print(
            Panel(
                textwrap.dedent("""
        This is a demo version of the Cognite Toolkit.

        It is intended to demonstrate the content and capabilities of the Toolkit.
        It is not intended to be used in production or development environments.

        To learn more about the Cognite Toolkit, visit https://docs.cognite.com/cdf/deploy/cdf_toolkit/.
        """)
            )
        )

    @property
    def _tmp_path(self) -> Path:
        return Path(tempfile.gettempdir()).resolve()

    @property
    def _build_dir(self) -> Path:
        build_path = self._tmp_path / "cognite-toolkit-build"
        build_path.mkdir(exist_ok=True)
        return build_path

    @property
    def _organization_dir(self) -> Path:
        organization_path = self._tmp_path / "cognite-toolkit-organization"
        organization_path.mkdir(exist_ok=True)
        return organization_path

    def quickstart(
        self, company_prefix: str | None, client_id: str | None = None, client_secret: str | None = None
    ) -> None:
        if sum([client_id is None, client_secret is None]) == 1:
            raise ValueError("Both client_id and client_secret must be provided or neither.")
        if company_prefix:
            self._verify_company_prefix(company_prefix)
        print(Panel("Running Toolkit QuickStart..."))
        user = self._cdf_tool_config.toolkit_client.iam.user_profiles.me()
        if client_id is None and client_secret is None:
            print("Client ID and secret not provided. Assuming user has all the necessary permissions.")
            self._init_build_deploy(user, company_prefix)
            return

        group_id: int | None = None
        try:
            # Lookup user ID to add user ID to the group to run the workflow
            auth = AuthCommand()
            auth_result = auth.verify(
                self._cdf_tool_config,
                dry_run=False,
                no_prompt=True,
                demo_principal=client_id,
            )
            group_id = auth_result.toolkit_group_id
            if auth_result.function_status is None:
                print(Panel("Unknown function status. If the demo fails, please check that functions are activated"))
            elif auth_result.function_status == "requested":
                print(
                    Panel(
                        "Function status is requested. Please wait for the function status to be activated before running the demo."
                    )
                )
                return
            elif auth_result.function_status == "inactive":
                print(Panel("Function status is inactive. Cannot run demo without functions."))
                return

            print("Switching to the demo service principal...")
            self._cdf_tool_config = CDFToolConfig(
                auth_vars=AuthVariables(
                    provider="cdf",
                    cluster=self._cdf_tool_config.cdf_cluster,
                    project=self._cdf_tool_config.project,
                    login_flow="client_credentials",
                    client_id=client_id,
                    client_secret=client_secret,
                )
            )
            self._init_build_deploy(user, company_prefix)
        finally:
            if group_id is not None:
                self._cdf_tool_config.toolkit_client.iam.groups.delete(id=group_id)

    def _init_build_deploy(self, user: UserProfile, company_prefix: str | None = None) -> None:
        modules_cmd = ModulesCommand()
        modules_cmd.run(
            lambda: modules_cmd.init(
                organization_dir=self._organization_dir,
                user_select="quickstart",
                clean=True,
                user_download_data=True,
                user_environments=["dev"],
            )
        )
        config_yaml = self._organization_dir / "config.dev.yaml"
        config_raw = config_yaml.read_text()
        # Ensure the user can execute the workflow
        config_raw = config_raw.replace("<your user id>", user.user_identifier)
        # To avoid warnings about not set values
        config_raw = config_raw.replace("<not set>", "123456-to-be-replaced")
        config_raw = config_raw.replace("<my-project-dev>", self._cdf_tool_config.project)
        if company_prefix is not None:
            config_raw = config_raw.replace("YourOrg", company_prefix)
        config_yaml.write_text(config_raw)

        # The Workflow trigger expects credentials to be set in the environment, so we delete it as
        # the user is expected to trigger the workflow manually.
        for workflow_trigger_file in (self._organization_dir / MODULES).rglob("*WorkflowTrigger.yaml"):
            workflow_trigger_file.unlink()

        build = BuildCommand()
        build.run(
            lambda: build.execute(
                build_dir=self._build_dir,
                organization_dir=self._organization_dir,
                selected=None,
                build_env_name="dev",
                verbose=False,
                ToolGlobals=self._cdf_tool_config,
                on_error="raise",
                no_clean=False,
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

    def _verify_company_prefix(self, company_prefix: str) -> None:
        """Needs to comply with the regex for container and views ExternalID

        * View:      ^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$
        * Container: ^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$
        """
        if not re.match(r"^[a-zA-Z]", company_prefix):
            raise ValueError("The company prefix must start with a letter.")
        if not re.match(r"^[a-zA-Z]([a-zA-Z0-9_]{0,253}[a-zA-Z0-9])?$", company_prefix):
            invalid_chars = re.findall(r"[^a-zA-Z0-9_]", company_prefix)
            if invalid_chars:
                raise ValueError(
                    f"The company prefix contains invalid characters: {', '.join(invalid_chars)}. Only letters, numbers, and underscores are allowed."
                )
            if len(company_prefix) > 255:
                raise ValueError("The company prefix must be 255 characters or less.")
            raise ValueError("The company prefix is invalid.")
        return
