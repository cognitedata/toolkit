from collections.abc import Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.auth.utils import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildFolder, BuildParameters, PostBuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import ValidationResultList
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult, DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


class BuildDeployValidateCommand(ToolkitCommand):
    """This is a composite command that builds, dry-runs, and validates."""

    def pre_flight(
        self,
        parameters: BuildParameters,
        post_build: PostBuildParameters,
        validation: Literal["syntax-only", "local", "remote", "capacity", "auto"] = "auto",
        client: ToolkitClient | None = None,
    ) -> tuple[BuildFolder, ValidationResultList]:
        if validation in ("remote", "capacity") and not client:
            raise ToolkitValueError("Client must be provided for remote or capacity validation.")
        build_cmd = BuildV2Command(
            silent=self.silent,
            skip_tracking=self.tracker.skip_tracking,
            client=self._client,
            print_warning=self.print_warning,
        )
        validate_cmd = ValidationCommand(
            silent=self.silent,
            skip_tracking=self.tracker.skip_tracking,
            client=self._client,
            print_warning=self.print_warning,
        )

        build_folder = build_cmd.build(parameters, context="integrated", client=client, display=True)
        if validation in ("none", "syntax-only"):
            return build_folder, ValidationResultList()

        resources = [resource for module in build_folder.built_modules for resource in module.resources]
        results: Sequence[DeploymentResult] = []
        if validation == "capacity" and client is not None:
            deploy_cmd = DeployV2Command(
                silent=self.silent,
                skip_tracking=self.tracker.skip_tracking,
                client=client,
                print_warning=self.print_warning,
            )

            env_vars = EnvironmentVariables.create_from_environment()
            results = deploy_cmd.deploy(
                user_build_dir=build_folder.build_dir,
                env_vars=env_vars,
                client=client,
                options=DeployOptions(
                    cdf_project=client.config.project,
                    dry_run=True,
                    environment_variables=env_vars.dump(),
                ),
            )
        validation_results = validate_cmd.validate(resources, results, None, client, display=True, context="integrated")

        build_cmd.post_build(
            build_folder,
            validation_results,
            client=client,
            parameters=post_build,
            display=True,
            verbose=parameters.verbose,
        )

        return build_folder, validation_results
