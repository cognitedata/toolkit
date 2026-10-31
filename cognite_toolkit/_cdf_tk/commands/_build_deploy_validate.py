from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildFolder, BuildParameters, PostBuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import ValidationResultList
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


class BuildDeployValidateCommand(ToolkitCommand):
    """This is a composite command that builds, dry-runs, and validates."""

    def pre_flight(
        self,
        parameters: BuildParameters,
        post_build: PostBuildParameters,
        validation: Literal["none", "syntax-only", "local", "remote", "capacity", "auto"] = "auto",
        client: ToolkitClient | None = None,
    ) -> tuple[BuildFolder, ValidationResultList]:
        if validation == "remote" and not client:
            raise ToolkitValueError("Client must be provided for remote validation.")
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

        if validation == "capacity":
            raise NotImplementedError(
                "The 'capacity' validation mode is not yet implemented. "
                "Please use 'local' or 'remote' validation modes instead."
            )

        resources = [resource for module in build_folder.built_modules for resource in module.resources]
        validation_results = validate_cmd.validate(resources, client, display=True)

        build_cmd.post_build(
            build_folder,
            validation_results,
            client=client,
            parameters=post_build,
            display=True,
            verbose=parameters.verbose,
        )

        return build_folder, validation_results
