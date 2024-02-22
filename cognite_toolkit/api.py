from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cognite_toolkit.cdf_tk.utils import CDFToolConfig


def build(
    source_dir: Path | str,
    variables: dict[str, Any] | None = None,
    target_dir: Path | str | None = None,
    modules: Sequence[str] | str | None = None,
) -> str:
    """Build silently the specified modules, default all, from source_dir into the target_dir. Raises
    an exception if the build fails.

    Args:
        source_dir: The directory containing the source configuration files.
        variables: The variables to use for resolving of config variables when building the modules.
        target_dir: The directory to write the built files to. If None, a tempt dir is created.
        modules: The modules to build, default all.
    Returns:
        The directory where the built files are located.
    """
    pass
    # We don't want to rely on an environment or a config file. Thus, we need to
    # implement build_config() and construct BuildConfigYAML and SystemYAML if needed.
    # We want to be able to specify the source_dir pointing to a module directory or a
    # folder structure with multiple modules. If there are multiple modules in the source_dir,
    # we want to be able to specify a single or a list of modules to build.
    # If a variable cannot be resolved, we want to raise an exception.
    return "path/to/built/files"


def deploy(
    source_dir: Path | str,
    resource_types: Sequence[str] | str | None = None,
    ToolGlobals: CDFToolConfig | None = None,
) -> None:
    """Deploy silently the yaml configurations in the specified source_dir to Cognite Data Fusion using the
    configured CDF client in ToolGlobals or (default) by picking up environment variables. Raises an exception
    if the deployment fails.

    Args:
        source_dir: The directory containing the source configuration files.
        resource_types: The resource types to deploy, default all.
        ToolGlobals: The configuration for the CDF client, default picked up from the environment.
    """
    pass
    # When deploying, we want to be able to specify the source_dir pointing to a directory with
    # either yaml files or a folder structure of yaml files in directories per resource type.
    # If resource_types is specified, we want to deploy only the specified resource types.
    # If the dir only contains yaml files, we expect resource_types to be only one resource type
    # specifying the resource type of the yaml files in the dir.
    # ToolGlobals can be supplied if a specific CDF client configuration is needed, if not, CogniteClient
    # should be constructed from environment variables.


# TODO: Add a deploy_from_template() function that does build() and deploy() in one go.
#      By specifying module and resource_type, we can pinpoint exactly what to deploy.
#      We could also supply a git URL instead of a source_dir, and the function would clone the repo
#      and build and deploy the specified resource types/modules.


def clean(
    source_dir: Path | str,
    resource_types: Sequence[str] | str | None = None,
    ToolGlobals: CDFToolConfig | None = None,
) -> None:
    """Clean silently the yaml configurations in the specified source_dir to Cognite Data Fusion using the
    configured CDF client in ToolGlobals or (default) by picking up environment variables. Raises an exception
    if the clean fails.

    Args:
        source_dir: The directory containing the source configuration files.
        resource_types: The resource types to deploy, default all.
        ToolGlobals: The configuration for the CDF client, default picked up from the environment.
    """
    pass
    # When cleaning, we want to be able to specify the source_dir pointing to a directory with
    # either yaml files or a folder structure of yaml files in directories per resource type.
    # If resource_types is specified, we want to clean only the specified resource types.
    # If the dir only contains yaml files, we expect resource_types to be only one resource type
    # specifying the resource type of the yaml files in the dir.


# TODO: Add a clean_from_template() function that does build() and clean() in one go.
#      By specifying module and resource_type, we can pinpoint exactly what to clean.
#      We could also supply a git URL instead of a source_dir, and the function would clone the repo
#      and build and clean the specified resource types/modules.


def run(
    resource_type: Sequence[str] | str,
    external_id: Sequence[str] | str,
    ToolGlobals: CDFToolConfig | None = None,
) -> None:
    """Run the specified resource in Cognite Data Fusion using the configured CDF client in ToolGlobals or
    (default) by picking up environment variables. Raises an exception if the run fails.

    Args:
        resource_type: The resource type to run (transformation, function).
        external_id: The external id of the resource to run.
        ToolGlobals: The configuration for the CDF client, default picked up from the environment.
    """
    pass
    # We want to be able to run one or more transformations or functions in CDF. We need to specify the
    # resource_type and the external_id of the resource to run. ToolGlobals can be supplied if a specific
    # CDF client configuration is needed, if not, CogniteClient should be constructed from environment variables.
