from pathlib import Path
from typing import Annotated, Any, Optional, Union

import typer
from cognite.client.data_classes.data_modeling import NodeId
from rich import print

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import PullCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.loaders import NodeLoader, TransformationLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig

CDF_TOML = CDFToml.load()


class PullApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.main)
        self.command("transformation")(self.pull_transformation_cmd)
        self.command("node")(self.pull_node_cmd)

    def main(self, ctx: typer.Context) -> None:
        """Commands pull functionality"""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf pull --help[/] for more information.")

    def pull_transformation_cmd(
        self,
        ctx: typer.Context,
        external_id: Annotated[
            Optional[str],
            typer.Argument(
                help="External id of the transformation to pull. If not provided, interactive mode will be used.",
            ),
        ] = None,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        env: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                "-e",
                help="Environment to use.",
            ),
        ] = CDF_TOML.cdf.default_env,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will pull the specified transformation and update its YAML file in the module folder"""
        cmd = PullCommand()
        cmd.run(
            lambda: cmd.execute(
                organization_dir,
                external_id,
                env,
                dry_run,
                verbose,
                CDFToolConfig.from_context(ctx),
                TransformationLoader,
            )
        )

    def pull_node_cmd(
        self,
        ctx: typer.Context,
        node_id: Annotated[
            Optional[list[str]],
            typer.Argument(
                help="The node id of the node to pull Should be two strings a space and external id. "
                "If not provided, interactive mode will be used.",
            ),
        ] = None,
        organization_dir: Annotated[
            Path,
            typer.Option(
                "--organization-dir",
                "-o",
                help="Where to find the module templates to build from",
            ),
        ] = CDF_TOML.cdf.default_organization_dir,
        env: Annotated[
            Optional[str],
            typer.Option(
                "--env",
                "-e",
                help="Environment to use.",
            ),
        ] = CDF_TOML.cdf.default_env,
        dry_run: Annotated[
            bool,
            typer.Option(
                "--dry-run",
                "-r",
                help="Whether to do a dry-run, do dry-run if present.",
            ),
        ] = False,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will pull the specified node and update its YAML file in the module folder."""

        cmd = PullCommand()
        node_id_: Union[NodeId, None] = None
        if node_id is not None:
            if len(node_id) != 2:
                raise ToolkitRequiredValueError("Node id should be two strings a space and external id.")
            space, external_id = node_id
            node_id_ = NodeId(space, external_id)

        cmd.run(
            lambda: cmd.execute(
                organization_dir,
                node_id_,
                env,
                dry_run,
                verbose,
                CDFToolConfig.from_context(ctx),
                NodeLoader,
            )
        )
