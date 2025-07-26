from pathlib import Path
from typing import Annotated, Any, Union

import typer
from cognite.client.data_classes import WorkflowVersionId
from cognite.client.data_classes.data_modeling import DataModelId, ViewId
from rich import print

from cognite_toolkit._cdf_tk.commands import DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import (
    AgentFinder,
    DataModelFinder,
    ExtractionPipelineFinder,
    GroupFinder,
    LocationFilterFinder,
    NodeFinder,
    TransformationFinder,
    WorkflowFinder,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitDeprecationWarning
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

from ._download_app import DownloadApp


class DumpApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_main)
        if Flags.DUMP_DATA.is_enabled():
            ToolkitDeprecationWarning(
                feature="dump-data alpha flag", alternative="Use the download alpha flag"
            ).print_warning()

        self.command("datamodel")(self.dump_datamodel_cmd)

        self.command("asset")(DownloadApp.dump_asset_cmd)
        self.command("timeseries")(DownloadApp.dump_timeseries_cmd)

        self.command("workflow")(self.dump_workflow)
        self.command("transformation")(self.dump_transformation)
        self.command("group")(self.dump_group)
        self.command("node")(self.dump_node)

        if Flags.DUMP_EXTENDED.is_enabled():
            self.command("location-filter")(self.dump_location_filters)
            self.command("extraction-pipeline")(self.dump_extraction_pipeline)

        if Flags.AGENTS.is_enabled() and Flags.DUMP_EXTENDED.is_enabled():
            self.command("agents")(self.dump_agents)

    @staticmethod
    def dump_main(ctx: typer.Context) -> None:
        """Commands to dump resource configurations from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump --help[/] for more information.")
        return None

    @staticmethod
    def dump_datamodel_cmd(
        ctx: typer.Context,
        data_model_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="Data model ID to dump. Format: space external_id version. Example: 'my_space my_external_id v1'. "
                "Note that version is optional and defaults to the latest published version. If nothing is provided,"
                "an interactive prompt will be shown to select the data model.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the datamodel files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        include_global: Annotated[
            bool,
            typer.Option(
                "--include-global",
                "-i",
                help="Include global containers, views, spaces in the dump. "
                "If this flag is not set, the global resources will be skipped.",
            ),
        ] = False,
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the datamodel.",
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
        """This command will dump the selected data model as yaml to the folder specified, defaults to /tmp."""
        selected_data_model: Union[DataModelId, None] = None
        if data_model_id is not None:
            if len(data_model_id) < 2:
                raise ToolkitRequiredValueError(
                    "Data model ID must have at least 2 parts: space, external_id, and, optionally, version."
                )
            selected_data_model = DataModelId(*data_model_id)
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                DataModelFinder(client, selected_data_model, include_global=include_global),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_workflow(
        ctx: typer.Context,
        workflow_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="Workflow ID to dump. Format: external_id version. Example: 'my_external_id v1'. "
                "If nothing is provided, an interactive prompt will be shown to select the workflow",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the workflow files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the workflow.",
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
        """This command will dump the selected workflow as yaml to the folder specified, defaults to /tmp."""
        selected_workflow: Union[WorkflowVersionId, None] = None
        if workflow_id is not None:
            if len(workflow_id) <= 1:
                raise ToolkitRequiredValueError(
                    "Workflow ID must have at least 1 part: external_id and, optionally, version."
                )
            selected_workflow = WorkflowVersionId(*workflow_id)
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                WorkflowFinder(client, selected_workflow),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_transformation(
        ctx: typer.Context,
        transformation_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="Transformation IDs to dump. Format: external_id. Example: 'my_external_id'. "
                "If nothing is provided, an interactive prompt will be shown to select the transformation(s).",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the transformation files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the transformation.",
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
        """This command will dump the selected transformation as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                TransformationFinder(client, tuple(transformation_id) if transformation_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_group(
        ctx: typer.Context,
        group_name: Annotated[
            str | None,
            typer.Argument(
                help="Group name to dump. Format: name. Example: 'my_group'. "
                "If nothing is provided, an interactive prompt will be shown to select the group.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the group files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the group.",
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
        """This command will dump the selected group as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()

        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                GroupFinder(client, group_name),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_agents(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external IDs of the agents you want to dump. You can provide multiple external IDs separated by spaces. "
                "If nothing is provided, an interactive prompt will be shown to select the agents.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the agent files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the agents.",
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
        """Dump on or more agents as yaml to the specified folder, defaults to /tmp."""

        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                AgentFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_node(
        ctx: typer.Context,
        view_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The view with the node properties you want to dump. Format: space externalId version. Example: 'my_space my_external_id version'. "
                "If nothing is provided, an interactive prompt will be shown to select the view.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the node files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the node.",
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
        """This command will dump the selected node as yaml to the folder specified, defaults to /tmp.
        The intended use case is to dump nodes which are used as configuration. It is not intended to dump
        large amounts of data.
        """
        client = EnvironmentVariables.create_from_environment().get_client()
        selected_view_id: Union[None, ViewId] = None
        if view_id is not None:
            if len(view_id) <= 2:
                raise ToolkitRequiredValueError(
                    "View ID must have at least 2 parts: space, external_id and, optionally, version."
                )
            selected_view_id = ViewId(*view_id)

        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                NodeFinder(client, selected_view_id),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_location_filters(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external IDs of the location filters you want to dump. You can provide multiple external IDs separated by spaces. "
                "If nothing is provided, an interactive prompt will be shown to select the location filters.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the location filters.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the location filters.",
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
        """This command will dump the selected location filters as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                LocationFilterFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_extraction_pipeline(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID(s) of the extraction pipeline you want to dump. "
                "If nothing is provided, an interactive prompt will be shown to select the extraction pipeline.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the extraction pipeline files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the extraction pipeline.",
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
        """This command will dump the selected extraction pipeline as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                ExtractionPipelineFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )
