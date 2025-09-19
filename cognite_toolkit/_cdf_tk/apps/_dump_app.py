from pathlib import Path
from typing import Annotated, Any, Union

import typer
from cognite.client.data_classes import WorkflowVersionId
from cognite.client.data_classes.data_modeling import DataModelId, ViewId
from rich import print

from cognite_toolkit._cdf_tk.commands import DumpDataCommand, DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_data import (
    AssetFinder,
    EventFinder,
    FileMetadataFinder,
    TimeSeriesFinder,
)
from cognite_toolkit._cdf_tk.commands.dump_resource import (
    AgentFinder,
    DataModelFinder,
    DataSetFinder,
    ExtractionPipelineFinder,
    FunctionFinder,
    GroupFinder,
    LocationFilterFinder,
    NodeFinder,
    StreamlitFinder,
    TransformationFinder,
    WorkflowFinder,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    EventInteractiveSelect,
    FileMetadataInteractiveSelect,
    TimeSeriesInteractiveSelect,
)


class DumpApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_main)
        if Flags.DUMP_DATA.is_enabled():
            self.add_typer(DumpDataApp(*args, **kwargs), name="data")
            self.add_typer(DumpConfigApp(*args, **kwargs), name="config")
        else:
            self.command("datamodel")(DumpConfigApp.dump_datamodel_cmd)

            self.command("asset")(DumpDataApp.dump_asset_cmd)
            self.command("timeseries")(DumpDataApp.dump_timeseries_cmd)

            self.command("workflow")(DumpConfigApp.dump_workflow)
            self.command("transformation")(DumpConfigApp.dump_transformation)
            self.command("group")(DumpConfigApp.dump_group)
            self.command("node")(DumpConfigApp.dump_node)

            if Flags.DUMP_EXTENDED.is_enabled():
                self.command("location-filter")(DumpConfigApp.dump_location_filters)
                self.command("extraction-pipeline")(DumpConfigApp.dump_extraction_pipeline)
                self.command("functions")(DumpConfigApp.dump_functions)
                self.command("datasets")(DumpConfigApp.dump_datasets)
                self.command("streamlit")(DumpConfigApp.dump_streamlit)

            if Flags.AGENTS.is_enabled() and Flags.DUMP_EXTENDED.is_enabled():
                self.command("agents")(DumpConfigApp.dump_agents)

    @staticmethod
    def dump_main(ctx: typer.Context) -> None:
        """Commands to dump resource configurations from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump --help[/] for more information.")
        return None


class DumpConfigApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_config_main)

        self.command("datamodel")(self.dump_datamodel_cmd)
        self.command("workflow")(self.dump_workflow)
        self.command("transformation")(self.dump_transformation)
        self.command("group")(self.dump_group)
        self.command("node")(self.dump_node)
        if Flags.DUMP_EXTENDED.is_enabled():
            self.command("location-filters")(self.dump_location_filters)
            self.command("extraction-pipeline")(self.dump_extraction_pipeline)
            self.command("datasets")(DumpConfigApp.dump_datasets)
            self.command("functions")(self.dump_functions)
            self.command("streamlit")(DumpConfigApp.dump_streamlit)
        if Flags.DUMP_EXTENDED.is_enabled() and Flags.AGENTS.is_enabled():
            self.command("agents")(self.dump_agents)

    @staticmethod
    def dump_config_main(ctx: typer.Context) -> None:
        """Commands to dump resource configurations from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump config --help[/] for more information.")
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
            list[str] | None,
            typer.Argument(
                help="Group name(s) to dump. Format: name. Example: 'my_group'. "
                "If nothing is provided, an interactive prompt will be shown to select the group(s).",
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
                GroupFinder(client, tuple(group_name) if group_name else None),
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

    @staticmethod
    def dump_datasets(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID(s) of the datasets you want to dump. "
                "If nothing is provided, an interactive prompt will be shown to select the datasets.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the dataset files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the datasets.",
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
        """This command will dump the selected datasets as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                DataSetFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_functions(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID(s) of the functions you want to dump. "
                "If nothing is provided, an interactive prompt will be shown to select the functions.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the function files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the functions.",
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
        """This command will dump the selected functions as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                FunctionFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )

    @staticmethod
    def dump_streamlit(
        ctx: typer.Context,
        external_id: Annotated[
            list[str] | None,
            typer.Argument(
                help="The external ID(s) of the Streamlit apps you want to dump. "
                "If nothing is provided, an interactive prompt will be shown to select the Streamlit apps.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the Streamlit app files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the Streamlit apps.",
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
        """This command will dump the selected Streamlit apps as yaml to the folder specified, defaults to /tmp."""
        client = EnvironmentVariables.create_from_environment().get_client()
        cmd = DumpResourceCommand()
        cmd.run(
            lambda: cmd.dump_to_yamls(
                StreamlitFinder(client, tuple(external_id) if external_id else None),
                output_dir=output_dir,
                clean=clean,
                verbose=verbose,
            )
        )


class DumpDataApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.dump_data_main)
        self.command("asset")(self.dump_asset_cmd)
        self.command("files-metadata")(self.dump_files_cmd)
        self.command("timeseries")(self.dump_timeseries_cmd)
        self.command("event")(self.dump_event_cmd)

    @staticmethod
    def dump_data_main(ctx: typer.Context) -> None:
        """Commands to dump data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf dump data --help[/] for more information.")
        return None

    @staticmethod
    def dump_asset_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Hierarchy to dump.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted"
                "to select which assets to dump",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the assets in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of assets to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the asset files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the asset.",
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
        """This command will dump the selected assets in the selected format to the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = AssetInteractiveSelect(client, "dump").select_hierarchies_and_data_sets()

        cmd.run(
            lambda: cmd.dump_table(
                AssetFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,
                verbose,
            )
        )

    @staticmethod
    def dump_files_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump filemetadata from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the filemetadata in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of filemetadata to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the filemetadata files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the filemetadata.",
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
        """This command will dump the selected events to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        cmd.validate_directory(output_dir, clean)
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = FileMetadataInteractiveSelect(client, "dump").select_hierarchies_and_data_sets()
        cmd.run(
            lambda: cmd.dump_table(
                FileMetadataFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,
                verbose,
            )
        )

    @staticmethod
    def dump_timeseries_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump timeseries from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the timeseries in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of timeseries to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the timeseries files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the timeseries.",
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
        """This command will dump the selected timeseries to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = TimeSeriesInteractiveSelect(client, "dump").select_hierarchies_and_data_sets()
        cmd.run(
            lambda: cmd.dump_table(
                TimeSeriesFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,
                verbose,
            )
        )

    @staticmethod
    def dump_event_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-h",
                help="Asset hierarchy (sub-trees) to dump event from.",
            ),
        ] = None,
        data_set: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="Data set to dump. If neither hierarchy nor data set is provided, the user will be prompted.",
            ),
        ] = None,
        format_: Annotated[
            str,
            typer.Option(
                "--format",
                "-f",
                help="Format to dump the event in. Supported formats: csv, and parquet.",
            ),
        ] = "csv",
        limit: Annotated[
            int | None,
            typer.Option(
                "--limit",
                "-l",
                help="Limit the number of events to dump.",
            ),
        ] = None,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to dump the events files.",
                allow_dash=True,
            ),
        ] = Path("tmp"),
        clean: Annotated[
            bool,
            typer.Option(
                "--clean",
                "-c",
                help="Delete the output directory before dumping the events.",
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
        """This command will dump the selected events to the selected format in the folder specified, defaults to /tmp."""
        cmd = DumpDataCommand()
        cmd.validate_directory(output_dir, clean)
        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None and data_set is None:
            hierarchy, data_set = EventInteractiveSelect(client, "dump").select_hierarchies_and_data_sets()
        cmd.run(
            lambda: cmd.dump_table(
                EventFinder(client, hierarchy or [], data_set or []),
                output_dir,
                clean,
                limit,
                format_,
                verbose,
            )
        )
