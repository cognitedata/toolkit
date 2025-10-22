from enum import Enum
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.constants import DATA_DEFAULT_DIR
from cognite_toolkit._cdf_tk.storageio import (
    AssetIO,
    HierarchyIO,
    InstanceIO,
    RawIO,
)
from cognite_toolkit._cdf_tk.storageio.selectors import (
    AssetCentricSelector,
    AssetSubtreeSelector,
    DataSetSelector,
    InstanceSpaceSelector,
    RawTableSelector,
    SelectedTable,
    SelectedView,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.interactive_select import (
    AssetInteractiveSelect,
    DataModelingSelect,
    RawTableInteractiveSelect,
)


class RawFormats(str, Enum):
    ndjson = "ndjson"
    yaml = "yaml"


class AssetCentricFormats(str, Enum):
    csv = "csv"
    parquet = "parquet"
    ndjson = "ndjson"


class HierarchyFormats(str, Enum):
    ndjson = "ndjson"


class InstanceFormats(str, Enum):
    ndjson = "ndjson"


class InstanceTypes(str, Enum):
    node = "node"
    edge = "edge"


class CompressionFormat(str, Enum):
    gzip = "gzip"
    none = "none"


DEFAULT_DOWNLOAD_DIR = Path(DATA_DEFAULT_DIR)


class DownloadApp(typer.Typer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback(invoke_without_command=True)(self.download_main)
        self.command("raw")(self.download_raw_cmd)
        self.command("assets")(self.download_assets_cmd)
        self.command("hierarchy")(self.download_hierarchy_cmd)
        self.command("instances")(self.download_instances_cmd)

    @staticmethod
    def download_main(ctx: typer.Context) -> None:
        """Commands to download data from CDF into a temporary directory."""
        if ctx.invoked_subcommand is None:
            print("Use [bold yellow]cdf download --help[/] for more information.")
        return None

    @staticmethod
    def download_raw_cmd(
        ctx: typer.Context,
        tables: Annotated[
            list[str] | None,
            typer.Argument(
                help="List of tables to download. If not provided, an interactive selection will be made.",
            ),
        ] = None,
        database: Annotated[
            str | None,
            typer.Option(
                "--database",
                "-d",
                help="Database to download from. If not provided, the user will be prompted to select a database.",
            ),
        ] = None,
        file_format: Annotated[
            RawFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the raw tables in. Supported formats: ndjson, yaml",
            ),
        ] = RawFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the raw tables. Supported formats: gzip, none.",
            ),
        ] = CompressionFormat.gzip,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the raw tables.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of records to download from each table. Use -1 to download all records.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download RAW tables from CDF into a temporary directory."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        if tables and database:
            selectors = [RawTable(db_name=database, table_name=table) for table in tables]
        elif tables and not database:
            raise typer.BadParameter(
                "The '--database' option is required when specifying tables as arguments.",
                param_hint="--database",
            )
        elif not tables and database:
            selectors = RawTableInteractiveSelect(client, "download").select_tables(database=database)
        else:
            selectors = RawTableInteractiveSelect(client, "download").select_tables()

        cmd.run(
            lambda: cmd.download(
                selectors=[
                    RawTableSelector(table=SelectedTable(db_name=item.db_name, table_name=item.table_name))
                    for item in selectors
                ],
                io=RawIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_assets_cmd(
        ctx: typer.Context,
        data_sets: Annotated[
            list[str] | None,
            typer.Option(
                "--data-set",
                "-d",
                help="List of data sets to download assets from. If this and hierarchy are not provided, an interactive selection will be made.",
            ),
        ] = None,
        hierarchy: Annotated[
            list[str] | None,
            typer.Option(
                "--hierarchy",
                "-r",
                help="List of asset hierarchies to download assets from. If this and data sets are not provided, an interactive selection will be made.",
            ),
        ] = None,
        file_format: Annotated[
            AssetCentricFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the assets in.",
            ),
        ] = AssetCentricFormats.csv,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the assets.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the assets.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of assets to download from each dataset/hierarchy. Use -1 to download all assets.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download assets from CDF into a temporary directory."""
        client = EnvironmentVariables.create_from_environment().get_client()
        is_interactive = not data_sets and not hierarchy
        if is_interactive:
            interactive = AssetInteractiveSelect(client, "download assets")
            selector_type = interactive.select_hierarchies_or_data_sets()
            if selector_type == "Data Set":
                data_sets = interactive.select_data_sets()
            else:
                hierarchy = interactive.select_hierarchies()

        selectors: list[AssetCentricSelector] = []
        if data_sets:
            selectors.extend([DataSetSelector(data_set_external_id=ds, kind="Assets") for ds in data_sets])
        if hierarchy:
            selectors.extend([AssetSubtreeSelector(hierarchy=h, kind="Assets") for h in hierarchy])
        cmd = DownloadCommand()
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=AssetIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_hierarchy_cmd(
        ctx: typer.Context,
        hierarchy: Annotated[
            str | None,
            typer.Argument(
                help="The asset hierarchy to download.",
            ),
        ] = None,
        file_format: Annotated[
            HierarchyFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format for downloading the asset hierarchy.",
            ),
        ] = HierarchyFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the assets.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the asset hierarchy.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum number of resources to download for each type. Use -1 to download all assets.",
            ),
        ] = 100_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download an asset hierarchy from CDF into a temporary directory."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        if hierarchy is None:
            selector = AssetInteractiveSelect(client, "download")
            hierarchy = selector.select_hierarchy(allow_empty=False)

        selectors = [
            # MyPy cannot see that resource_type is one of the allowed literals.
            AssetSubtreeSelector(hierarchy=hierarchy, kind=resource_type)  # type: ignore[arg-type]
            for resource_type in ["Assets", "Events", "FileMetadata", "TimeSeries"]
        ]
        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=HierarchyIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )

    @staticmethod
    def download_instances_cmd(
        ctx: typer.Context,
        instance_space: Annotated[
            str | None,
            typer.Option(
                "--instance-space",
                "-s",
                help="The instance space to download instances from. If not provided, an interactive "
                "selection will be made.",
            ),
        ] = None,
        schema_space: Annotated[
            str | None,
            typer.Option(
                "--schema-space",
                "-c",
                help="The schema space where the views are located.",
            ),
        ] = None,
        view_external_ids: Annotated[
            list[str] | None,
            typer.Option(
                "--view",
                "-w",
                help="List of view external IDs to download properties for the "
                "instances. To specify version use a forward slash, e.g. viewExternalId/v1.",
            ),
        ] = None,
        instance_type: Annotated[
            InstanceTypes,
            typer.Option(
                "--instance-type",
                "-t",
                help="The type of instances to download.",
            ),
        ] = InstanceTypes.node,
        file_format: Annotated[
            InstanceFormats,
            typer.Option(
                "--format",
                "-f",
                help="Format to download the instances in.",
            ),
        ] = InstanceFormats.ndjson,
        compression: Annotated[
            CompressionFormat,
            typer.Option(
                "--compression",
                "-z",
                help="Compression format to use when downloading the instances.",
            ),
        ] = CompressionFormat.none,
        output_dir: Annotated[
            Path,
            typer.Option(
                "--output-dir",
                "-o",
                help="Where to download the instances.",
                allow_dash=True,
            ),
        ] = DEFAULT_DOWNLOAD_DIR,
        limit: Annotated[
            int,
            typer.Option(
                "--limit",
                "-l",
                help="The maximum the number of instances to download from each view. Use -1 to download all.",
            ),
        ] = 10_000,
        verbose: Annotated[
            bool,
            typer.Option(
                "--verbose",
                "-v",
                help="Turn on to get more verbose output when running the command",
            ),
        ] = False,
    ) -> None:
        """This command will download Instances from CDF into a temporary directory."""
        cmd = DownloadCommand()

        client = EnvironmentVariables.create_from_environment().get_client()
        if instance_space is None:
            selector = DataModelingSelect(client, "download instances")
            selected_instance_space = selector.select_instance_space(multiselect=False)
            selected_instance_type = selector.select_instance_type()
            selected_schema_space = selector.select_schema_space(
                include_global=True, message="In which space is the views with instance properties located?"
            ).space
            selected_views = selector.select_view(
                multiselect=True,
                space=selected_schema_space,
                message="Select views to download instance properties from.",
                include_global=True,
                instance_type=selected_instance_type,
            )
            selectors: list[InstanceSpaceSelector] = [
                InstanceSpaceSelector(
                    instance_space=selected_instance_space,
                    view=SelectedView(
                        space=selected_schema_space,
                        external_id=view.external_id,
                        version=view.version,
                    ),
                    instance_type=selected_instance_type,
                )
                for view in selected_views
            ]
        elif schema_space is None and view_external_ids is None:
            selectors = [InstanceSpaceSelector(instance_space=instance_space, instance_type=instance_type.value)]
        elif schema_space is not None and view_external_ids is not None:
            selectors = [
                InstanceSpaceSelector(
                    instance_space=instance_space,
                    view=SelectedView(
                        space=schema_space,
                        external_id=view_id_str.split("/", maxsplit=1)[0],
                        version=view_id_str.split("/", maxsplit=1)[1] if "/" in view_id_str else None,
                    ),
                    instance_type=instance_type.value,
                )
                for view_id_str in view_external_ids
            ]
        else:
            raise typer.BadParameter(
                "Both '--schema-space' and '--view' must be provided together.",
                param_hint="--view",
            )

        cmd.run(
            lambda: cmd.download(
                selectors=selectors,
                io=InstanceIO(client),
                output_dir=output_dir,
                file_format=f".{file_format.value}",
                compression=compression.value,
                limit=limit if limit != -1 else None,
                verbose=verbose,
            )
        )
