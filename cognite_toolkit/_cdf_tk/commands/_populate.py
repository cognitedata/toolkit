import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
import questionary
import typer
from cognite.client.data_classes.data_modeling import (
    DataModel,
    MappedProperty,
    NodeApply,
    NodeOrEdgeData,
    PropertyType,
    Space,
    View,
    ViewId,
)
from cognite.client.data_classes.data_modeling.data_types import (
    Boolean,
    CDFExternalIdReference,
    Date,
    DirectRelation,
    Enum,
    Float32,
    Float64,
    Int32,
    Int64,
    Json,
    ListablePropertyType,
    Text,
    Timestamp,
)
from cognite.client.exceptions import CogniteAPIError
from questionary import Choice
from rich import print
from rich.markup import escape
from rich.progress import Progress

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import (
    CDFAPIError,
    ToolkitFileNotFoundError,
    ToolkitRequiredValueError,
    ToolkitResourceMissingError,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker, humanize_collection
from cognite_toolkit._cdf_tk.utils.file import get_table_columns, read_csv

from ._base import ToolkitCommand


@dataclass
class PopulateConfig:
    view: View
    table: Path
    instance_space: str
    external_id_column: str


class PopulateCommand(ToolkitCommand):
    def view(
        self,
        client: ToolkitClient,
        view_id: list[str] | None = None,
        table: Path | None = None,
        instance_space: str | None = None,
        external_id_column: str | None = None,
        verbose: bool = False,
    ) -> None:
        if view_id is None:
            config = self._get_config_from_user(client)
        else:
            config = self._validate_config(view_id, table, instance_space, external_id_column, client)

        if config.table.suffix == ".csv":
            data = read_csv(config.table)
        else:
            # Parquet - already validated
            data = pd.read_parquet(config.table)

        properties_by_column, property_types_by_column = self._properties_by_column(list(data.columns), config.view)

        with Progress() as progress:
            task = progress.add_task("Populating view", total=len(data))
            for chunk in chunker(data.to_dict(orient="records"), 1_000):
                nodes = [
                    NodeApply(
                        space=config.instance_space,
                        external_id=row[config.external_id_column],
                        sources=[
                            NodeOrEdgeData(
                                source=config.view.as_id(),
                                properties={
                                    properties_by_column[col]: self._serialize_value(
                                        value, property_types_by_column[col], config.instance_space
                                    )
                                    for col, value in row.items()
                                    if col in properties_by_column and col in property_types_by_column
                                },
                            )
                        ],
                    )
                    for row in chunk
                ]
                try:
                    created = client.data_modeling.instances.apply(nodes=nodes, auto_create_direct_relations=True)
                except CogniteAPIError as e:
                    raise CDFAPIError(f"Failed to populate view; {escape(str(e))}")
                else:
                    progress.update(task, advance=len(created.nodes))

    def _get_config_from_user(self, client: ToolkitClient) -> PopulateConfig:
        view = self._get_view_from_user(client)
        table = self._get_table_from_user()
        instance_space = self._get_instance_space_from_user(client)
        external_id_column = self._get_external_id_column_from_user(table)
        return PopulateConfig(
            view=view,
            table=table,
            instance_space=instance_space,
            external_id_column=external_id_column,
        )

    @staticmethod
    def _get_view_from_user(client: ToolkitClient) -> View:
        data_models = client.data_modeling.data_models.list(inline_views=False, limit=-1, all_versions=False)
        data_model_choices = [
            Choice(f"{dm.as_id().as_tuple()}", value=dm)
            for dm in sorted(data_models, key=lambda dm: (dm.space, dm.external_id))
        ]
        selected_data_model: DataModel[ViewId] | None = questionary.select(
            "Select the data model containing the view to populate",
            choices=data_model_choices,
        ).ask()

        if selected_data_model is None:
            print("No data model selected. Exiting.")
            raise typer.Exit(0)

        view_options = [
            Choice(view.external_id, value=view)
            for view in sorted(selected_data_model.views, key=lambda v: v.external_id, reverse=True)
        ]
        selected_view: ViewId | None = questionary.select(
            "Select the view to populate",
            choices=view_options,
        ).ask()
        if selected_view is None:
            print("No view selected. Exiting.")
            raise typer.Exit(0)
        view = client.data_modeling.views.retrieve(selected_view)
        return view[0]

    def _get_table_from_user(self) -> Path:
        selected_table: str | None = questionary.path("Enter the path to the table to populate the view with").ask()
        if selected_table is None:
            print("No table path provided. Exiting.")
            raise typer.Exit(0)
        table_path = Path(selected_table)
        if not table_path.exists():
            print("Table path does not exist.")
            return self._get_table_from_user()
        if table_path.suffix not in (".csv", ".parquet"):
            print("Only CSV and Parquet files are supported. Please provide a valid file.")
            return self._get_table_from_user()
        return table_path

    @staticmethod
    def _get_instance_space_from_user(client: ToolkitClient) -> str:
        spaces = client.data_modeling.spaces.list(limit=-1)
        space_choices = [Choice(space.space, value=space) for space in sorted(spaces, key=lambda s: s.space)]
        selected_space: Space | None = questionary.select(
            "Select the instance space to write the nodes to", choices=space_choices
        ).ask()
        if selected_space is None:
            print("No instance space selected. Exiting.")
            raise typer.Exit(0)
        return selected_space.space

    @staticmethod
    def _get_external_id_column_from_user(table: Path) -> str:
        columns = get_table_columns(table)
        selected_column: str | None = questionary.select(
            "Select the column in the table that contains the external IDs of the nodes",
            choices=[Choice(col, value=col) for col in columns],
        ).ask()
        if selected_column is None:
            print("No external ID column selected. Exiting.")
            raise typer.Exit(0)
        return selected_column

    @staticmethod
    def _validate_config(
        user_view_id: list[str],
        table: Path | None,
        instance_space: str | None,
        external_id_column: str | None,
        client: ToolkitClient,
    ) -> PopulateConfig:
        if missing := [name for name, value in locals().items() if value is None]:
            raise ToolkitRequiredValueError(f"Missing required values: {humanize_collection(missing)}")
        # Happy Mypy
        instance_space = cast(str, instance_space)
        table = cast(Path, table)
        external_id_column = cast(str, external_id_column)

        if not table.exists():
            raise ToolkitFileNotFoundError(f"Table {table.as_posix()} not found", table)
        columns = {col.casefold() for col in get_table_columns(table)}
        if external_id_column.casefold() not in columns:
            raise ToolkitRequiredValueError(
                f"External ID column {external_id_column!r} not found in table {table.name}", external_id_column
            )

        view_id = ViewId.load(tuple(user_view_id))  # type: ignore[arg-type]
        try:
            views = client.data_modeling.views.retrieve(view_id)
        except CogniteAPIError as e:
            raise CDFAPIError(f"Failed to retrieve view {view_id:!r}; {escape(str(e))}")
        if not views:
            raise ToolkitResourceMissingError(f"View {view_id} not found", repr(view_id))
        view = max(views, key=lambda v: v.created_time)
        try:
            space = client.data_modeling.spaces.retrieve(instance_space)
        except CogniteAPIError as e:
            raise CDFAPIError(f"Failed to retrieve instance space {instance_space!r}; {escape(str(e))}")
        if space is None:
            raise ToolkitResourceMissingError(f"Instance space {instance_space} not found", repr(instance_space))

        return PopulateConfig(
            view=view, table=table, instance_space=instance_space, external_id_column=external_id_column
        )

    @staticmethod
    def _properties_by_column(columns: list[str], view: View) -> tuple[dict[str, str], dict[str, PropertyType]]:
        properties_by_column: dict[str, str] = {}
        property_types_by_column: dict[str, PropertyType] = {}
        container_property_by_id = {
            prop_id.casefold(): (prop_id, prop)
            for prop_id, prop in view.properties.items()
            if isinstance(prop, MappedProperty)
        }
        for col in columns:
            if col.casefold() not in container_property_by_id:
                continue
            prop_id, prop = container_property_by_id[col.casefold()]
            properties_by_column[col] = prop_id
            property_types_by_column[col] = prop.type
        return properties_by_column, property_types_by_column

    @classmethod
    def _serialize_value(cls, value: Any, property_type: PropertyType, instance_space: str) -> Any:
        if isinstance(value, str):
            try:
                return cls._serialize_value(json.loads(value), property_type, instance_space)
            except json.JSONDecodeError:
                ...
        elif isinstance(property_type, ListablePropertyType) and property_type.is_list and isinstance(value, list):
            return [cls._serialize_value(v, property_type, instance_space) for v in value]
        elif isinstance(property_type, ListablePropertyType) and property_type.is_list and not isinstance(value, list):
            return [cls._serialize_value(value, property_type, instance_space)]

        if value is None:
            return None

        match (property_type, value):
            case (Text() | CDFExternalIdReference(), _):
                return str(value)
            case (Boolean(), str()):
                return value.lower() in ("true", "1")
            case (Boolean(), _):
                return bool(value)
            case (Timestamp(), _):
                return pd.Timestamp(value).to_pydatetime().isoformat(timespec="milliseconds")
            case (Date(), _):
                return pd.Timestamp(value).strftime("%Y-%m-%d")
            case (Json(), dict() | list() | str()):
                return value
            case (Float32() | Float64(), _):
                float_value = float(value)
                if math.isinf(float_value) or math.isnan(float_value):
                    return None
                return float_value
            case (Int32() | Int64(), _):
                try:
                    return int(value)
                except ValueError:
                    return None
            case (DirectRelation(), _str):
                return {"space": instance_space, "externalId": value}
            case (DirectRelation(), _):
                return value
            case (Enum(), _):
                return next(
                    (
                        opt
                        for opt in property_type.values.keys()
                        if opt.casefold() == str(value).casefold()
                        and str(value).casefold() != (property_type.unknown_value or "").casefold()
                    ),
                    None,
                )
            case _:
                return value
