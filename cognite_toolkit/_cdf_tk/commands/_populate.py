import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd
from cognite.client.data_classes.data_modeling import (
    MappedProperty,
    NodeApply,
    NodeOrEdgeData,
    PropertyType,
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
from rich.markup import escape
from rich.progress import Progress

from build.functions.contextualization_connection_writer.handler import chunker
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import (
    CDFAPIError,
    ToolkitFileNotFoundError,
    ToolkitRequiredValueError,
    ToolkitResourceMissingError,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
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
        raise NotImplementedError()

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

        match property_type:
            case Text() | CDFExternalIdReference():
                return str(value)
            case Boolean():
                if isinstance(value, str):
                    return value.lower() in ("true", "1")
                return bool(value)
            case Timestamp():
                return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            case Date():
                return pd.Timestamp(value).strftime("%Y-%m-%d")
            case Json():
                return value
            case Float32() | Float64():
                return float(value)
            case Int32() | Int64():
                return int(value)
            case DirectRelation():
                if isinstance(value, str):
                    return {"space": instance_space, "externalId": value}
                return value
            case Enum():
                return next(
                    (opt for opt in property_type.values.keys() if opt.casefold() == str(value).casefold()),
                    property_type.unknown_value,
                )
            case _:
                return value
