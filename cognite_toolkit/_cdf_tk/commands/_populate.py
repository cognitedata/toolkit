from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, PropertyType, View
from cognite.client.exceptions import CogniteAPIError
from rich.markup import escape
from rich.progress import Progress

from build.functions.contextualization_connection_writer.handler import chunker
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import CDFAPIError
from cognite_toolkit._cdf_tk.utils.file import read_csv

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

        properties_by_column = self._properties_by_column(list(data.columns), config.view)
        property_types_by_column = self._property_types_by_column(list(data.columns), config.view)

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
                                        value, property_types_by_column[col]
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

    def _validate_config(
        self,
        view_id: list[str],
        table: Path | None,
        instance_space: str | None,
        external_id_column: str | None,
        client: ToolkitClient,
    ) -> PopulateConfig:
        raise NotImplementedError()

    def _properties_by_column(self, columns: list[str], view: View) -> dict[str, str]:
        raise NotImplementedError()

    def _property_types_by_column(self, columns: list[str], view: View) -> dict[str, PropertyType]:
        raise NotImplementedError()

    def _serialize_value(self, value: Any, property_type: PropertyType) -> Any:
        raise NotImplementedError()
