from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.resource_ios import ResourceFileIO, ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._neat._reader import NeatTableError, NeatTableReader
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatFormat, NeatSpreadsheet, NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._constant import SHEET_METADATA
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatYAMLSource

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning


@final
class NeatIO(ResourceFileIO[DataModelId, NeatYAML | NeatSpreadsheet]):
    folder_name = "data_modeling"
    kind = "Neat"
    yaml_cls = NeatFormat

    @classmethod
    def get_id(cls, item: dict) -> DataModelId:
        metadata = item.get(SHEET_METADATA)
        if not metadata or "space" not in metadata or "externalId" not in metadata or "version" not in metadata:
            raise KeyError("Neat resource must have a metadata section with space and externalId")
        return DataModelId(space=metadata["space"], external_id=metadata["externalId"], version=metadata["version"])

    @classmethod
    def as_crud_type(cls) -> ResourceType:
        raise RuntimeError("Bug in Toolkit: NeatIO is multiple crud types, use the to_crud_types methods instead")

    @classmethod
    def get_dependencies(cls, resource: NeatYAML | NeatSpreadsheet) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        return []

    @classmethod
    def to_crud_types(
        cls, raw: dict[str, Any], source_file: Path, resource: NeatYAML | NeatSpreadsheet | None = None
    ) -> "Iterable[CrudResource[Identifier] | BuildError | BuildWarning]":
        if resource is None:
            # We cannot convert to crud types without a valid NeatYAML or NeatSpreadsheet resource,
            # this already should have been validated and give errors.
            return

        if isinstance(resource, NeatSpreadsheet):
            reader = NeatTableReader(resource.neat_yaml, resource.table_source)
        else:
            reader = NeatTableReader(resource, NeatYAMLSource(source=source_file))

        try:
            results = reader.read()
        except NeatTableError:
            yield from reader.get_warnings()
            yield from reader.get_errors()
            return
        yield from reader.get_warnings()
        yield from reader.get_errors()
        if results.space.identifier.space in reader.governed_spaces:
            # Should always be true, but we check anyway to avoid returning resources outside the governed spaces.
            yield results.space
        for container in results.containers:
            if container.identifier.space in reader.governed_spaces:
                yield container
        for view in results.views:
            if view.identifier.space in reader.governed_spaces:
                yield view
        for node_type in results.node_types:
            if node_type.identifier.space in reader.governed_spaces:
                yield node_type
        if results.data_model.identifier.space in reader.governed_spaces:
            # Should always be true, but we check anyway to avoid returning resources outside the governed spaces.
            yield results.data_model

    @classmethod
    def from_crud_types(cls, resources: Iterable[CrudResource[Identifier]]) -> dict[str, Any]:
        raise RuntimeError("Bug in Toolkit: Neat does not support conversion from API format to YAML format.")
