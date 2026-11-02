from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ResourceFileIO, ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._neat._reader import NeatTableError, NeatTableReader
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatFormat, NeatSpreadsheet, NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatTableSource

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning


@final
class NeatIO(ResourceFileIO[DataModelId, NeatYAML | NeatSpreadsheet]):
    folder_name = "data_modeling"
    kind = "Neat"
    yaml_cls = NeatFormat

    @classmethod
    def get_id(cls, item: dict) -> DataModelId:
        raise NotImplementedError()

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
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError

        if resource is None:
            yield BuildError(
                code="INVALID-VALUE",
                message="Neat requires valid syntax to be converted to API format.",
                source_file=source_file,
                alpha=Flags.NEAT.is_enabled(),
            )
            return

        if isinstance(resource, NeatSpreadsheet):
            neat_yaml = resource.neat_yaml
            source = resource.source
        else:
            neat_yaml = resource
            source = NeatTableSource(source=source_file.as_posix())

        reader = NeatTableReader(neat_yaml, source)
        try:
            results = reader.read()
        except NeatTableError as e:
            yield from reader.get_warnings()
            yield BuildError(
                code="INVALID-VALUE",
                message=str(e),
                source_file=source_file,
                alpha=Flags.NEAT.is_enabled(),
            )
            return
        yield from reader.get_warnings()
        yield results.space
        yield from results.containers
        yield from results.views
        yield from results.node_types
        yield results.data_model

    @classmethod
    def from_crud_types(cls, resources: Iterable[CrudResource[Identifier]]) -> dict[str, Any]:
        raise RuntimeError("Bug in Toolkit: Neat does not support conversion from API format to YAML format.")
