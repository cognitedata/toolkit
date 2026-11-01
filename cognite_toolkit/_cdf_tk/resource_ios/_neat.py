from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ResourceFileIO, ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.yaml_classes import NeatYAML

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning


@final
class NeatIO(ResourceFileIO[DataModelId, NeatYAML]):
    @classmethod
    def get_id(cls, item: dict) -> DataModelId:
        raise NotImplementedError("Not implemented yet")

    @classmethod
    def as_crud_type(cls) -> ResourceType:
        raise RuntimeError("Bug in Toolkit: NeatIO is multiple crud types, use the to_crud_types methods instead")

    @classmethod
    def get_dependencies(cls, resource: NeatYAML) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        raise NotImplementedError("Not implemented yet")

    @classmethod
    def to_crud_types(
        cls, raw: dict[str, Any], source_file: Path, resource: NeatYAML | None = None
    ) -> "Iterable[CrudResource[Identifier] | BuildError | BuildWarning]":
        if resource is None:
            from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError

            yield BuildError(
                code="INVALID-VALUE",
                message="Neat requires valid YAML syntax to be converted to API format.",
                source_file=source_file,
                alpha=Flags.NEAT.is_enabled(),
            )
            return
        raise NotImplementedError("Neat conversion to CRUD types is not yet implemented")

    @classmethod
    def from_crud_types(cls, resources: Iterable[CrudResource[Identifier]]) -> dict[str, Any]:
        raise RuntimeError("Bug in Toolkit: Neat does not support conversion from API format to YAML format.")
