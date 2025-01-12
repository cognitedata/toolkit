from __future__ import annotations

from abc import abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterator, MutableSequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, SupportsIndex, TypeVar, cast, overload

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.loaders import get_loader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    calculate_str_or_file_hash,
    load_yaml_inject_variables,
    safe_read,
)

from ._build_variables import BuildVariables

if TYPE_CHECKING:
    from ._built_modules import BuiltModule


@dataclass
class SourceLocation:
    """This represents the location of a built resource in a module structure.

    Args:
        path: The relative path to the resource from the project directory.
    """

    path: Path

    @property
    @abstractmethod
    def hash(self) -> str:
        """The hash of the resource file."""
        raise NotImplementedError()

    def dump(self) -> dict[str, Any]:
        return {
            "path": self.path.as_posix(),
            "hash": str(self.hash),
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> SourceLocation:
        return SourceLocationEager(
            path=Path(data["path"]),
            _hash=str(data["hash"]),
        )


@dataclass
class SourceLocationLazy(SourceLocation):
    absolute_path: Path

    @cached_property
    def hash(self) -> str:
        if self.absolute_path.is_dir():
            return calculate_directory_hash(self.absolute_path, shorten=True)
        else:
            return calculate_str_or_file_hash(self.absolute_path, shorten=True)


@dataclass
class SourceLocationEager(SourceLocation):
    _hash: str

    @property
    def hash(self) -> str:
        return self._hash


@dataclass
class BuiltResource(Generic[T_ID]):
    """This represents a built resource.

    Args:
        identifier: The unique identifier of the resource.
        source: The source location of the resource.
        kind: The kind of resource.
        destination: The destination of the resource.
        extra_sources: Extra source locations of the resource, for example, Transformations might have
            .sql files that are used to build the final resource.

    """

    identifier: T_ID
    source: SourceLocation
    kind: str
    destination: Path | None
    extra_sources: list[SourceLocation] | None

    @classmethod
    def load(cls: type[T_BuiltResource], data: dict[str, Any], resource_folder: str) -> T_BuiltResource:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        kind = data["kind"]
        loader = cast(ResourceLoader, get_loader(resource_folder, kind))
        identifier = loader.get_id(data["identifier"])

        return cls(
            source=SourceLocation.load(data["source"]),
            kind=kind,
            identifier=identifier,
            destination=Path(data["destination"]) if "destination" in data else None,
            extra_sources=[SourceLocation.load(source) for source in data.get("extra_sources", [])] or None,
        )

    def dump(self, resource_folder: str, include_destination: bool = False) -> dict[str, Any]:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        loader = cast(ResourceLoader, get_loader(resource_folder, self.kind))
        dumped = loader.dump_id(self.identifier)

        output: dict[str, Any] = {
            "identifier": dumped,
            "source": self.source.dump(),
            "kind": self.kind,
        }
        if include_destination and self.destination:
            output["destination"] = self.destination.as_posix()
        if self.extra_sources:
            output["extra_sources"] = [source.dump() for source in self.extra_sources]
        return output

    def create_full(self, module: BuiltModule, resource_dir: str) -> BuiltResourceFull[T_ID]:
        return BuiltResourceFull(
            identifier=self.identifier,
            source=self.source,
            kind=self.kind,
            destination=self.destination,
            build_variables=module.build_variables,
            module_name=module.name,
            module_location=module.location.path,
            resource_dir=resource_dir,
            extra_sources=self.extra_sources,
        )


T_BuiltResource = TypeVar("T_BuiltResource", bound=BuiltResource)


@dataclass
class BuiltResourceFull(BuiltResource[T_ID]):
    build_variables: BuildVariables
    module_name: str
    module_location: Path
    resource_dir: str

    def load_resource_dict(
        self, environment_variables: dict[str, str | None], validate: bool = False
    ) -> dict[str, Any]:
        content = self.build_variables.replace(safe_read(self.source.path))
        loader = cast(ResourceLoader, get_loader(self.resource_dir, self.kind))
        raw = load_yaml_inject_variables(
            content,
            environment_variables,
            validate=validate,
            original_filepath=self.source.path,
        )
        if isinstance(raw, dict):
            return raw
        elif isinstance(raw, list):
            for item in raw:
                if loader.get_id(item) == self.identifier:
                    return item
        raise ToolkitMissingResourceError(f"Resource {self.identifier} not found in {self.source.path}")


class BuiltResourceList(list, MutableSequence[BuiltResource[T_ID]], Generic[T_ID]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[BuiltResource[T_ID]] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[BuiltResource[T_ID]]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> BuiltResource[T_ID]: ...

    @overload
    def __getitem__(self, index: slice) -> BuiltResourceList[T_ID]: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> BuiltResource[T_ID] | BuiltResourceList[T_ID]:
        if isinstance(index, slice):
            return BuiltResourceList[T_ID](super().__getitem__(index))
        return super().__getitem__(index)

    @property
    def identifiers(self) -> list[T_ID]:
        return [resource.identifier for resource in self]

    @classmethod
    def load(cls, data: list[dict[str, Any]], resource_folder: str) -> BuiltResourceList[T_ID]:
        return cls([BuiltResource.load(resource_data, resource_folder) for resource_data in data])

    def dump(self, resource_folder: str, include_destination: bool = False) -> list[dict[str, Any]]:
        return [resource.dump(resource_folder, include_destination) for resource in self]

    def get_resource_directories(self, resource_folder: str) -> set[Path]:
        output: set[Path] = set()
        for resource in self:
            index = next((i for i, part in enumerate(resource.source.path.parts) if part == resource_folder), None)
            if index is None:
                continue
            path = Path("/".join(resource.source.path.parts[: index + 1]))
            output.add(path)

        return output


class BuiltFullResourceList(BuiltResourceList[T_ID]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[BuiltResourceFull[T_ID]] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[BuiltResourceFull[T_ID]]:
        return cast(Iterator[BuiltResourceFull[T_ID]], super().__iter__())

    @overload
    def __getitem__(self, index: SupportsIndex) -> BuiltResourceFull[T_ID]: ...

    @overload
    def __getitem__(self, index: slice) -> BuiltFullResourceList[T_ID]: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> BuiltResourceFull[T_ID] | BuiltFullResourceList[T_ID]:
        if isinstance(index, slice):
            return BuiltFullResourceList[T_ID](super().__getitem__(index))
        return cast(BuiltResourceFull[T_ID], super().__getitem__(index))

    def by_file(self) -> dict[Path, BuiltFullResourceList[T_ID]]:
        resources_by_file: dict[Path, BuiltFullResourceList[T_ID]] = defaultdict(lambda: BuiltFullResourceList())
        for resource in self:
            resources_by_file[resource.source.path].append(resource)
        return resources_by_file
