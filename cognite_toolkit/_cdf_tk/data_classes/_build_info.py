from __future__ import annotations

from abc import abstractmethod
from collections.abc import Collection, Iterator, MutableSequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Generic, SupportsIndex, cast, overload

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError
from cognite_toolkit._cdf_tk.loaders import ResourceTypes, get_loader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, ResourceLoader, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    calculate_str_or_file_hash,
    load_yaml_inject_variables,
    safe_read,
)

from ._build_variables import BuildVariables


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
            "hash": self.hash,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> SourceLocation:
        return SourceLocationEager(
            path=Path(data["path"]),
            _hash=data["hash"],
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
    identifier: T_ID
    location: SourceLocation
    kind: str
    destination: Path | None

    @classmethod
    def load(cls, data: dict[str, Any], resource_folder: str) -> BuiltResource:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        kind = data["kind"]
        loader = cast(ResourceLoader, get_loader(resource_folder, kind))
        identifier = loader.get_id(data["identifier"])

        return cls(
            location=SourceLocation.load(data["location"]),
            kind=kind,
            identifier=identifier,
            destination=Path(data["destination"]) if "destination" in data else None,
        )

    def dump(self, resource_folder: str, include_destination: bool = False) -> dict[str, Any]:
        from cognite_toolkit._cdf_tk.loaders import ResourceLoader, get_loader

        loader = cast(ResourceLoader, get_loader(resource_folder, self.kind))
        dumped = loader.dump_id(self.identifier)

        output = {
            "identifier": dumped,
            "location": self.location.dump(),
            "kind": self.kind,
        }
        if include_destination and self.destination:
            output["destination"] = self.destination.as_posix()
        return output

    def create_full(self, module: BuiltModule, resource_dir: str) -> BuiltResourceFull[T_ID]:
        return BuiltResourceFull(
            identifier=self.identifier,
            location=self.location,
            kind=self.kind,
            destination=self.destination,
            build_variables=module.build_variables,
            module_name=module.name,
            module_location=module.location.path,
            resource_dir=resource_dir,
        )


@dataclass
class BuiltResourceFull(BuiltResource[T_ID]):
    build_variables: BuildVariables
    module_name: str
    module_location: Path
    resource_dir: str

    def load_resource_dict(
        self, environment_variables: dict[str, str | None], validate: bool = False
    ) -> dict[str, Any]:
        content = self.build_variables.replace(safe_read(self.location.path))
        loader = cast(ResourceLoader, get_loader(self.resource_dir, self.kind))
        raw = load_yaml_inject_variables(content, environment_variables, validate=validate)
        if isinstance(raw, dict):
            return raw
        elif isinstance(raw, list):
            for item in raw:
                if loader.get_id(item) == self.identifier:
                    return item
        raise ToolkitMissingResourceError(f"Resource {self.identifier} not found in {self.location.path}")

    def load_resource(
        self,
        environment_variables: dict[str, str | None],
        loader: type[
            ResourceLoader[
                T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
            ]
        ]
        | None = None,
    ) -> T_WriteClass:
        """Load the resource from the build info.

        Args:
            environment_variables: The environment variables to inject into the resource file.
            loader: The loader to use to load the resource. If not provided, the loader will be inferred from the resource directory and kind.
                The motivation to explicitly provide the loader is to get the correct type hints.
        """
        loader = loader or get_loader(self.resource_dir, self.kind)  # type: ignore[assignment]
        return loader.resource_write_cls.load(self.load_resource_dict(environment_variables))  # type: ignore[misc, union-attr]


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


@dataclass
class BuiltModule:
    name: str
    location: SourceLocation
    build_variables: BuildVariables
    resources: dict[str, BuiltResourceList]
    warning_count: int
    status: str

    @classmethod
    def load(cls, data: dict[str, Any]) -> BuiltModule:
        return cls(
            name=data["name"],
            location=SourceLocation.load(data["location"]),
            build_variables=BuildVariables.load(data["build_variables"]),
            resources={
                key: BuiltResourceList([BuiltResource.load(resource_data, key) for resource_data in resources_data])
                for key, resources_data in data["resources"].items()
            },
            warning_count=data.get("warning_count", 0),
            status=data.get("status", "Success"),
        )

    def dump(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "location": self.location.dump(),
            "build_variables": self.build_variables.dump(),
            "resources": {
                key: [resource.dump(key) for resource in resources] for key, resources in self.resources.items()
            },
            "warning_count": self.warning_count,
            "status": self.status,
        }


@dataclass
class BuiltModuleList(list, MutableSequence[BuiltModule]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[BuiltModule] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[BuiltModule]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> BuiltModule: ...

    @overload
    def __getitem__(self, index: slice) -> BuiltModuleList: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> BuiltModule | BuiltModuleList:
        if isinstance(index, slice):
            return BuiltModuleList(super().__getitem__(index))
        return super().__getitem__(index)

    def get_resources(self, id_type: type[T_ID], resource_dir: ResourceTypes, kind: str) -> BuiltFullResourceList[T_ID]:
        return BuiltFullResourceList[T_ID](
            [
                resource.create_full(module, resource_dir)
                for module in self
                for resource in module.resources.get(resource_dir, [])
                if resource.kind == kind
            ]
        )
