from __future__ import annotations

from collections.abc import Collection, Iterator, MutableSequence
from dataclasses import dataclass
from typing import Any, SupportsIndex, overload

from cognite_toolkit._cdf_tk.loaders import ResourceTypes
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID

from ._build_variables import BuildVariables
from ._built_resources import (
    BuiltFullResourceList,
    BuiltResource,
    BuiltResourceList,
    SourceLocation,
)


@dataclass
class BuiltModule:
    name: str
    location: SourceLocation
    build_variables: BuildVariables
    resources: dict[str, BuiltResourceList]
    warning_count: int
    status: str
    iteration: int

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
            iteration=data.get("iteration", 1),
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
            "iteration": self.iteration,
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

    def as_resources_by_folder(self) -> dict[str, BuiltResourceList[T_ID]]:
        resources_by_folder: dict[str, BuiltResourceList[T_ID]] = {}
        for module in self:
            for resource_dir, resources in module.resources.items():
                resources_by_folder.setdefault(resource_dir, BuiltResourceList()).extend(resources)
        return resources_by_folder
