import shutil
import sys
from collections import defaultdict
from collections.abc import Collection, Iterator, Sequence
from functools import cached_property
from pathlib import Path
from typing import Any, SupportsIndex, overload

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, resource_folder_from_path

from ._module_toml import ModuleToml

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ModuleLocationModel(BaseModel):
    """This represents the location of a module in a directory structure.

    Args:
        dir: The absolute path to the module directory.
        source_absolute_path: The absolute path to the source directory.
        source_paths: The paths to all files in the module.
        is_selected: Whether the module is selected by the user.
        definition: The module definition loaded from module.toml.
    """

    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    dir: Path
    source_absolute_path: Path
    source_paths: list[Path] = Field(default_factory=list)
    is_selected: bool = False
    definition: ModuleToml | None = None

    @field_validator("dir", "source_absolute_path")
    @classmethod
    def validate_paths_exist(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Path does not exist: {v}")
        if not v.is_absolute():
            raise ValueError(f"Path must be absolute: {v}")
        return v

    @field_validator("dir")
    @classmethod
    def validate_dir_is_directory(cls, v: Path) -> Path:
        if not v.is_dir():
            raise ValueError(f"Module directory must be a directory: {v}")
        return v

    @field_validator("source_paths")
    @classmethod
    def validate_source_paths(cls, v: list[Path], info: Any) -> list[Path]:
        if not info.data:
            return v

        module_dir = info.data.get("dir")
        if not module_dir:
            return v

        for path in v:
            if not path.is_absolute():
                raise ValueError(f"Source path must be absolute: {path}")
            try:
                path.relative_to(module_dir)
            except ValueError:
                raise ValueError(
                    f"Source path {path} is not within module directory {module_dir}. "
                    "All source paths must be within the module directory."
                )
        return v

    @model_validator(mode="after")
    def validate_module_structure(self) -> "ModuleLocationModel":
        if not self.resource_directories:
            raise ValueError(
                f"Module at {self.dir} has no valid resource directories. "
                "A module must contain at least one subdirectory matching a resource type "
                "(e.g., 'data_models', 'transformations', 'spaces', etc.)."
            )
        return self

    @property
    def has_example_data(self) -> bool:
        return bool(self.definition and self.definition.data)

    @property
    def name(self) -> str:
        return self.dir.name

    @property
    def title(self) -> str | None:
        return self.definition.title if self.definition else None

    @property
    def module_id(self) -> str | None:
        return self.definition.id if self.definition else None

    @property
    def package_id(self) -> str | None:
        return self.definition.package_id if self.definition else None

    @property
    def relative_path(self) -> Path:
        try:
            return self.dir.relative_to(self.source_absolute_path)
        except ValueError as e:
            raise ValueError(
                f"Module directory {self.dir} is not within source directory {self.source_absolute_path}"
            ) from e

    @property
    def module_selections(self) -> set[str | Path]:
        return {self.name, self.relative_path, *self.parent_relative_paths}

    @property
    def is_selected_by_default(self) -> bool:
        return self.definition.is_selected_by_default if self.definition else False

    @cached_property
    def parent_relative_paths(self) -> set[Path]:
        return set(self.relative_path.parents)

    @cached_property
    def hash(self) -> str:
        return calculate_directory_hash(self.dir, shorten=True)

    @cached_property
    def resource_directories(self) -> set[str]:
        source_path_by_resource_folder, _ = self._source_paths_by_resource_folder
        return set(source_path_by_resource_folder.keys())

    @property
    def _source_paths_by_resource_folder(self) -> tuple[dict[str, list[Path]], set[str]]:
        source_paths_by_resource_folder = defaultdict(list)
        invalid_resource_directory: set[str] = set()

        for filepath in self.source_paths:
            try:
                resource_folder = resource_folder_from_path(filepath)
            except ValueError:
                relative_to_module = filepath.relative_to(self.dir)
                is_file_in_resource_folder = relative_to_module.parts[0] == filepath.name
                if not is_file_in_resource_folder:
                    invalid_resource_directory.add(relative_to_module.parts[0])
                continue

            if filepath.is_file():
                source_paths_by_resource_folder[resource_folder].append(filepath)

        return source_paths_by_resource_folder, invalid_resource_directory

    @cached_property
    def source_paths_by_resource_folder(self) -> dict[str, list[Path]]:
        source_paths_by_resource_folder, _ = self._source_paths_by_resource_folder

        for filepaths in source_paths_by_resource_folder.values():

            def sort_key(p: Path) -> tuple[int, int, str]:
                first = {".yaml": 0, ".yml": 0}.get(p.suffix.lower(), 1)
                if result := INDEX_PATTERN.search(p.stem):
                    return first, int(result.group()[:-1]), p.name
                else:
                    return first, len(filepaths) + 1, p.name

            filepaths.sort(key=sort_key)

        return source_paths_by_resource_folder

    @cached_property
    def not_resource_directories(self) -> set[str]:
        return self._source_paths_by_resource_folder[1]

    @cached_property
    def dependencies(self) -> set[str]:
        return set(self.definition.dependencies) if self.definition else set()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name}, "
            f"is_selected={self.is_selected}, "
            f"file_count={len(self.source_paths)})"
        )

    def __str__(self) -> str:
        return self.name

    def as_read_module(self) -> "ReadModuleModel":
        return ReadModuleModel(
            dir=self.dir,
            resource_directories=tuple(self.resource_directories),
            module_id=self.module_id,
            package_id=self.package_id,
        )

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        module_dir: Path,
        source_paths: list[Path],
        user_selected_modules: set[str | Path] | None = None,
    ) -> Self:
        user_selected_modules = user_selected_modules or {Path("")}
        relative_module_dir = module_dir.relative_to(organization_dir)

        module_toml: ModuleToml | None = None
        module_toml_path = module_dir / ModuleToml.filename
        if module_toml_path.exists():
            try:
                module_toml = ModuleToml.load(module_toml_path)
            except Exception as e:
                raise ValueError(
                    f"Failed to load {ModuleToml.filename} from {module_dir}: {e}. "
                    "Please check the file format and ensure it's valid TOML."
                ) from e

        is_selected = cls._is_selected_module(relative_module_dir, user_selected_modules)

        try:
            return cls(
                dir=module_dir,
                source_absolute_path=organization_dir,
                source_paths=source_paths,
                is_selected=is_selected,
                definition=module_toml,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create module from {module_dir}: {e}. "
                "Please ensure the module directory structure is correct."
            ) from e

    @staticmethod
    def _is_selected_module(relative_module_dir: Path, user_selected: set[str | Path]) -> bool:
        return (
            relative_module_dir.name in user_selected
            or relative_module_dir in user_selected
            or any(parent in user_selected for parent in relative_module_dir.parents)
        )


class ReadModuleModel(BaseModel):
    """This is a short representation of a module.

    Args:
        dir: The absolute path to the module directory.
        resource_directories: The resource directories in the module.
        module_id: The ID of the module.
        package_id: The ID of the package.
    """

    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    dir: Path
    resource_directories: tuple[str, ...] = Field(default_factory=tuple)
    module_id: str | None = None
    package_id: str | None = None

    @field_validator("dir")
    @classmethod
    def validate_dir_exists(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Module directory does not exist: {v}")
        if not v.is_dir():
            raise ValueError(f"Module directory must be a directory: {v}")
        return v

    def resource_dir_path(self, resource_folder: str) -> Path | None:
        for resource_dir in self.resource_directories:
            if resource_dir == resource_folder and (resource_path := self.dir / resource_folder).exists():
                return resource_path
        return None

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        try:
            return cls(
                dir=Path(data["dir"]),
                resource_directories=tuple(data.get("resource_directories", [])),
                module_id=data.get("module_id"),
                package_id=data.get("package_id"),
            )
        except Exception as e:
            raise ValueError(f"Failed to load ReadModuleModel from data: {e}") from e

    def dump(self) -> dict[str, Any]:
        return {
            "dir": self.dir.as_posix(),
            "resource_directories": list(self.resource_directories),
            "module_id": self.module_id,
            "package_id": self.package_id,
        }


class ModuleDirectoriesModel(tuple, Sequence[ModuleLocationModel]):
    def __new__(cls, collection: Collection[ModuleLocationModel] | None) -> Self:
        return super().__new__(cls, tuple(collection or []))

    def __init__(self, collection: Collection[ModuleLocationModel] | None) -> None:
        super().__init__()

    @cached_property
    def available(self) -> set[str | Path]:
        return {selection for module_location in self for selection in module_location.module_selections}

    @cached_property
    def selected(self) -> "ModuleDirectoriesModel":
        return ModuleDirectoriesModel([module for module in self if module.is_selected])

    @cached_property
    def available_paths(self) -> set[Path]:
        return {item for item in self.available if isinstance(item, Path)}

    @cached_property
    def available_names(self) -> set[str]:
        return {item for item in self.available if isinstance(item, str)}

    @classmethod
    def load(
        cls,
        organization_dir: Path,
        user_selected_modules: set[str | Path] | None = None,
    ) -> Self:
        from cognite_toolkit._cdf_tk.utils import iterate_modules

        user_selected_modules = user_selected_modules or {Path("")}

        module_locations: list[ModuleLocationModel] = []
        errors: list[str] = []

        try:
            for module, source_paths in iterate_modules(organization_dir):
                try:
                    module_location = ModuleLocationModel.load(
                        organization_dir=organization_dir,
                        module_dir=module,
                        source_paths=source_paths,
                        user_selected_modules=user_selected_modules,
                    )
                    module_locations.append(module_location)
                except ValueError as e:
                    errors.append(f"Module {module}: {e}")
                except Exception as e:
                    errors.append(
                        f"Unexpected error loading module {module}: {e}. "
                        "Please check the module structure and file permissions."
                    )
        except Exception as e:
            raise ValueError(
                f"Failed to iterate modules in {organization_dir}: {e}. "
                "Please ensure the organization directory exists and is accessible."
            ) from e

        if errors and not module_locations:
            error_msg = "\n".join(f"  - {error}" for error in errors)
            raise ValueError(
                f"No valid modules found in {organization_dir}. Encountered the following errors:\n{error_msg}"
            )

        return cls(module_locations)

    def dump(self, organization_dir: Path) -> None:
        """Dumps the module directories to the source directory.

        Args:
            organization_dir: The absolute path to the source directory.
        """
        for module in self:
            module_dir = organization_dir / module.relative_path
            module_dir.mkdir(parents=True, exist_ok=True)
            for source_file in module.source_paths:
                relative_file_path = source_file.relative_to(module.dir)
                absolute_file_path = module_dir / relative_file_path
                absolute_file_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_file, absolute_file_path)

    def as_path_by_name(self) -> dict[str, list[Path]]:
        module_path_by_name: dict[str, list[Path]] = defaultdict(list)
        for module in self:
            module_path_by_name[module.name].append(module.relative_path)
        return module_path_by_name

    def __iter__(self) -> Iterator[ModuleLocationModel]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> ModuleLocationModel: ...

    @overload
    def __getitem__(self, index: slice) -> "ModuleDirectoriesModel": ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> "ModuleLocationModel | ModuleDirectoriesModel":
        if isinstance(index, slice):
            return ModuleDirectoriesModel(super().__getitem__(index))
        return super().__getitem__(index)
