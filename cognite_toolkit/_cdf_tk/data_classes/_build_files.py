from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cognite_toolkit._cdf_tk.data_classes import (
    SourceLocation,
)
from cognite_toolkit._cdf_tk.loaders import (
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import (
    FileReadWarning,
)


@dataclass
class BuildSourceFile:
    source: SourceLocation
    content: str
    loaded: list[dict[str, Any]] | dict[str, Any] | None = None
    warnings: WarningList[FileReadWarning] = field(default_factory=WarningList[FileReadWarning])


@dataclass
class BuildDestinationFile:
    path: Path
    loaded: list[dict[str, Any]] | dict[str, Any]
    loader: type[ResourceLoader]
    source: SourceLocation
    extra_sources: list[SourceLocation] | None

    @property
    def content(self) -> str:
        return yaml.safe_dump(self.loaded, sort_keys=False)

