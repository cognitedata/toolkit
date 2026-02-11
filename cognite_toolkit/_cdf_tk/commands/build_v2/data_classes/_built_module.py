from pathlib import Path

from pydantic import ConfigDict, Field

from ._module import Module


class BuiltModule(Module):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    built_files: list[Path] = Field(default_factory=list)
