from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class BuiltModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="Name of the module, typically the name of the folder containing the module.")
    built_files: list[Path] = Field(default_factory=list)
