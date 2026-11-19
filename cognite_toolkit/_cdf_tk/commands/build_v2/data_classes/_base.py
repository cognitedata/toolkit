from pathlib import Path
from typing import Self

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, read_yaml_file


class YAMLFile(BaseModel):
    @classmethod
    def from_yaml(cls, yaml_str: str) -> Self:
        """Create an instance of the class from a YAML string."""
        data = read_yaml_content(yaml_str)
        return cls.model_validate(data)

    @classmethod
    def from_yaml_file(cls, file_path: Path) -> Self:
        """Create an instance of the class from a YAML file."""
        data = read_yaml_file(file_path, expected_output="dict")
        return cls.model_validate(data)
