from pathlib import Path

from pydantic import BaseModel


class BuildParameters(BaseModel):
    organization_dir: Path
    build_dir: Path
    build_env_name: str
