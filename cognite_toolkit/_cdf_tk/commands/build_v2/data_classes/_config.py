import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, TypeAlias

from pydantic import BaseModel, Field, JsonValue, field_validator
from pydantic_core.core_schema import ValidationInfo

from ._base import YAMLFile
from ._types import ValidationType


def _normalize_config_variables(value: Any) -> Any:
    """Coerce YAML-parsed date/datetime values to strings for JSON-compatible config variables."""
    if isinstance(value, datetime | date):
        return str(value)
    if isinstance(value, dict):
        return {_normalize_config_variables(key): _normalize_config_variables(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_config_variables(item) for item in value]
    return value


DeployProtection: TypeAlias = Literal["none", "protected"]


class Environment(BaseModel):
    name: str = "dev"
    project: str = Field(default_factory=lambda: os.environ.get("CDF_PROJECT", "UNKNOWN"))
    validation_type: ValidationType = Field("dev", alias="validation-type")
    selection_type: Literal["path", "git-diff"] = Field("path", alias="selection-type")
    deploy_branch: str | None = Field(None, alias="deploy-branch")
    deploy_protection: DeployProtection | None = Field(None, alias="deploy-protection")
    selected: list[str] | None = None

    @field_validator("selection_type", mode="after")
    @classmethod
    def _require_validation_type_dev(
        cls, value: Literal["path", "git-diff"], info: ValidationInfo
    ) -> Literal["path", "git-diff"]:
        if value == "git-diff" and info.data.get("validation_type") != "dev":
            raise ValueError("`selection-type` can only be set to 'git-diff' when `validation-type` is 'dev'")
        return value


class ConfigYAML(YAMLFile):
    filename: ClassVar[str] = "config.{name}.yaml"

    environment: Environment = Field(default_factory=Environment)
    variables: dict[str, JsonValue] | None = None

    @field_validator("variables", mode="before")
    @classmethod
    def _normalize_variables(cls, value: Any) -> Any:
        if value is None:
            return value
        return _normalize_config_variables(value)

    @classmethod
    def get_filename(cls, name: str) -> str:
        return cls.filename.format(name=name)

    @classmethod
    def get_filepath(cls, organization_dir: Path, name: str) -> Path:
        return organization_dir / cls.get_filename(name)
