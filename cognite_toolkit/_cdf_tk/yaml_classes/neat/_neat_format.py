from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast, get_args

from pydantic import BaseModel, ConfigDict, Discriminator, Field, PrivateAttr, Tag, field_validator, model_validator
from pydantic.fields import FieldInfo

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource

from ._constant import SHEET_CONTAINERS, SHEET_ENUM, SHEET_METADATA, SHEET_NODES, SHEET_PROPERTIES, SHEET_VIEWS
from ._spreadsheet_loader import _load_spreadsheet
from ._table import DMSContainer, DMSEnum, DMSNode, DMSProperty, DMSView, normalize_table_names
from ._table_source import NeatTableSource

Space = Annotated[str, Field(min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)]


class NeatMetadata(BaseModel):
    space: Space = Field(
        description="The Space identifier (id).",
    )
    external_id: str = Field(
        description="External-id of the data model.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="Version of the data model.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="Human readable name for the data model.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the data model.",
        max_length=1024,
    )
    creator: str | None = Field(None, description="The creator of the data model.", max_length=255)
    governed_spaces: list[Space] | None = Field(None, description="List of spaces governed by the data model.")

    @field_validator("governed_spaces", mode="before")
    @classmethod
    def _split_on_comma(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",")]
        return value

    @model_validator(mode="before")
    @classmethod
    def _key_value_pairs(cls, data: Any) -> Any:
        """Convert a list of key-value pairs to a dict."""
        if isinstance(data, list):
            output: dict[str, Any] = {}
            for item in data:
                if not isinstance(item, dict):
                    continue
                # Spreadsheets use 'Key'/'Value' columns, while YAML typically uses 'key'/'value'.
                normalized = {key.casefold() if isinstance(key, str) else key: value for key, value in item.items()}
                if "key" in normalized and "value" in normalized:
                    output[normalized["key"]] = normalized["value"]
            return output
        return data


class NeatYAML(ToolkitResource):
    """A data model described with the NEAT table YAML format."""

    metadata: NeatMetadata = Field(validation_alias=SHEET_METADATA)
    properties: list[DMSProperty] = Field(validation_alias=SHEET_PROPERTIES)
    views: list[DMSView] = Field(validation_alias=SHEET_VIEWS)
    containers: list[DMSContainer] = Field(default_factory=list, validation_alias=SHEET_CONTAINERS)
    enum: list[DMSEnum] = Field(default_factory=list, validation_alias=SHEET_ENUM)
    nodes: list[DMSNode] = Field(default_factory=list, validation_alias=SHEET_NODES)

    @model_validator(mode="before")
    @classmethod
    def _title_case_keys(cls, data: Any) -> Any:
        return normalize_table_names(data)

    def as_id(self) -> DataModelId:
        """Return the data model identifier declared in the Metadata table."""
        return DataModelId(
            space=self.metadata.space, external_id=self.metadata.external_id, version=self.metadata.version
        )

    @classmethod
    def get_sheet_columns(
        cls, sheet_id: str, sheet: FieldInfo | None = None, *, column_type: Literal["all", "required"] = "required"
    ) -> list[str]:
        if sheet_id not in cls.model_fields.keys():
            raise KeyError(f"Invalid field id: {sheet_id}")
        if sheet is None:
            sheet = cls.model_fields[sheet_id]
        if sheet.validation_alias == SHEET_METADATA:
            return ["Key", "Value"]
        return [
            # We know all fields has validation_alias because of the alias_generator in TableDMS
            cast(str, sheet_field.validation_alias)
            # All the fields in the sheet's model are lists.
            for sheet_field in get_args(sheet.annotation)[0].model_fields.values()
            if sheet_field.is_required() or column_type == "all"
        ]


class NeatSpreadsheet(ToolkitResource):
    model_config = ConfigDict(populate_by_name=True)

    spreadsheet: Path
    _loaded: tuple[NeatYAML, NeatTableSource] | None = PrivateAttr(default=None)

    @field_validator("spreadsheet", mode="before")
    @classmethod
    def convert_str_to_path(cls, value: Any) -> Any:
        # Strict validation rejects str for Path, but YAML always provides text.
        if isinstance(value, str):
            return Path(value)
        return value

    @field_validator("spreadsheet")
    @classmethod
    def validate_spreadsheet_suffix(cls, value: Path) -> Path:
        if value.suffix.casefold() != ".xlsx":
            raise ValueError("Spreadsheet must reference an .xlsx file.")
        return value

    @property
    def neat_yaml(self) -> NeatYAML:
        """Return the NEAT YAML representation of the spreadsheet."""
        return self._load_spreadsheet()[0]

    @property
    def table_source(self) -> NeatTableSource:
        """Return the source information for the spreadsheet."""
        return self._load_spreadsheet()[1]

    def _load_spreadsheet(self) -> tuple[NeatYAML, NeatTableSource]:
        if self._loaded is None:
            tables, source = _load_spreadsheet(self.spreadsheet)
            self._loaded = NeatYAML.model_validate(tables), source
        return self._loaded

    def as_id(self) -> DataModelId:
        return self.neat_yaml.as_id()

    @model_validator(mode="after")
    def _validate_spreadsheet(self) -> Self:
        """Load the spreadsheet and validate the NEAT tables."""
        self._load_spreadsheet()
        return self


def _discriminate_neat_format(value: Any) -> str | None:
    if isinstance(value, NeatYAML):
        return "yaml"
    if isinstance(value, NeatSpreadsheet):
        return "spreadsheet"
    if not isinstance(value, dict):
        return None
    if "spreadsheet" in value:
        return "spreadsheet"
    if "metadata" in {key.casefold() for key in value if isinstance(key, str)}:
        return "yaml"
    # Returning None makes pydantic raise the custom 'invalid_neat_format' error.
    return None


NeatFormat = Annotated[
    Annotated[NeatYAML, Tag("yaml")] | Annotated[NeatSpreadsheet, Tag("spreadsheet")],
    Discriminator(
        _discriminate_neat_format,
        custom_error_type="invalid_neat_format",
        custom_error_message="Invalid NEAT format. Expected either a 'Metadata' key (YAML format) or a "
        "'spreadsheet' key (spreadsheet format).",
    ),
]
