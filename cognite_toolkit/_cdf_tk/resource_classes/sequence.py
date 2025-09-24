from typing import Literal

from pydantic import Field, model_validator

from .base import BaseModelResource, ToolkitResource


class SequenceColumnDTO(BaseModelResource):
    external_id: str = Field(
        description="The external ID of the column.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the column.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="The description of the column.",
        max_length=1000,
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
        max_length=256,
    )
    value_type: Literal["STRING", "string", "DOUBLE", "double", "LONG", "long"] | None = Field(
        default=None,
        description="What type the datapoints in a column will have.",
    )


class SequenceYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the sequence.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="The description of the sequence.",
        max_length=1000,
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="The external ID of the data set that the sequence associated with.",
    )
    asset_external_id: str | None = Field(
        default=None,
        description="The external ID of the asset that the sequence associated with.",
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
        max_length=256,
    )
    columns: list[SequenceColumnDTO] = Field(
        description="List of column definitions.",
        min_length=1,
        max_length=400,
    )


class SequenceRowDTO(BaseModelResource):
    row_number: int = Field(
        description="The row number of the row.",
        ge=0,
    )
    values: list[str | int | float | None] = Field(
        description="List of values in the order defined in the columns field",
        min_length=1,
        max_length=400,
    )


class SequenceRowYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=256,
    )
    columns: list[str] = Field(
        description="List of column definitions.",
        min_length=1,
        max_length=200,
    )
    rows: list[SequenceRowDTO] = Field(
        description="List of row definitions.",
        min_length=1,
        max_length=10000,
    )

    @model_validator(mode="after")
    def validate_values_match_columns(self) -> "SequenceRowYAML":
        """Validate that the number of values in each row matches the number of columns."""
        total_columns = len(self.columns)
        for i, row in enumerate(self.rows):
            if len(row.values) != total_columns:
                raise ValueError(
                    f"Row number {row.row_number} has {len(row.values)} value(s). "
                    f"Each row must have exactly {total_columns} value(s) which is the same as the number of column(s)."
                )
        return self
