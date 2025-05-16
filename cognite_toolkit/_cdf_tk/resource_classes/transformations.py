from typing import Literal

from pydantic import Field, ValidationError, model_validator

from .base import ToolkitResource


class DataModelInfo(ToolkitResource):
    space: str = Field(description="Space of the Data Model.")
    external_id: str = Field(description="External ID of the Data Model.")
    version: str = Field(description="Version of the Data Model.")
    destination_type: str = Field(description="External ID of the type(view) in the data model.")
    destination_relationship_from_type: str | None = Field(
        default=None, description="Property Identifier of the connection definition in destinationType."
    )


class ViewInfo(ToolkitResource):
    space: str = Field(description="Space of the view.")
    external_id: str = Field(description="External ID of the view.")
    version: str = Field(description="Version of the view.")


class EdgeType(ToolkitResource):
    space: str = Field(description="Space of the type.")
    external_id: str = Field(description="External ID of the type.")


class DestinationSourceType(ToolkitResource):
    type: Literal[
        "assets",
        "events",
        "asset_hierarchy",
        "datapoints",
        "string_datapoints",
        "timeseries",
        "sequences",
        "files",
        "labels",
        "relationships",
        "data_sets",
        "instances",
        "nodes",
        "edges",
        "raw",
        "sequence_rows",
    ] = Field(description="The type of the destination resource.")


class DataModelSource(DestinationSourceType):
    data_model: DataModelInfo = Field(description="Target data model info.")
    instance_space: str | None = Field(default=None, description="The space where the instances will be created.")


class ViewDataSource(DestinationSourceType):
    view: ViewInfo | None = Field(default=None, description="Target view info.")
    edge_type: EdgeType | None = Field(default=None, description="Target type of the connection definition.")
    instance_space: str | None = Field(
        default=None, description="The space where the instances(nodes/edges) will be created."
    )


class RawDataSource(DestinationSourceType):
    database: str = Field(description="The database name.")
    table: str = Field(description="The table name.")


class SequenceRowDataSource(DestinationSourceType):
    external_id: str = Field(description="The externalId of sequence.")


class AuthenticationClientIdSecret(ToolkitResource):
    client_id: str = Field(description="Client Id.")
    client_secret: str = Field(description="Client Secret.")
    scopes: str | list[str] | None = Field(default=None, description="Scopes for the authentication.")
    token_uri: str = Field(description="OAuth token url.")
    cdf_project_name: str = Field(description="CDF project name.")
    audience: str | None = Field(default=None, description="Audience for the authentication.")


class OIDCCredential(ToolkitResource):
    read: AuthenticationClientIdSecret | None = Field(
        default=None, description="Source/Read authentication information."
    )
    write: AuthenticationClientIdSecret | None = Field(
        default=None, description="Destination/Write authentication information."
    )


class TransformationYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.")
    name: str = Field(description="Name of the transformation.")
    ignore_null_fields: bool = Field(
        description="Indicates how null values are handled on updates: ignore or set null."
    )
    destination: (
        DestinationSourceType | DataModelSource | ViewDataSource | RawDataSource | SequenceRowDataSource | None
    ) = Field(default=None, description="Destination data type.")
    query: str | None = Field(default=None, description="SQL query of the transformation.")
    conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = Field(
        default=None,
        description="Behavior when the data already exists.",
    )
    is_public: bool | None = Field(
        default=None,
        description="Indicates if the transformation is visible to all in project or only to the owner.",
    )
    authentication: AuthenticationClientIdSecret | OIDCCredential | None = Field(
        default=None,
        description="Authentication information for the transformation.",
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="External ID of the data set to which the transformation belongs.",
    )
    tags: list[str] | None = Field(
        default=None,
        description="List of tags for the Transformation.",
        max_length=5,
    )

    @model_validator(mode="before")
    def validate_destination_type(cls, values: dict) -> dict:
        """
        Validate the destination type and ensure it matches the expected structure.
        Raises:
            ValueError: If the destination type is not recognized or if the structure is invalid.
        """
        destination = values.get("destination")
        if destination is None:
            return values

        dest_type = destination.get("type")

        try:
            if dest_type == "sequence_rows":
                SequenceRowDataSource.model_validate(destination)

            elif dest_type == "raw":
                RawDataSource.model_validate(destination)

            elif dest_type in ["instances"]:
                DataModelSource.model_validate(destination)

            elif dest_type in ["nodes", "edges"]:
                ViewDataSource.model_validate(destination)

        except ValidationError as e:
            raise ValueError(f"Invalid destination data for type '{dest_type}': {e!s}")

        return values
