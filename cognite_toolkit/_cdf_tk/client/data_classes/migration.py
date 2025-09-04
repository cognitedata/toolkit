import json
import sys
import warnings
from dataclasses import dataclass
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedNode,
    TypedNodeApply,
)

from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE
from cognite_toolkit._cdf_tk.tk_warnings import IgnoredValueWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentric

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass(frozen=True)
class AssetCentricId(CogniteObject):
    resource_type: AssetCentric
    id_: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load an AssetCentricId from a dictionary."""
        return cls(
            resource_type=resource["resourceType"],
            id_=resource["id"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the AssetCentricId to a dictionary."""
        return {
            "resourceType" if camel_case else "resource_type": self.resource_type,
            "id" if camel_case else "id_": self.id_,
        }

    def __str__(self) -> str:
        """Return a string representation of the AssetCentricId."""
        return f"{self.resource_type}(id={self.id_})"


class _InstanceSourceProperties:
    resource_type = PropertyOptions("resourceType")
    id_ = PropertyOptions("id")
    data_set_id = PropertyOptions("dataSetId")
    classic_external_id = PropertyOptions("classicExternalId")
    preferred_consumer_view_id = PropertyOptions("preferredConsumerViewId")
    ingestion_view = PropertyOptions("ingestionView")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cognite_migration", "InstanceSource", "v1")


class InstanceSource(_InstanceSourceProperties, TypedNode):
    """This represents the reading format of instance source.

    It is used to when data is read from CDF.

    The source of the instance in asset-centric resources.

    Args:
        space: The space where the node is located.
        external_id: The external id of the instance source.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        resource_type: The resource type field.
        id_: The id field.
        data_set_id: The data set id field.
        classic_external_id: The classic external id field.
        preferred_consumer_view_id: The preferred consumer view id field.
        ingestion_view: The ingestion view field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        resource_type: Literal["asset", "event", "file", "sequence", "timeseries"],
        id_: int,
        data_set_id: int | None = None,
        classic_external_id: str | None = None,
        preferred_consumer_view_id: ViewId | None = None,
        ingestion_view: DirectRelationReference | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.resource_type = resource_type
        self.id_ = id_
        self.data_set_id = data_set_id
        self.classic_external_id = classic_external_id
        self.preferred_consumer_view_id = preferred_consumer_view_id
        self.ingestion_view = DirectRelationReference.load(ingestion_view) if ingestion_view else None

    @classmethod
    def _load_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        if "preferredConsumerViewId" in resource:
            preferred_consumer_view_id = resource.pop("preferredConsumerViewId")
            try:
                resource["preferredConsumerViewId"] = ViewId.load(preferred_consumer_view_id)
            except (TypeError, KeyError) as e:
                warnings.warn(
                    IgnoredValueWarning(
                        name="InstanceSource.preferredConsumerViewId",
                        value=json.dumps(preferred_consumer_view_id),
                        reason=f"Invalid ViewId format expected 'space', 'externalId', 'version': {e!s}",
                    )
                )
        return super()._load_properties(resource)

    def as_asset_centric_id(self) -> AssetCentricId:
        """Return the AssetCentricId representation of the mapping."""
        return AssetCentricId(
            resource_type=self.resource_type,
            id_=self.id_,
        )

    def consumer_view(self) -> ViewId:
        if self.preferred_consumer_view_id:
            return self.preferred_consumer_view_id
        if self.resource_type == "sequence":
            raise ValueError(f"Missing consumer view for sequence {self.external_id}.")
        # Default consumer view for asset-centric resources
        external_id = {
            "asset": "CogniteAsset",
            "event": "CogniteActivity",
            "file": "CogniteFile",
            "timeseries": "CogniteTimeSeries",
        }[self.resource_type]
        return ViewId("cdf_cdm", external_id, "v1")


@dataclass
class AssetCentricToViewMapping(CogniteObject):
    to_property_id: dict[str, str]
    metadata_to_property_id: dict[str, str] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load an AssetCentricToViewMapping from a dictionary."""
        to_property_id = resource["toPropertyId"]
        metadata_to_property_id = resource.get("metadataToPropertyId")

        if not isinstance(to_property_id, dict):
            raise TypeError(f"toPropertyId must be a dictionary, not {type(to_property_id)}")

        if invalid_keys := [
            key for key, value in to_property_id.items() if not (isinstance(key, str) and isinstance(value, str))
        ]:
            raise TypeError(
                f"toPropertyId keys and values must be strings, found invalid keys: {humanize_collection(invalid_keys)}"
            )

        if metadata_to_property_id is not None and not isinstance(metadata_to_property_id, dict):
            raise TypeError(f"metadataToPropertyId must be a dictionary or None, not {type(metadata_to_property_id)}")

        if metadata_to_property_id and (
            invalid_metadata_keys := [
                key
                for key, value in metadata_to_property_id.items()
                if not (isinstance(key, str) and isinstance(value, str))
            ]
        ):
            raise TypeError(
                f"metadataToPropertyId keys and values must be strings, found invalid keys: {humanize_collection(invalid_metadata_keys)}"
            )

        return cls(
            to_property_id=to_property_id,
            metadata_to_property_id=metadata_to_property_id,
        )


class _ViewSourceProperties:
    resource_type = PropertyOptions("resourceType")
    view_id = PropertyOptions("viewId")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cognite_migration", "ViewSource", "v1")


class ViewSourceApply(_ViewSourceProperties, TypedNodeApply):
    """This represents the writing format of view source.

    It is used to when data is written to CDF.

    The source of the view in asset-centric resources.

    Args:
        external_id: The external id of the view source.
        resource_type: The resource type field.
        view_id: The view id field.
        mapping: The mapping field.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        resource_type: Literal["asset", "event", "file", "sequence", "timeseries"],
        view_id: ViewId,
        mapping: AssetCentricToViewMapping,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, COGNITE_MIGRATION_SPACE, external_id, existing_version, type)
        self.resource_type = resource_type
        self.view_id = view_id
        self.mapping = mapping

    def dump(self, camel_case: bool = True, context: Literal["api", "local"] = "api") -> dict[str, Any]:
        """Dumps the object to a dictionary.

        Args:
            camel_case: Whether to use camel case or not.
            context: If 'api', the output is for the API and will match the Node API schema. If 'local', the output is
                for a YAML file and all properties are  on the same level as the node properties. See below

        Example:
            >>> node = ViewSourceApply(
            ...    external_id="myMapping",
            ...    resource_type="asset",
            ...    view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            ...    mapping=AssetCentricToViewMapping(to_property_id={"name": "name"}),
            ... )
            >>> node.dump(camel_case=True, context="api")
            {
                "space": "cognite_migration",
                "externalId": "myMapping",
                "sources": [
                    {
                        "source": {
                            "space": "cognite_migration",
                            "externalId": "ViewSource",
                            "version": "v1",
                            "type": "view"
                            },

                        "properties": {
                            "resourceType": "asset",
                            "viewId": {
                                "space": "cdf_cdm",
                                "externalId": "CogniteAsset",
                                "version": "v1"
                            },
                            "mapping": {
                                "toPropertyId": {
                                    "name": "name"
                                }
                            }
                        }
                    }
                ]
            }
            >>> node.dump(camel_case=True, context="local")
            {
                "externalId": "myMapping",
                "resourceType": "asset",
                "viewId": {
                    "space": "cdf_cdm",
                    "externalId": "CogniteAsset",
                    "version": "v1"
                },
                "mapping": {
                    "toPropertyId": {
                        "name": "name"
                    }
                },
            }

        Returns:
            dict[str, Any]: The dumped dictionary representation of the object.
        """
        output = super().dump(camel_case)
        source = output["sources"][0]
        properties = source["properties"]
        properties["viewId"] = self.view_id.dump(camel_case=camel_case, include_type=context == "api")
        properties["mapping"] = self.mapping.dump(camel_case=camel_case)

        if context == "local":
            for key in ("space", "sources", "instanceType"):
                output.pop(key, None)
            output.update(properties)
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        base_props = cls._load_base_properties(resource)
        properties = cls._load_properties(resource)
        if "viewId" in resource:
            properties["view_id"] = ViewId.load(resource["viewId"])
        if "mapping" in resource:
            properties["mapping"] = AssetCentricToViewMapping._load(resource["mapping"], cognite_client=cognite_client)

        return cls(**base_props, **properties)


class ViewSource(_ViewSourceProperties, TypedNode):
    """This represents the reading format of view source.

    It is used to when data is read from CDF.

    The source of the view in asset-centric resources.

    Args:
        external_id: The external id of the view source.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        resource_type: The resource type field.
        view_id: The view id field.
        mapping: The mapping field.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        resource_type: Literal["asset", "event", "file", "sequence", "timeseries"],
        view_id: ViewId,
        mapping: AssetCentricToViewMapping,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(
            self, COGNITE_MIGRATION_SPACE, external_id, version, last_updated_time, created_time, deleted_time, type
        )
        self.resource_type = resource_type
        self.view_id = view_id
        self.mapping = mapping

    def as_write(self) -> ViewSourceApply:
        return ViewSourceApply(
            self.external_id,
            resource_type=self.resource_type,
            view_id=self.view_id,
            mapping=self.mapping,
            existing_version=self.version,
            type=self.type,
        )

    @classmethod
    def _load_properties(cls, resource: dict[str, Any]) -> dict[str, Any]:
        if "viewId" in resource:
            view_id = resource.pop("viewId")
            try:
                resource["viewId"] = ViewId.load(view_id)
            except (TypeError, KeyError) as e:
                raise ValueError(f"Invalid viewId format. Expected 'space', 'externalId', 'version'. Error: {e!s}")
        if "mapping" in resource:
            mapping = resource.pop("mapping")
            try:
                resource["mapping"] = AssetCentricToViewMapping._load(mapping)
            except (TypeError, KeyError) as e:
                raise ValueError(
                    f"Invalid mapping format. Expected 'toPropertyId' and optionally 'metadataToPropertyId'. Error: {e!s}"
                )
        return super()._load_properties(resource)

    def _dump_properties(self) -> dict[str, Any]:
        """Dump the properties of the ViewSourceApply."""
        return {
            "resourceType": self.resource_type,
            "viewId": self.view_id.dump(),
            "mapping": self.mapping.dump(),
        }
