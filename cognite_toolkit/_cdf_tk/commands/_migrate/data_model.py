from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    BtreeIndex,
    ContainerPropertyDefinition,
    ContainerRequest,
    DataModelRequest,
    DirectNodeRelation,
    EnumProperty,
    EnumValue,
    Int64Property,
    JSONProperty,
    SpaceRequest,
    TextProperty,
    UniquenessConstraintDefinition,
    ViewCorePropertyRequest,
    ViewRequest,
)

SPACE = SpaceRequest(
    space="cognite_migration",
    description="Space for the asset-centric to data modeling migration",
    name="cdf_migration",
)
COGNITE_MIGRATION_SPACE_ID = SPACE.space

RESOURCE_VIEW_MAPPING = ContainerRequest(
    space=SPACE.space,
    external_id="ResourceViewMapping",
    used_for="node",
    properties={
        "resourceType": ContainerPropertyDefinition(
            type=TextProperty(max_text_size=255),
            nullable=False,
        ),
        "viewId": ContainerPropertyDefinition(
            type=JSONProperty(),
            nullable=False,
        ),
        "propertyMapping": ContainerPropertyDefinition(
            type=JSONProperty(),
            nullable=False,
        ),
    },
)

INSTANCE_SOURCE_CONTAINER = ContainerRequest(
    space=SPACE.space,
    external_id="InstanceSource",
    used_for="node",
    properties={
        "resourceType": ContainerPropertyDefinition(
            type=EnumProperty(
                values={
                    "timeseries": EnumValue(),
                    "asset": EnumValue(),
                    "file": EnumValue(),
                    "event": EnumValue(),
                    "sequence": EnumValue(),
                }
            ),
            nullable=False,
        ),
        "id": ContainerPropertyDefinition(
            type=Int64Property(),
            nullable=False,
        ),
        "dataSetId": ContainerPropertyDefinition(
            type=Int64Property(),
            nullable=True,
        ),
        "classicExternalId": ContainerPropertyDefinition(
            type=TextProperty(),
            nullable=True,
        ),
        "preferredConsumerViewId": ContainerPropertyDefinition(
            type=JSONProperty(),
            nullable=True,
        ),
        "resourceViewMapping": ContainerPropertyDefinition(
            type=DirectNodeRelation(container=RESOURCE_VIEW_MAPPING.as_id()), nullable=True
        ),
    },
    indexes={
        "id": BtreeIndex(properties=["id"], cursorable=True),
        "resourceType": BtreeIndex(properties=["resourceType", "id"], cursorable=False),
    },
)

CREATED_SOURCE_SYSTEM = ContainerRequest(
    space=SPACE.space,
    external_id="CreatedSourceSystem",
    used_for="node",
    properties={
        "source": ContainerPropertyDefinition(
            type=TextProperty(max_text_size=128),
            nullable=False,
        )
    },
    constraints={"sourceUnique": UniquenessConstraintDefinition(properties=["source"])},
    indexes={
        "source": BtreeIndex(properties=["source"], cursorable=True),
    },
)

SPACE_SOURCE = ContainerRequest(
    space=SPACE.space,
    external_id="SpaceSource",
    used_for="node",
    properties={
        "instanceSpace": ContainerPropertyDefinition(
            type=TextProperty(max_text_size=64),
            nullable=False,
        ),
        "dataSetId": ContainerPropertyDefinition(
            type=Int64Property(),
            nullable=False,
        ),
        "dataSetExternalId": ContainerPropertyDefinition(
            type=TextProperty(max_text_size=256),
            nullable=True,
        ),
    },
    indexes={
        "space": BtreeIndex(properties=["instanceSpace"], cursorable=True),
        "dataSetId": BtreeIndex(properties=["dataSetId"], cursorable=True),
        "dataSetExternalId": BtreeIndex(properties=["dataSetExternalId"], cursorable=True),
    },
    constraints={
        "dataSetIdUnique": UniquenessConstraintDefinition(properties=["dataSetId"]),
    },
)

CONTAINERS = [RESOURCE_VIEW_MAPPING, INSTANCE_SOURCE_CONTAINER, CREATED_SOURCE_SYSTEM, SPACE_SOURCE]

RESOURCE_VIEW_MAPPING_VIEW = ViewRequest(
    space=SPACE.space,
    external_id="ResourceViewMapping",
    version="v1",
    name="ResourceViewMapping",
    description="The mapping from asset-centric resources to data modeling view.",
    properties={
        "resourceType": ViewCorePropertyRequest(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="resourceType",
        ),
        "viewId": ViewCorePropertyRequest(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="viewId",
        ),
        "propertyMapping": ViewCorePropertyRequest(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="propertyMapping",
        ),
    },
)

INSTANCE_SOURCE_VIEW = ViewRequest(
    space=SPACE.space,
    external_id="InstanceSource",
    version="v1",
    name="InstanceSource",
    description="The source of the instance in asset-centric resources.",
    properties={
        "resourceType": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="resourceType",
        ),
        "id": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="id",
        ),
        "dataSetId": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="dataSetId",
        ),
        "classicExternalId": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="classicExternalId",
        ),
        "preferredConsumerViewId": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="preferredConsumerViewId",
        ),
        "resourceViewMapping": ViewCorePropertyRequest(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="resourceViewMapping",
            source=RESOURCE_VIEW_MAPPING_VIEW.as_id(),
        ),
    },
)

CREATED_SOURCE_SYSTEM_VIEW = ViewRequest(
    space=SPACE.space,
    external_id="CreatedSourceSystem",
    version="v1",
    name="CreatedSourceSystem",
    description="The source string the SourceSystem was created from.",
    properties={
        "source": ViewCorePropertyRequest(
            container=CREATED_SOURCE_SYSTEM.as_id(),
            container_property_identifier="source",
        ),
    },
)

SPACE_SOURCE_VIEW = ViewRequest(
    space=SPACE.space,
    external_id="SpaceSource",
    version="v1",
    name="SpaceSource",
    description="The mapping from CDF spaces to data sets.",
    properties={
        "instanceSpace": ViewCorePropertyRequest(
            container=SPACE_SOURCE.as_id(),
            container_property_identifier="instanceSpace",
            description="The identifier of the created instances space.",
        ),
        "dataSetId": ViewCorePropertyRequest(
            container=SPACE_SOURCE.as_id(),
            container_property_identifier="dataSetId",
            description="The dataSetId the space was created from.",
        ),
        "dataSetExternalId": ViewCorePropertyRequest(
            container=SPACE_SOURCE.as_id(),
            container_property_identifier="dataSetExternalId",
            description="The externalId of the dataSet (if present) the space was created from.",
        ),
    },
)

INSTANCE_SOURCE_VIEW_ID = INSTANCE_SOURCE_VIEW.as_id()
RESOURCE_VIEW_MAPPING_VIEW_ID = RESOURCE_VIEW_MAPPING_VIEW.as_id()
CREATED_SOURCE_SYSTEM_VIEW_ID = CREATED_SOURCE_SYSTEM_VIEW.as_id()
SPACE_SOURCE_VIEW_ID = SPACE_SOURCE_VIEW.as_id()

VIEWS = [RESOURCE_VIEW_MAPPING_VIEW, INSTANCE_SOURCE_VIEW, CREATED_SOURCE_SYSTEM_VIEW, SPACE_SOURCE_VIEW]

COGNITE_MIGRATION_MODEL = DataModelRequest(
    space=SPACE.space,
    external_id="CogniteMigration",
    version="v1",
    name="CDF Migration Model",
    description="Data model for migrating asset-centric resources to data modeling resources in CDF.",
    views=[INSTANCE_SOURCE_VIEW_ID, RESOURCE_VIEW_MAPPING_VIEW_ID, CREATED_SOURCE_SYSTEM_VIEW_ID, SPACE_SOURCE_VIEW_ID],
)

MODEL_ID = COGNITE_MIGRATION_MODEL.as_id()
