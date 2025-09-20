from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.containers import BTreeIndex

SPACE = dm.SpaceApply(
    "cognite_migration", description="Space for the asset-centric to data modeling migration", name="cdf_migration"
)

RESOURCE_VIEW_MAPPING = dm.ContainerApply(
    space=SPACE.space,
    external_id="ResourceViewMapping",
    used_for="node",
    properties={
        "resourceType": dm.ContainerProperty(
            type=dm.Text(max_text_size=255),
            nullable=False,
        ),
        "viewId": dm.ContainerProperty(
            type=dm.data_types.Json(),
            nullable=False,
        ),
        "propertyMapping": dm.ContainerProperty(
            type=dm.data_types.Json(),
            nullable=False,
        ),
    },
)

INSTANCE_SOURCE_CONTAINER = dm.ContainerApply(
    space=SPACE.space,
    external_id="InstanceSource",
    used_for="node",
    properties={
        "resourceType": dm.ContainerProperty(
            type=dm.data_types.Enum(
                values={
                    "timeseries": dm.data_types.EnumValue(),
                    "asset": dm.data_types.EnumValue(),
                    "file": dm.data_types.EnumValue(),
                    "event": dm.data_types.EnumValue(),
                    "sequence": dm.data_types.EnumValue(),
                }
            ),
            nullable=False,
        ),
        "id": dm.ContainerProperty(
            type=dm.data_types.Int64(),
            nullable=False,
        ),
        "dataSetId": dm.ContainerProperty(
            type=dm.data_types.Int64(),
            nullable=True,
        ),
        "classicExternalId": dm.ContainerProperty(
            type=dm.data_types.Text(),
            nullable=True,
        ),
        "preferredConsumerViewId": dm.ContainerProperty(
            type=dm.data_types.Json(),
            nullable=True,
        ),
        "resourceViewMapping": dm.ContainerProperty(
            type=dm.data_types.DirectRelation(container=RESOURCE_VIEW_MAPPING.as_id()), nullable=True
        ),
    },
    indexes={
        "id": BTreeIndex(["id"], cursorable=True),
        "resourceType": BTreeIndex(["resourceType", "id"], cursorable=False),
    },
)
CONTAINERS = [RESOURCE_VIEW_MAPPING, INSTANCE_SOURCE_CONTAINER]

RESOURCE_VIEW_MAPPING_VIEW = dm.ViewApply(
    space=SPACE.space,
    external_id="ResourceViewMapping",
    version="v1",
    name="ResourceViewMapping",
    description="The mapping from asset-centric resources to data modeling view.",
    properties={
        "resourceType": dm.MappedPropertyApply(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="resourceType",
        ),
        "viewId": dm.MappedPropertyApply(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="viewId",
        ),
        "propertyMapping": dm.MappedPropertyApply(
            container=RESOURCE_VIEW_MAPPING.as_id(),
            container_property_identifier="propertyMapping",
        ),
    },
)

INSTANCE_SOURCE_VIEW = dm.ViewApply(
    space=SPACE.space,
    external_id="InstanceSource",
    version="v1",
    name="InstanceSource",
    description="The source of the instance in asset-centric resources.",
    properties={
        "resourceType": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="resourceType",
        ),
        "id": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="id",
        ),
        "dataSetId": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="dataSetId",
        ),
        "classicExternalId": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="classicExternalId",
        ),
        "preferredConsumerViewId": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="preferredConsumerViewId",
        ),
        "resourceViewMapping": dm.MappedPropertyApply(
            container=INSTANCE_SOURCE_CONTAINER.as_id(),
            container_property_identifier="resourceViewMapping",
            source=RESOURCE_VIEW_MAPPING_VIEW.as_id(),
        ),
    },
)

INSTANCE_SOURCE_VIEW_ID = INSTANCE_SOURCE_VIEW.as_id()
RESOURCE_VIEW_MAPPING_VIEW_ID = RESOURCE_VIEW_MAPPING_VIEW.as_id()

VIEWS = [RESOURCE_VIEW_MAPPING_VIEW, INSTANCE_SOURCE_VIEW]

COGNITE_MIGRATION_MODEL = dm.DataModelApply(
    space=SPACE.space,
    external_id="CogniteMigration",
    version="v1",
    name="CDF Migration Model",
    description="Data model for migrating asset-centric resources to data modeling resources in CDF.",
    views=[RESOURCE_VIEW_MAPPING_VIEW.as_id(), INSTANCE_SOURCE_VIEW.as_id()],
)

MODEL_ID = COGNITE_MIGRATION_MODEL.as_id()
