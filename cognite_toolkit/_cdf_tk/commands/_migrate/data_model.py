from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.containers import BTreeIndex

SPACE = dm.SpaceApply(
    "cognite_migration", description="Space for the asset-centric to data modeling migration", name="cdf_migration"
)
MAPPING_CONTAINER = dm.ContainerApply(
    space=SPACE.space,
    external_id="Mapping",
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
    },
    indexes={
        "id": BTreeIndex(["id"], cursorable=True),
        "resourceType": BTreeIndex(["resourceType", "id"], cursorable=False),
    },
)

MAPPING_VIEW = dm.ViewApply(
    space=SPACE.space,
    external_id="Mapping",
    version="v1",
    name="Mapping",
    description="The mapping between asset-centric and data modeling resources",
    properties={
        "resourceType": dm.MappedPropertyApply(
            container=MAPPING_CONTAINER.as_id(),
            container_property_identifier="resourceType",
        ),
        "id": dm.MappedPropertyApply(
            container=MAPPING_CONTAINER.as_id(),
            container_property_identifier="id",
        ),
        "dataSetId": dm.MappedPropertyApply(
            container=MAPPING_CONTAINER.as_id(),
            container_property_identifier="dataSetId",
        ),
        "classicExternalId": dm.MappedPropertyApply(
            container=MAPPING_CONTAINER.as_id(),
            container_property_identifier="classicExternalId",
        ),
    },
)

MAPPING_VIEW_ID = MAPPING_VIEW.as_id()

COGNITE_MIGRATION_MODEL = dm.DataModelApply(
    space=SPACE.space,
    external_id="CogniteMigration",
    version="v1",
    name="CDF Migration Model",
    description="Data model for migrating asset-centric resources to data modeling resources in CDF.",
    views=[MAPPING_VIEW.as_id()],
)

MODEL_ID = COGNITE_MIGRATION_MODEL.as_id()
