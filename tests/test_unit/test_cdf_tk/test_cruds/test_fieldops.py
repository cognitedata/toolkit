import pytest

from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import (
    APMConfigWrite,
    FeatureConfiguration,
    ResourceFilters,
    RootLocationConfiguration,
    RootLocationDataFilters,
)
from cognite_toolkit._cdf_tk.cruds import (
    AssetCRUD,
    DataSetsCRUD,
    GroupResourceScopedCRUD,
    InfieldV1CRUD,
    SpaceCRUD,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags


class TestInfieldV1Loader:
    @pytest.mark.skipif(not Flags.INFIELD.is_enabled(), reason="Alpha feature is not enabled")
    def test_dependent_items(self) -> None:
        item = APMConfigWrite(
            external_id="my_config",
            app_data_space_id="my_app_data_space",
            customer_data_space_id="my_customer_data_space",
            feature_configuration=FeatureConfiguration(
                root_location_configurations=[
                    RootLocationConfiguration(
                        asset_external_id="my_root_asset",
                        template_admins=["my_admin_group1", "my_admin_group2"],
                        checklist_admins=["my_admin_group3"],
                        source_data_instance_space="my_source_data_space",
                        data_filters=RootLocationDataFilters(
                            assets=ResourceFilters(
                                asset_subtree_external_ids=["my_asset_subtree"],
                            ),
                        ),
                    )
                ]
            ),
        )
        dumped = item.dump()
        dumped["featureConfiguration"]["rootLocationConfigurations"][0]["dataSetExternalId"] = "my_dataset"
        dumped["featureConfiguration"]["rootLocationConfigurations"][0]["dataFilters"]["assets"][
            "dataSetExternalIds"
        ] = ["my_other_dataset"]

        actual = {
            (loader_cls.__name__, identifier) for loader_cls, identifier in InfieldV1CRUD.get_dependent_items(dumped)
        }

        assert actual == {
            (AssetCRUD.__name__, "my_root_asset"),
            (DataSetsCRUD.__name__, "my_dataset"),
            (SpaceCRUD.__name__, "my_app_data_space"),
            (SpaceCRUD.__name__, "my_customer_data_space"),
            (SpaceCRUD.__name__, "my_source_data_space"),
            (GroupResourceScopedCRUD.__name__, "my_admin_group1"),
            (GroupResourceScopedCRUD.__name__, "my_admin_group2"),
            (GroupResourceScopedCRUD.__name__, "my_admin_group3"),
            (DataSetsCRUD.__name__, "my_other_dataset"),
            (AssetCRUD.__name__, "my_asset_subtree"),
            (SpaceCRUD.__name__, "my_source_data_space"),
        }
