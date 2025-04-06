import pytest

from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import (
    APMConfigWrite,
    FeatureConfiguration,
    ResourceFilters,
    RootLocationConfiguration,
    RootLocationDataFilters,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.loaders import (
    AssetLoader,
    DataSetsLoader,
    GroupResourceScopedLoader,
    InfieldV1Loader,
    SpaceLoader,
)


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
            (loader_cls.__name__, identifier) for loader_cls, identifier in InfieldV1Loader.get_dependent_items(dumped)
        }

        assert actual == {
            (AssetLoader.__name__, "my_root_asset"),
            (DataSetsLoader.__name__, "my_dataset"),
            (SpaceLoader.__name__, "my_app_data_space"),
            (SpaceLoader.__name__, "my_customer_data_space"),
            (SpaceLoader.__name__, "my_source_data_space"),
            (GroupResourceScopedLoader.__name__, "my_admin_group1"),
            (GroupResourceScopedLoader.__name__, "my_admin_group2"),
            (GroupResourceScopedLoader.__name__, "my_admin_group3"),
            (DataSetsLoader.__name__, "my_other_dataset"),
            (AssetLoader.__name__, "my_asset_subtree"),
            (SpaceLoader.__name__, "my_source_data_space"),
        }
