import pytest
from cognite.client.data_classes.data_modeling import Node

from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import APMConfig, APMConfigCore, APMConfigWrite
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestAPMConfigV1Class:
    def test_apm_config_data_class_dump_load(self) -> None:
        config = FakeCogniteResourceGenerator(seed=1337).create_instance(APMConfig)

        dumped = config.dump()
        assert config == APMConfig._load(dumped)

    def test_apm_config_write_data_class_dump_load(self) -> None:
        config = FakeCogniteResourceGenerator(seed=1333).create_instance(APMConfigWrite)

        dumped = config.dump()
        assert config == APMConfigWrite._load(dumped)

    def test_load_unknown_field_in_feature_configuration(self) -> None:
        config = FakeCogniteResourceGenerator(seed=1337).create_instance(APMConfig)
        dumped = config.dump()
        unknown_object = {
            "with_unknown_field": True,
            "unknown_field": "unknown_value",
        }
        dumped["featureConfiguration"]["unknown_field"] = unknown_object

        loaded = APMConfig._load(dumped)
        redumped = loaded.dump()
        assert redumped["featureConfiguration"]["unknown_field"] == unknown_object

    def test_load_unknown_nested_field_in_feature_configuration(self) -> None:
        config = FakeCogniteResourceGenerator(seed=1337).create_instance(APMConfig)
        dumped = config.dump()
        unknown_object = {
            "with_unknown_field": True,
            "unknown_field": "unknown_value",
        }
        assert isinstance(dumped["featureConfiguration"]["rootLocationConfigurations"], list)
        dumped["featureConfiguration"]["rootLocationConfigurations"].append(unknown_object)

        loaded = APMConfig._load(dumped)
        redumped = loaded.dump()
        assert redumped["featureConfiguration"]["rootLocationConfigurations"][-1] == unknown_object

    def test_from_node_as_write_to_node(self) -> None:
        node = Node._load(
            {
                "space": "APM_Config",
                "externalId": "my_config",
                "version": 1,
                "lastUpdatedTime": 1,
                "createdTime": 1,
                "properties": {
                    "APM_Config": {
                        "APM_Config/1": {
                            "customerDataSpaceId": "APM_SourceData",
                            "customerDataSpaceVersion": "1",
                            "name": "Infield APM App Config",
                            "featureConfiguration": {
                                "rootLocationConfigurations": [
                                    {
                                        "assetExternalId": "MyAsset",
                                        "appDataInstanceSpace": "my_instance_space",
                                        "sourceDataInstanceSpace": "my_source_space",
                                        "templateAdmins": ["admin1", "admin2"],
                                        "checklistAdmins": ["admin3"],
                                    },
                                ],
                            },
                        }
                    }
                },
            }
        )

        read_config = APMConfig.from_node(node)

        write_config = read_config.as_write()

        node_apply = write_config.as_node()

        assert node_apply.dump() == node.as_write().dump()

    @pytest.mark.parametrize("config_cls", [APMConfig, APMConfigWrite])
    def test_apm_config_write_dump_load_yaml(self, config_cls: type[APMConfigCore]) -> None:
        instance = FakeCogniteResourceGenerator(seed=1338).create_instance(config_cls)

        yaml_str = instance.dump_yaml()

        loaded_instance = config_cls.load(yaml_str)

        assert loaded_instance.dump() == instance.dump()
