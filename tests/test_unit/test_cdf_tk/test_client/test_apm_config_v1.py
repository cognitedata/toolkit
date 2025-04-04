from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import APMConfig, APMConfigWrite
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestAPMConfigV1Class:
    def test_apm_config_data_class_dump_load(self):
        config = FakeCogniteResourceGenerator(seed=1337).create_instance(APMConfig)

        dumped = config.dump()
        assert config == APMConfig._load(dumped)

    def test_apm_config_write_data_class_dump_load(self):
        config = FakeCogniteResourceGenerator(seed=1333).create_instance(APMConfigWrite)

        dumped = config.dump()
        assert config == APMConfigWrite._load(dumped)

    def test_load_unknown_field_in_feature_configuration(self):
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

    def test_load_unknown_nested_field_in_feature_configuration(self):
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
