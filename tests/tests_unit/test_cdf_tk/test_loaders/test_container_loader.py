import pytest

from cognite_toolkit._cdf_tk._parameters import read_parameters_from_dict
from cognite_toolkit._cdf_tk.loaders import ContainerLoader


class TestContainerLoader:
    @pytest.mark.parametrize(
        "item",
        [
            pytest.param(
                {
                    "properties": {
                        "myDirectRelation": {
                            "name": "my direct relation",
                            "type": {
                                "type": "direct",
                                "container": {
                                    "type": "container",
                                    "space": "sp_my_space",
                                    "externalId": "my_container",
                                },
                            },
                        }
                    }
                },
                id="Direct relation property with require constraint.",
            ),
        ],
    )
    def test_valid_spec(self, item: dict):
        spec = ContainerLoader.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"
