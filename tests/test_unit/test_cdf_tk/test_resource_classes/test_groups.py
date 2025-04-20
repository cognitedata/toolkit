import pytest

from cognite_toolkit._cdf_tk.resource_classes.groups import Group


class TestGroup:
    @pytest.mark.parametrize(
        "data",
        [
            {"name": "no-capabilities", "sourceId": "123-345"},
            {
                "name": "data-model-capabilities",
                "capabilities": [
                    {"dataModelInstancesAcl": {"actions": ["READ", "WRITE", "WRITE_PROPERTIES"], "scope": {"all": {}}}},
                    {
                        "dataModelsAcl": {
                            "actions": ["READ", "WRITE"],
                            "scope": {"spaceIdScope": {"spaceIds": ["space1"]}},
                        }
                    },
                ],
                "members": "allUserAccounts",
            },
        ],
    )
    def test_load_valid_groups(self, data: dict[str, object]) -> None:
        loaded = Group.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
