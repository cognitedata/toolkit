import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.resource_classes import as_message
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

    @pytest.mark.parametrize(
        "data, expected_message",
        [
            (
                {"sourceId": "123-345"},
                ("Failed to load group: 1 errors in validation\n  - Error 1 Field required: 'name'"),
            ),
            (
                {
                    "name": "invalid-group",
                    "capabilities": [{"dataModelInstancesAcl": {"actions": ["INVALID_ACTION"]}}],
                    "members": "allUserAccounts",
                },
                (
                    "Failed to load group: 2 errors in validation\n"
                    "  - Error 1 in location 'capabilities.0.actions.0': Input should be 'READ', "
                    "'WRITE' or 'WRITE_PROPERTIES'\n"
                    "  - Error 2 Field required: 'capabilities.0.scope'"
                ),
            ),
            (
                {
                    "name": "invalid-group",
                    "capabilities": [
                        {"dataModelsAcl": {"actions": ["WRITE"], "scope": {"notExisting": {"spaceIds": {"my_space"}}}}}
                    ],
                    "members": "allUserAccounts",
                },
                (
                    "Failed to load group: 1 errors in validation\n"
                    "  - Error 1 in location 'capabilities.0.scope': Value error, Invalid scope "
                    "name 'notExisting'. Expected one of all or spaceIdScope"
                ),
            ),
        ],
    )
    def test_load_invalid_groups(self, data: dict[str, object], expected_message: str) -> None:
        try:
            Group.model_validate(data)
        except ValidationError as e:
            assert expected_message in as_message(e, "load group")
        else:
            assert False, f"Expected ValidationError with message '{expected_message}'"
