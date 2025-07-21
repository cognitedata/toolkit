import pytest
from cognite.client.data_classes.capabilities import Capability, DataModelsAcl, ProjectCapability, ProjectCapabilityList
from cognite.client.data_classes.iam import TokenInspection

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess


class TestValidateAccess:
    @pytest.mark.parametrize(
        "capabilities, space, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read data models. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.SpaceID(["space1"]))],
                "space2",
                "You have no permission to read the 'space2' space. This is required to test the operation.",
                id="Space mismatch",
            ),
        ],
    )
    def test_model_access_raise(self, capabilities: list[Capability], space: str | None, expected_error: str) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.data_model(["read"], space=space)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, space, expected_result",
        [
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.SpaceID(["space1"]))],
                "space1",
                None,
                id="Space match",
            ),
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.All())], None, None, id="All scope"
            ),
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.SpaceID(["space1", "space2"]))],
                None,
                ["space1", "space2"],
                id="Limited list of spaces",
            ),
        ],
    )
    def test_model_access(
        self, capabilities: list[Capability], space: str | None, expected_result: list[str] | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            result = validator.data_model(["read"], space=space)
            assert result == expected_result

    @staticmethod
    def _create_inspection_obj(capabilities: list[Capability]) -> TokenInspection:
        inspection = TokenInspection(
            "123",
            [],
            ProjectCapabilityList(
                [
                    ProjectCapability(
                        capability=capability,
                        project_scope=ProjectCapability.Scope.All(),
                    )
                    for capability in capabilities
                ]
            ),
        )
        return inspection
