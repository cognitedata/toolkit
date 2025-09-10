import pytest
from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    FilesAcl,
    ProjectCapability,
    ProjectCapabilityList,
    TimeSeriesAcl,
)
from cognite.client.data_classes.iam import TokenInspection

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess


class TestValidateAccess:
    @pytest.mark.parametrize(
        "capabilities, spaces, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read data models. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.SpaceID(["space1"]))],
                {"space2"},
                "You have no permission to read the 'space2' space(s). This is required to test the operation.",
                id="Space mismatch",
            ),
        ],
    )
    def test_model_access_raise(
        self, capabilities: list[Capability], spaces: set[str] | None, expected_error: str
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.data_model(["read"], spaces=spaces)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, spaces, expected_result",
        [
            pytest.param(
                [DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.SpaceID(["space1"]))],
                {"space1"},
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
        self, capabilities: list[Capability], spaces: set[str] | None, expected_result: list[str] | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            result = validator.data_model(["read"], spaces=spaces)
            assert result == expected_result

    @pytest.mark.parametrize(
        "capabilities, spaces, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read instances. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [
                    DataModelInstancesAcl(
                        [DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.SpaceID(["space1"])
                    )
                ],
                {"space2"},
                "You have no permission to read instances in the 'space2' space(s). This is required to test the operation instances.",
                id="Space mismatch",
            ),
        ],
    )
    def test_instances_access_raise(
        self, capabilities: list[Capability], spaces: set[str] | None, expected_error: str
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.instances(["read"], spaces=spaces)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, spaces, expected_result",
        [
            pytest.param(
                [
                    DataModelInstancesAcl(
                        [DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.SpaceID(["space1"])
                    )
                ],
                {"space1"},
                None,
                id="Space match",
            ),
            pytest.param(
                [DataModelInstancesAcl([DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.All())],
                None,
                None,
                id="All scope",
            ),
            pytest.param(
                [
                    DataModelInstancesAcl(
                        [DataModelInstancesAcl.Action.Read], DataModelInstancesAcl.Scope.SpaceID(["space1", "space2"])
                    )
                ],
                None,
                ["space1", "space2"],
                id="Limited list of spaces",
            ),
        ],
    )
    def test_instances_access(
        self, capabilities: list[Capability], spaces: set[str] | None, expected_result: list[str] | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            validator = ValidateAccess(client, "test the operation")
            result = validator.instances(["read"], spaces=spaces)
            assert result == expected_result

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read time series. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1]))],
                {2},
                "You have no permission to read time series in dataset 2. This is required to test the operation.",
                id="Dataset mismatch",
            ),
            pytest.param(
                [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.AssetRootID([10]))],
                {1},
                "You have no permission to read time series in dataset 1. This is required to test the operation.",
                id="Access by other scope type",
            ),
        ],
    )
    def test_timeseries_access_raise(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_error: str
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            client.lookup.assets.external_id.side_effect = external_id_lookup
            client.lookup.time_series.external_id.side_effect = external_id_lookup

            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.timeseries(["read"], dataset_ids=dataset_ids)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_result",
        [
            pytest.param(
                [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1, 2]))],
                {1},
                None,
                id="Dataset match",
            ),
            pytest.param(
                [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.All())],
                None,
                None,
                id="All scope",
            ),
            pytest.param(
                [TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([1, 2]))],
                None,
                {"dataset": ["1", "2"]},
                id="Limited list of datasets",
            ),
            pytest.param(
                [
                    TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.AssetRootID([10, 20])),
                    TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.DataSet([15, 25])),
                    TimeSeriesAcl([TimeSeriesAcl.Action.Read], TimeSeriesAcl.Scope.ID([100, 200])),
                ],
                None,
                {
                    "asset root": ["10", "20"],
                    "dataset": ["15", "25"],
                    "time series": ["100", "200"],
                },
                id="Multiple scopes with IDs",
            ),
        ],
    )
    def test_timeseries_access(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_result: dict | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            client.lookup.assets.external_id.side_effect = external_id_lookup
            client.lookup.time_series.external_id.side_effect = external_id_lookup

            validator = ValidateAccess(client, "test the operation")
            result = validator.timeseries(["read"], dataset_ids=dataset_ids)
            assert result == expected_result

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read files. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [FilesAcl([FilesAcl.Action.Read], FilesAcl.Scope.DataSet([1]))],
                {2},
                "You have no permission to read files in dataset 2. This is required to test the operation.",
                id="Dataset mismatch",
            ),
        ],
    )
    def test_files_access_raise(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_error: str
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.files(["read"], dataset_ids=dataset_ids)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_result",
        [
            pytest.param(
                [FilesAcl([FilesAcl.Action.Read], FilesAcl.Scope.DataSet([1, 2]))],
                {1},
                None,
                id="Dataset match",
            ),
            pytest.param(
                [FilesAcl([FilesAcl.Action.Read], FilesAcl.Scope.All())],
                None,
                None,
                id="All scope",
            ),
            pytest.param(
                [FilesAcl([FilesAcl.Action.Read], FilesAcl.Scope.DataSet([1, 2]))],
                None,
                {"dataset": ["1", "2"]},
                id="Limited list of datasets",
            ),
        ],
    )
    def test_files_access(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_result: dict | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            validator = ValidateAccess(client, "test the operation")
            result = validator.files(["read"], dataset_ids=dataset_ids)
            assert result == expected_result

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_error",
        [
            pytest.param(
                [],
                None,
                "You have no permission to read assets. This is required to test the operation.",
                id="No capabilities",
            ),
            pytest.param(
                [AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.DataSet([1]))],
                {1, 2},
                "You have no permission to read assets in dataset(s) 2. This is required to test the operation.",
                id="Missing dataset",
            ),
        ],
    )
    def test_assets_access_raise(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_error: str
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            validator = ValidateAccess(client, "test the operation")
            with pytest.raises(AuthorizationError) as exc:
                validator.assets(["read"], dataset_ids=dataset_ids)
            assert str(exc.value) == expected_error

    @pytest.mark.parametrize(
        "capabilities, dataset_ids, expected_result",
        [
            pytest.param(
                [AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.DataSet([1, 2]))],
                {1},
                None,
                id="Dataset match",
            ),
            pytest.param(
                [AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.All())],
                None,
                None,
                id="All scope",
            ),
            pytest.param(
                [AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.DataSet([1, 2]))],
                None,
                {"dataset": ["1", "2"]},
                id="Limited list of datasets",
            ),
        ],
    )
    def test_assets_access(
        self, capabilities: list[Capability], dataset_ids: set[int] | None, expected_result: dict | None
    ) -> None:
        inspection = self._create_inspection_obj(capabilities)

        def external_id_lookup(ids: list[int]) -> list[str]:
            return [str(id_) for id_ in ids]

        with monkeypatch_toolkit_client() as client:
            client.iam.token.inspect.return_value = inspection
            client.lookup.data_sets.external_id.side_effect = external_id_lookup
            validator = ValidateAccess(client, "test the operation")
            result = validator.assets(["read"], dataset_ids=dataset_ids)
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
