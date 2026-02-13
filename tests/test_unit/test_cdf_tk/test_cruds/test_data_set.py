from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.cruds import DataSetsCRUD, ResourceWorker
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataSetsLoader:
    def test_upsert_data_set(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ):
        loader = DataSetsCRUD.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "data_sets" / "1.my_datasets.yaml", env_vars_with_client.dump()
        )
        assert len(raw_list) == 2

        # Set the properties that are set on the server side and load as DataSetResponse
        first_dict = {**raw_list[0], "id": 42, "createdTime": 42, "lastUpdatedTime": 42}
        first = DataSetResponse._load(first_dict)
        # Simulate that the data set is already in CDF
        toolkit_client_approval.append(DataSetResponse, first)

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([LOAD_DATA / "data_sets" / "1.my_datasets.yaml"])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 1, "change": 0, "delete": 0, "unchanged": 1}
