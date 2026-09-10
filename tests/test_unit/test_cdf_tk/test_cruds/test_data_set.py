from copy import deepcopy

from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.commands import DeployV2Command
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import ReadResource
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataSetsLoader:
    def test_upsert_data_set(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ):
        loader = DataSetsIO.create_loader(env_vars_with_client.get_client())
        filepath = LOAD_DATA / "data_sets" / "1.my_datasets.yaml"
        raw_list = loader.load_resource_file(filepath, env_vars_with_client.dump())
        assert len(raw_list) == 2

        # Set the properties that are set on the server side and load as DataSetResponse
        first_dict = {**raw_list[0], "id": 42, "createdTime": 42, "lastUpdatedTime": 42}
        first = DataSetResponse._load(first_dict)
        # Simulate that the data set is already in CDF
        toolkit_client_approval.append(DataSetResponse, first)

        resource_by_id = {}
        for resource_dict in raw_list:
            resource = loader.load_resource(deepcopy(resource_dict))
            resource_id = loader.get_id(resource)
            resource_by_id[resource_id] = ReadResource(resource, resource_dict, [filepath])
        existing_list = loader.retrieve(list(resource_by_id.keys()))
        result = DeployV2Command.categorize_resources(
            loader,
            resource_by_id=resource_by_id,
            cdf_by_id={loader.get_id(item): item for item in existing_list},
        )

        assert {
            "create": len(result.to_create),
            "change": len(result.to_update),
            "delete": len(result.to_delete),
            "unchanged": len(result.unchanged),
        } == {"create": 1, "change": 0, "delete": 0, "unchanged": 1}
