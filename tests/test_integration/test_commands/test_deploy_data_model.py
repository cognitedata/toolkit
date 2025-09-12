import pytest
from cognite.client import data_modeling as dm

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, SpaceCRUD, ViewCRUD
from tests.data import STRONGLY_COUPLED_MODEL


@pytest.fixture()
def deployed_container_space_coupled_model(toolkit_client: ToolkitClient) -> None:
    space_loader = SpaceCRUD(toolkit_client, STRONGLY_COUPLED_MODEL, None)
    files = space_loader.find_files()
    assert len(files) == 1
    space = dm.SpaceApply.load(files[0].read_text(encoding="utf-8"))
    if not space_loader.retrieve([space.as_id()]):
        created_space = space_loader.create([space])
        assert len(created_space) == 1

    container_loader = ContainerCRUD(toolkit_client, STRONGLY_COUPLED_MODEL, None)
    containers = dm.ContainerApplyList(
        [dm.ContainerApply.load(file.read_text(encoding="utf-8")) for file in container_loader.find_files()]
    )

    if len(container_loader.retrieve(containers.as_ids())) != len(containers):
        created_containers = container_loader.create(containers)
        assert len(created_containers) == len(containers)


@pytest.mark.usefixtures("deployed_container_space_coupled_model")
def test_deploy_strongly_coupled_model(toolkit_client: ToolkitClient) -> None:
    loader = ViewCRUD(toolkit_client, STRONGLY_COUPLED_MODEL, None)
    views = dm.ViewApplyList([dm.ViewApply.load(file.read_text(encoding="utf-8")) for file in loader.find_files()])

    try:
        created = loader.create(views)

        assert len(views) == len(created)
    finally:
        toolkit_client.data_modeling.views.delete(views.as_ids())
