import httpx
import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import CRUD_LIST, Loader


class TestResourceCRUD:
    @pytest.mark.parametrize(
        "crud_cls", [crud_cls for crud_cls in CRUD_LIST if crud_cls.folder_name != "robotics"]
    )  # Robotics does not have a public doc_url
    def test_doc_url_is_valid(self, crud_cls: type[Loader], toolkit_client: ToolkitClient) -> None:
        crud = crud_cls.create_loader(toolkit_client)

        url = crud.doc_url()
        response = httpx.get(url, follow_redirects=True)  # raises for bad responses
        if response.status_code < 200 or response.status_code > 300:
            raise AssertionError(
                f"{crud.display_name} doc_url is not accessible ({url}). Status code: {response.status_code}"
            )
