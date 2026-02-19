import asyncio

import httpx

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import CRUD_LIST, Loader


class TestResourceCRUD:
    def test_doc_url_is_valid(self, toolkit_client: ToolkitClient) -> None:
        """Test that all CRUD doc_urls are accessible (requests made in parallel)."""
        # Robotics does not have a public doc_url; Data Products docs are not yet published (404).
        skip_folders = {"robotics", "data_products"}
        crud_classes = [crud_cls for crud_cls in CRUD_LIST if crud_cls.folder_name not in skip_folders]

        async def check_url(crud_cls: type[Loader], client: httpx.AsyncClient) -> str | None:
            crud = crud_cls.create_loader(toolkit_client)
            url = crud.doc_url()
            try:
                response = await client.get(url, follow_redirects=True)
                if response.status_code < 200 or response.status_code >= 300:
                    return f"{crud.display_name} doc_url is not accessible ({url}). Status code: {response.status_code}"
            except httpx.RequestError as e:
                return f"{crud.display_name} doc_url request failed ({url}). Error: {e}"
            return None

        async def check_all_urls() -> list[str]:
            async with httpx.AsyncClient() as client:
                results = await asyncio.gather(*[check_url(crud_cls, client) for crud_cls in crud_classes])
            return [error for error in results if error is not None]

        errors = asyncio.run(check_all_urls())
        if errors:
            raise AssertionError("The following doc_urls are not accessible:\n" + "\n - ".join(errors))
