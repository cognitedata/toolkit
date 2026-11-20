import asyncio

import httpx2

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_IO_LIST,
    AppIO,
    AppVersionIO,
    DataProductIO,
    DataProductVersionIO,
    ResourceIO,
)

# APIs excluded from the doc_url test: either unofficial/internal APIs not publicly documented,
# or APIs whose public documentation is not yet available.
INTERNAL_DOCS = {
    DataProductIO,
    DataProductVersionIO,
    AppIO,
    AppVersionIO,
}
DOC_URL_ATTEMPTS = 3


class TestResourceCRUD:
    def test_doc_url_is_valid(self, toolkit_client: ToolkitClient) -> None:
        """Test that all CRUD doc_urls are accessible (requests made in parallel)."""
        # Robotics does not have a public doc_url
        crud_classes = [crud_cls for crud_cls in RESOURCE_IO_LIST if crud_cls not in INTERNAL_DOCS]

        async def check_url(crud_cls: type[ResourceIO], client: httpx2.AsyncClient) -> str | None:
            crud = crud_cls(toolkit_client)
            url = crud.doc_url()
            error: str | None = None
            for attempt in range(DOC_URL_ATTEMPTS):
                if attempt:
                    await asyncio.sleep(2**attempt)
                try:
                    response = await client.get(url, follow_redirects=True)
                except httpx2.RequestError as e:
                    error = f"{crud.display_name} doc_url request failed ({url}). Error: {type(e).__name__} {e}"
                    continue
                if 200 <= response.status_code < 300:
                    return None
                error = f"{crud.display_name} doc_url is not accessible ({url}). Status code: {response.status_code}"
                if response.status_code < 500:
                    return error
            return error

        async def check_all_urls() -> list[str]:
            async with httpx2.AsyncClient(timeout=30.0) as client:
                results = await asyncio.gather(*[check_url(crud_cls, client) for crud_cls in crud_classes])
            return [error for error in results if error is not None]

        errors = asyncio.run(check_all_urls())
        if errors:
            raise AssertionError("The following doc_urls are not accessible:\n - " + "\n - ".join(errors))
