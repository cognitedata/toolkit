from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelResponse
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType


class ThreeDModelAPI:
    ENDPOINT = "/3d/models"
    _LIST_REQUEST_MAX_LIMIT = 1000

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self._console = console
        self._config = http_client.config

    def iterate(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ThreeDModelResponse]:
        if not (0 < limit <= self._LIST_REQUEST_MAX_LIMIT):
            raise ValueError(f"Limit must be between 1 and {self._LIST_REQUEST_MAX_LIMIT}, got {limit}.")
        parameters: dict[str, PrimitiveType] = {
            # There is a bug in the API. The parameter includeRevisionInfo is expected to be lower case and not
            # camel case as documented. You get error message: Unrecognized query parameter includeRevisionInfo,
            # did you mean includerevisioninfo?
            "includerevisioninfo": include_revision_info,
            "limit": limit,
        }
        if published is not None:
            parameters["published"] = published
        if cursor is not None:
            parameters["cursor"] = cursor
        responses = self._http_client.request_with_retries(
            ParamRequest(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="GET",
                parameters=parameters,
            )
        )
        responses.raise_for_status()
        return PagedResponse[ThreeDModelResponse].model_validate(responses.get_first_body())

    def list(
        self,
        published: bool | None = None,
        include_revision_info: bool = False,
        limit: int | None = 100,
        cursor: str | None = None,
    ) -> list[ThreeDModelResponse]:
        results: list[ThreeDModelResponse] = []
        while True:
            request_limit = (
                self._LIST_REQUEST_MAX_LIMIT
                if limit is None
                else min(limit - len(results), self._LIST_REQUEST_MAX_LIMIT)
            )
            if request_limit <= 0:
                break
            page = self.iterate(
                published=published,
                include_revision_info=include_revision_info,
                limit=request_limit,
                cursor=cursor,
            )
            results.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return results


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.models = ThreeDModelAPI(http_client, console)
