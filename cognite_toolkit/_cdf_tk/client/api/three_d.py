from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.api_classes import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelResponse
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, RequestMessage2
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType


class ThreeDModelAPI:
    ENDPOINT = "/3d/models"

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
        if not (0 < limit <= 1000):
            raise ValueError("Limit must be between 1 and 1000.")
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
        responses = self._http_client.request_single_retries(
            RequestMessage2(
                endpoint_url=self._config.create_api_url(self.ENDPOINT),
                method="GET",
                parameters=parameters,
            )
        )
        success_response = responses.get_success_or_raise()
        return PagedResponse[ThreeDModelResponse].model_validate(success_response.body_json)


class ThreeDAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.models = ThreeDModelAPI(http_client, console)
