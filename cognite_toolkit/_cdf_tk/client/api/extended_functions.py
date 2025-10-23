from cognite.client import CogniteClient
from cognite.client._api.functions import FunctionsAPI
from cognite.client.config import global_config
from cognite.client.data_classes import Function, FunctionWrite
from cognite.client.utils.useful_types import SequenceNotStr
from rich.console import Console

from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, SimpleBodyRequest
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class ExtendedFunctionsAPI(FunctionsAPI):
    def __init__(
        self,
        config: ToolkitClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        """
        Extended Functions API to include custom headers and payload preparation.
        """
        super().__init__(config, api_version, cognite_client)
        self._toolkit_config = config
        self._toolkit_http_client = HTTPClient(config, max_retries=global_config.max_retries)

    def create_with_429_retry(self, function: FunctionWrite, console: Console | None = None) -> Function:
        """Create a function with manual retry handling for 429 Too Many Requests responses.

        This method is a workaround for scenarios where the function creation API is temporarily unavailable
        due to a full queue on the server side.

        Args:
            function (FunctionWrite): The function to create.
            console (Console | None): The rich console to use for printing warnings.

        Returns:
            Function: The created function object.
        """
        result = self._toolkit_http_client.request_with_retries(
            message=SimpleBodyRequest(
                endpoint_url=self._toolkit_config.create_api_url("/functions"),
                method="POST",
                body_content={"items": [function.dump(camel_case=True)]},
            ),
            console=console,
        )
        result.raise_for_status()
        # We assume the API response is one item on a successful creation
        return Function._load(result.get_first_body()["items"][0], cognite_client=self._cognite_client)  # type: ignore[arg-type,index]

    def delete_with_429_retry(
        self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False, console: Console | None = None
    ) -> None:
        """Delete one or more functions with retry handling for 429 Too Many Requests responses.

        This method is an improvement over the standard delete method in the FunctionsAPI.


        Args:
            external_id (SequenceNotStr[str]): The external IDs of the functions to delete.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found.
            console (Console | None): The rich console to use for printing warnings.

        Returns:
            None
        """
        for chunk in chunker(external_id, self._DELETE_LIMIT):
            body_content: dict[str, JsonVal] = {
                "items": [{"externalId": eid} for eid in chunk],
            }
            if ignore_unknown_ids:
                body_content["ignoreUnknownIds"] = True
            self._toolkit_http_client.request_with_retries(
                message=SimpleBodyRequest(
                    endpoint_url=self._toolkit_config.create_api_url("/functions/delete"),
                    method="POST",
                    body_content=body_content,
                ),
                console=console,
            ).raise_for_status()
        return None
