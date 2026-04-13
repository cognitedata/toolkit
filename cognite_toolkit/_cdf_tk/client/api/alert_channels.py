from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.alert_channel import AlertChannelResponse


class AlertChannelsAPI(CDFResourceAPI[AlertChannelResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={"list": Endpoint(method="POST", path="/alerts/channels/list", item_limit=1000)},
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[AlertChannelResponse]:
        return PagedResponse[AlertChannelResponse].model_validate_json(response.body)

    def list(self) -> list[AlertChannelResponse]:
        """Lists all alert channels in the project."""
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(method=endpoint.method, endpoint_url=self._make_url(endpoint.path), body_content={})
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items
