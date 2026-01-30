from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.instances import MultiWrappedInstancesAPI, WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    InFieldCDMLocationConfigRequest,
    InFieldCDMLocationConfigResponse,
    InFieldLocationConfigRequest,
    InFieldLocationConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedNodeIdentifier,
)


class InfieldConfigAPI(MultiWrappedInstancesAPI[InFieldLocationConfigRequest, InFieldLocationConfigResponse]): ...


class InFieldCDMConfigAPI(
    WrappedInstancesAPI[TypedNodeIdentifier, InFieldCDMLocationConfigRequest, InFieldCDMLocationConfigResponse]
):
    def _validate_response(self, response: SuccessResponse) -> ResponseItems[TypedNodeIdentifier]:
        return ResponseItems[TypedNodeIdentifier].model_validate_json(response.body)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[InFieldCDMLocationConfigResponse]:
        return PagedResponse[InFieldCDMLocationConfigResponse].model_validate_json(response.body)


class InfieldAPI:
    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self._http_client = http_client
        self.config = InfieldConfigAPI(http_client)
        self.cdm_config = InFieldCDMConfigAPI(http_client)
