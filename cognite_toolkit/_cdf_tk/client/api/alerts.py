from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .alert_channels import AlertChannelsAPI


class AlertsAPI:
    def __init__(self, http_client: HTTPClient) -> None:
        self.channels = AlertChannelsAPI(http_client)
