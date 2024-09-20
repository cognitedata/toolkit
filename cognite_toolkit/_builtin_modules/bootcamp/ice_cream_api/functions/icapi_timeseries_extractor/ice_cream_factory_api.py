from typing import Dict, Union

import orjson
from cognite.client.data_classes import TimeSeries
from requests import Response, Session, adapters


class IceCreamFactoryAPI:
    """Class for Ice Cream Factory API."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.adapter = adapters.HTTPAdapter(max_retries=3)
        self.session = Session()
        self.session.mount("https://", self.adapter)

    def get_response(
        self, headers: Dict[str, str], url_suffix: str, params: Dict[str, Union[str, int, float]] = {}
    ) -> Response:
        """
        Get response from API.

        Args:
            headers: request header
            url_suffix: string to add to base url
            params: query parameters
        """

        response = self.session.get(f"{self.base_url}/{url_suffix}", headers=headers, timeout=40, params=params)
        response.raise_for_status()
        return response

    def get_timeseries(self):
        """
        Get sites from the Ice Cream API and create a list Assets
        """
        response = self.get_response(headers={}, url_suffix="timeseries/oee")

        timeseries = orjson.loads(response.content)

        timeseries = [TimeSeries(**ts) for ts in timeseries]

        return timeseries
