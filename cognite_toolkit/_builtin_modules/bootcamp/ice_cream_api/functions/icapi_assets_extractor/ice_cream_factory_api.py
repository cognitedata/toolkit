from typing import Dict, Union

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

    def get_sites_csv(self):
        """
        Get a dataframe for all sites from the Ice Cream API's site/{city}/csv endpoint
        """
        response = self.get_response(headers={}, url_suffix="site/all/csv")

        return response.text
