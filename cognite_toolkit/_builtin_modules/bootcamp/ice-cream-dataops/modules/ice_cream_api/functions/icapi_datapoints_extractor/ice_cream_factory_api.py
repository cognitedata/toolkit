from typing import Dict, Union

import orjson
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

    def get_datapoints(
        self, timeseries_ext_id: str, start: Union[str, int, float], end: Union[str, int, float]
    ):
        """
        Get datapoints for a timeseries external id. This will also return datapoints for an associated timeseries

        (e.g. request for external id "HPM2C561:planned_status" will return datapoints for "HPM2C561:planned_status" AND
        "HPM2C561:status". Similar, request for timeseries with external id "HPM2C561:count" will return datapoints for
        "HPM2C561:count" AND ""HPM2C561:good").

        Args:
            timeseries_ext_id: external id of timeseries to get datapoints for
            start: start for datapoints (UNIX timestamp (int, float) or string with format 'YYYY-MM-DD HH:MM')
            end: end for datapoints (UNIX timestamp (int, float) or string with format 'YYYY-MM-DD HH:MM')
        """
        params = {"start": start, "end": end, "external_id": timeseries_ext_id}
        response = self.get_response(headers={}, url_suffix="datapoints/oee", params=params)

        datapoints_dict = orjson.loads(response.content)

        for ts, dps in datapoints_dict.items():
            # convert timestamp to ms (*1000) for CDF uploads
            datapoints_dict[ts] = [(dp[0] * 1000, dp[1]) for dp in dps]

        return datapoints_dict
