"""Metering resource classes for the Cognite Metering API (alpha).

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Metering
"""

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import MeterId


class MeterDatapoint(BaseModelObject):
    timestamp: int
    average: float


class Meter(BaseModelObject):
    """Consumption data for a single meter.

    The meter id consists of the service name and a service-scoped metering name,
    for example ``atlas.monthly_ai_tokens``. The datapoints are only populated when
    ``start`` and ``number_of_datapoints`` are given in the request.
    """

    meter_id: str
    datapoints: list[MeterDatapoint]

    def as_id(self) -> MeterId:
        return MeterId(meter_id=self.meter_id)
