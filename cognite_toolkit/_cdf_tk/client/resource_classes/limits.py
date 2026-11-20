"""Resource classes for the Cognite Limits API.

Based on the alpha API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Limits
"""

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import LimitId


class LimitValueResponse(BaseModelObject):
    """A limit value for a project, e.g. ``atlas.monthly_ai_tokens``."""

    limit_id: str
    value: float

    def as_id(self) -> LimitId:
        return LimitId(limit_id=self.limit_id)
