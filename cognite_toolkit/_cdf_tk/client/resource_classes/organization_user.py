"""Organization user resource classes for the Cognite Organization Users API.

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/User-profiles
"""

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class OrganizationUser(BaseModelObject):
    id: str
    picture_url: str
    email: str | None = None
    name: str | None = None
    given_name: str | None = None
    middle_name: str | None = None
    family_name: str | None = None
