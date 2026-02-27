"""User Profile resource classes for the Cognite User Profiles API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/User-profiles
"""

from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import UserProfileId

IdentityType = Literal["USER", "SERVICE_PRINCIPAL", "INTERNAL_SERVICE"]


class UserProfile(BaseModelObject):
    user_identifier: str
    display_name: str | None = None
    given_name: str | None = None
    surname: str | None = None
    email: str | None = None
    job_title: str | None = None
    identity_type: IdentityType
    last_updated_time: int

    def as_id(self) -> UserProfileId:
        return UserProfileId(user_identifier=self.user_identifier)
