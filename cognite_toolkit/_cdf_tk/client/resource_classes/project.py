import sys
from collections.abc import Iterator
from typing import Literal

from pydantic import RootModel

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class UserProfilesConfiguration(BaseModelObject):
    enabled: bool


class Claim(BaseModelObject):
    claim_name: str


class OidcConfiguration(BaseModelObject):
    jwks_url: str
    token_url: str | None = None
    issuer: str
    audience: str
    skew_ms: int | None = None
    access_claims: list[Claim]
    scope_claims: list[Claim]
    log_claims: list[Claim]
    is_group_callback_enabled: bool | None = None
    identity_provider_scope: str | None = None


class OrganizationResponse(BaseModelObject):
    name: str
    url_name: str
    organization: str
    user_profiles_configuration: UserProfilesConfiguration
    oidc_configuration: OidcConfiguration | None = None


class ProjectStatus(BaseModelObject):
    """Project status information."""

    data_modeling_status: Literal["HYBRID", "DATA_MODELING_ONLY"]
    url_name: str


class ProjectStatusList(RootModel[list[ProjectStatus]]):
    root: list[ProjectStatus]
    _project: str

    def __iter__(self) -> Iterator[ProjectStatus]:  # type: ignore[override]
        return iter(self.root)

    def __len__(self) -> int:
        return len(self.root)

    def __getitem__(self, index: int) -> ProjectStatus:
        return self.root[index]

    @classmethod
    def _load(cls, data: list[dict]) -> Self:
        """Load from a list of dictionaries."""
        return cls(root=[ProjectStatus._load(item) for item in data], _project="")

    @property
    def this_project(self) -> ProjectStatus:
        """Returns the ProjectStatus of the current project."""
        for item in self.root:
            if self._project == item.url_name:
                return item
        raise ValueError(f"Project '{self._project}' not found in the list of projects.")
