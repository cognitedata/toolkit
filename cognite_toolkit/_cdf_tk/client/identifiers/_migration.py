import sys
from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricTypeExtended

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class AssetCentricExternalId(Identifier):
    resource_type: AssetCentricTypeExtended
    external_id: str

    def __str__(self) -> str:
        return f"{self.resource_type}_externalId={self.external_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"resourceType-{self.resource_type}.externalId-{self.external_id}"
        return f"{self.resource_type}.{self.external_id}"

    @property
    def id_value(self) -> str:
        """Generic name of the identifier.

        The AssetCentricId has the same property. Thus, this means that these two
        classes can be used interchangeably when only the value of the identifier is needed, and not the type.
        """
        return self.external_id

    @classmethod
    def from_external_ids(cls, resource_type: AssetCentricTypeExtended, external_ids: Iterable[str]) -> list[Self]:
        return [cls(resource_type=resource_type, external_id=ext_id) for ext_id in external_ids]
