from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricTypeExtended

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class AssetCentricExternalId(Identifier):
    resource_type: AssetCentricTypeExtended
    external_id: str

    def __str__(self) -> str:
        return f"{self.resource_type}_externalId={self.external_id}"

    @property
    def id_value(self) -> str:
        """Generic name of the identifier.

        The AssetCentricId has the same property. Thus, this means that these two
        classes can be used interchangeably when only the value of the identifier is needed, and not the type.
        """
        return self.external_id
