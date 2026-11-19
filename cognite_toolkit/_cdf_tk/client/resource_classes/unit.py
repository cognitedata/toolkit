from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class UnitConversion(BaseModelObject):
    """The multiplier and offset for converting between units of the same quantity."""

    multiplier: float
    offset: float


class UnitResponse(BaseModelObject):
    """A unit in the CDF unit catalog, for example "temperature:deg_c"."""

    external_id: str
    name: str
    long_name: str
    symbol: str
    alias_names: list[str]
    quantity: str
    conversion: UnitConversion
    source: str | None = None
    source_reference: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class UnitSystemQuantity(BaseModelObject):
    name: str
    unit_external_id: str


class UnitSystemResponse(BaseModelObject):
    """A unit system, for example "SI", with the default unit of each quantity."""

    name: str
    quantities: list[UnitSystemQuantity]
