from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class LimitValue(BaseModelObject):
    limit_id: str
    value: float
