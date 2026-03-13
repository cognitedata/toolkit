from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling.instances import (
    InstanceApply,
)


class InstanceApplyList(CogniteResourceList[InstanceApply]):
    """A list of instances to be applied (created or updated)."""

    _RESOURCE = InstanceApply
