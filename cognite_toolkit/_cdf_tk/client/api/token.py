from functools import cached_property

from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.iam import TokenInspection


class TokenAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    @cached_property
    def token(self) -> TokenInspection:
        return self._client.iam.token.inspect()

    def get_scope(self, actions: list[Capability.Action]) -> Capability.Scope:
        raise NotImplementedError()
