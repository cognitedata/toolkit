from cognite.client import CogniteClient


class RunAPI:
    def __init__(self, cognite_client: CogniteClient):
        self._client = cognite_client

    def transformation(self, external_id: str) -> None: ...

    def function(self, external_id: str) -> None: ...
