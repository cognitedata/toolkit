from __future__ import annotations

from cognite.client import CogniteClient


def handle(data: dict, client: CogniteClient) -> dict:
    print("Running handler with data: ", data)
    print("Token Inspect: ", client.iam.token.inspect())
    return {"status": "succeeded", "data": data}
