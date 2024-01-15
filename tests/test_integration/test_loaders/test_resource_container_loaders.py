from cognite.client import CogniteClient


def test_contact_with_client(cognite_client: CogniteClient):
    token = cognite_client.iam.token.inspect()

    assert token.capabilities
