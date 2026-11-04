from cognite.client.credentials import CredentialProvider


class NoCredentials(CredentialProvider):
    def authorization_header(self) -> tuple[str, str]:
        raise RuntimeError("The NoCredentials credential provider does not provide any credentials.")
