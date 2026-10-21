from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import MQTTSourceRequest
from cognite_toolkit._cdf_tk.resource_ios import HostedExtractorSourceIO
from tests.test_unit.approval_client import ApprovalToolkitClient

PEM_CERT = "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIU\n-----END CERTIFICATE-----"
PEM_KEY = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG\n-----END PRIVATE KEY-----"

SOURCE_YAML = """type: mqtt5
externalId: my_mqtt
host: localhost
caCertificate:
  type: pem
  certificate: ${CA_CERT}
authCertificate:
  type: pem
  certificate: ${AUTH_CERT}
  key: ${AUTH_KEY}
  keyPassword: my_password
"""


def _escape(value: str) -> str:
    return value.replace("\n", "\\n")


class TestHostedExtractorSourceIO:
    @pytest.mark.parametrize(
        "env_vars",
        [
            pytest.param({"CA_CERT": PEM_CERT, "AUTH_CERT": PEM_CERT, "AUTH_KEY": PEM_KEY}, id="real newlines"),
            pytest.param(
                {"CA_CERT": _escape(PEM_CERT), "AUTH_CERT": _escape(PEM_CERT), "AUTH_KEY": _escape(PEM_KEY)},
                id="escaped newlines",
            ),
        ],
    )
    def test_load_certificates_from_environment_variables(
        self, env_vars: dict[str, str | None], toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        io = HostedExtractorSourceIO.create_io(toolkit_client_approval.mock_client)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = SOURCE_YAML

        raw = io.load_resource_file(filepath, env_vars)[0]

        assert raw["caCertificate"]["certificate"] == PEM_CERT
        assert raw["authCertificate"]["certificate"] == PEM_CERT
        assert raw["authCertificate"]["key"] == PEM_KEY
        assert raw["authCertificate"]["keyPassword"] == "my_password"

        loaded = io.load_resource(raw)
        assert isinstance(loaded, MQTTSourceRequest)
        assert loaded.auth_certificate is not None
        assert loaded.auth_certificate.certificate == PEM_CERT

    def test_escaped_and_real_newlines_give_same_hash(self, toolkit_client_cheap: ToolkitClient) -> None:
        io = HostedExtractorSourceIO.create_io(toolkit_client_cheap)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = SOURCE_YAML

        real = io.load_resource_file(filepath, {"CA_CERT": PEM_CERT, "AUTH_CERT": PEM_CERT, "AUTH_KEY": PEM_KEY})[0]
        escaped = io.load_resource_file(
            filepath, {"CA_CERT": _escape(PEM_CERT), "AUTH_CERT": _escape(PEM_CERT), "AUTH_KEY": _escape(PEM_KEY)}
        )[0]

        assert real["metadata"] == escaped["metadata"]
