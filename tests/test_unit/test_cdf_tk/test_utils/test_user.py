from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.resource_classes.principal import ServiceAccountPrincipal, UserPrincipal
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.utils.user import UserInfo


def test_load_from_user_principal() -> None:
    with monkeypatch_toolkit_client() as client:
        client.principals.me.return_value = UserPrincipal(
            id="user-uuid-123", name="Alice", email="alice@example.com", picture_url="http://example.com/pic.jpg"
        )

    result = UserInfo.load(client)

    assert result.model_dump(exclude_unset=True) == {
        "email": "alice@example.com",
        "id": "user-uuid-123",
        "name": "Alice",
        "type": "user",
    }


def test_load_from_service_account_principal() -> None:
    client = _mock_tk_client()
    principal = MagicMock(spec=ServiceAccountPrincipal)
    principal.id = "sa-uuid-456"
    principal.name = "my-sa"
    principal.external_id = "sa-ext-id"
    client.principals.me.return_value = principal

    result = UserInfo.load(client)

    assert result.type == "service_account"
    assert result.id == "sa-uuid-456"
    assert result.external_id == "sa-ext-id"


def test_load_falls_back_to_user_profile() -> None:
    client = _mock_tk_client()
    client.principals.me.side_effect = Exception("unauthorized")

    profile = MagicMock()
    profile.user_identifier = "profile-uid-789"
    profile.display_name = "Bob"
    profile.email = "bob@example.com"

    client.user_profiles.me.return_value = profile

    result = UserInfo.load(client)

    assert result.type == "user"
    assert result.id == "profile-uid-789"


def test_load_returns_unknown_when_all_fail() -> None:
    client = _mock_tk_client()
    client.principals.me.side_effect = Exception("error")
    client.user_profiles.me.side_effect = Exception("error")

    result = UserInfo.load(client)

    assert result == UserInfo(type="unknown")
    assert result.id is None
