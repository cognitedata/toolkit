from cognite_toolkit._cdf_tk.client.resource_classes.principal import (
    CreatedBy,
    ServiceAccountPrincipal,
    UserPrincipal,
)
from cognite_toolkit._cdf_tk.client.resource_classes.user_profile import UserProfile
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
    with monkeypatch_toolkit_client() as client:
        client.principals.me.return_value = ServiceAccountPrincipal(
            id="sa-uuid-456",
            name="my-sa",
            external_id="sa-ext-id",
            picture_url="http://example.com/sa-pic.jpg",
            description="A service account",
            created_by=CreatedBy(org_id="org-123", user_id="user-123"),
            created_time=1234567890,
            last_updated_time=1234567890,
        )

    result = UserInfo.load(client)

    assert result.model_dump(exclude_unset=True) == {
        "type": "service_account",
        "id": "sa-uuid-456",
        "name": "my-sa",
        "external_id": "sa-ext-id",
    }


def test_load_falls_back_to_user_profile() -> None:
    with monkeypatch_toolkit_client() as client:
        client.principals.me.side_effect = Exception("unauthorized")
        client.user_profiles.me.return_value = UserProfile(
            user_identifier="profile-uid-789",
            display_name="Bob",
            email="bob@example.com",
            identity_type="USER",
            last_updated_time=1234567890,
        )

    result = UserInfo.load(client)

    assert result.model_dump(exclude_unset=True) == {
        "type": "user",
        "id": "profile-uid-789",
        "name": "Bob",
        "email": "bob@example.com",
    }


def test_load_returns_unknown_when_all_fail() -> None:
    with monkeypatch_toolkit_client() as client:
        client.principals.me.side_effect = Exception("error")
        client.user_profiles.me.side_effect = Exception("error")

    result = UserInfo.load(client)

    assert result == UserInfo(type="unknown")
