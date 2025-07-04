import pytest
import requests

from cognite_toolkit._cdf_tk.constants import URL


@pytest.mark.parametrize(
    "url, name",
    [pytest.param(url, name, id=name) for name, url in vars(URL).items() if not name.startswith("_")],
)
def test_url_returns_200(url: str, name: str) -> None:
    assert requests.get(url).status_code == 200, f"Failed to get a 200 response from the URL.{name}."
