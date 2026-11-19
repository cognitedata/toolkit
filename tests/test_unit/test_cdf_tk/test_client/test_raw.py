import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestRawRowsAPI:
    def test_cursors(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/raw/dbs/my_db/tables/my_table/cursors")).mock(
            return_value=httpx2.Response(200, json={"items": ["c1", "c2"]})
        )

        cursors = toolkit_client.raw.tables.rows.cursors("my_db", "my_table", number_of_cursors=2)

        assert (cursors, dict(route.calls[0].request.url.params)) == (["c1", "c2"], {"numberOfCursors": "2"})
