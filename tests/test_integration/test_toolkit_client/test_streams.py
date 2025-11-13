from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.streams import StreamRequest, StreamResponse
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestStreamsAPI:
    def test_create_list_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        stream = StreamRequest.model_validate(
            {
                "externalId": f"test_stream_{RUN_UNIQUE_ID}".lower(),
                "settings": {"template": {"name": "ImmutableTestStream"}},
            }
        )

        # create stream
        created_list = toolkit_client.streams.create([stream])
        assert len(created_list) == 1
        assert isinstance(created_list[0], StreamResponse)
        assert created_list[0].external_id == stream.external_id
        assert created_list[0].created_from_template == "ImmutableTestStream"

        # list streams
        all_streams = toolkit_client.streams.list()
        assert len(all_streams) == 1
        assert all_streams[0].external_id == stream.external_id

        # retrieve stream
        retrieved = toolkit_client.streams.retrieve(stream.external_id)
        assert isinstance(retrieved, StreamResponse)
        assert retrieved.external_id == stream.external_id
        assert retrieved.created_time > 0
        assert retrieved.created_from_template == "ImmutableTestStream"

        # delete stream
        toolkit_client.streams.delete(stream.external_id)

        # list streams after delete
        all_streams_after_delete = toolkit_client.streams.list()
        assert len(all_streams_after_delete) == 0
