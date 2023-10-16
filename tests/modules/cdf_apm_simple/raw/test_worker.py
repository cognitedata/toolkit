import pandas
from pydantic import ValidationError
import pytest
import os

from scripts.worker import RawDatabaseContent, Worker
from cognite.client.testing import CogniteClientMock


@pytest.fixture
def file():
    dir = os.path.dirname(os.path.abspath(__file__))
    return f"{dir}/data/workitems.csv"


def test_load_content(file):
    df = pandas.read_csv(file)
    assert df is not None

    data = RawDatabaseContent(database_name="workitems", table_name="workitems", data=df)
    assert data is not None


def test_write_content(file):

    worker = Worker(
        client=CogniteClientMock(),
        content=[
            RawDatabaseContent(
                database_name="workitems",
                table_name="workitems",
                data=pandas.read_csv(file),
            )
        ],
        reset=True
    )

    assert worker is not None

    worker.createtables()

    #todo: assert that table api is called with delete/write

