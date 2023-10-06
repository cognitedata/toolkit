# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import json
import pandas as pd
from typing import List, Dict, Any
from cognite.client.data_classes.time_series import TimeSeries
from cognite.client.data_classes.iam import Group
from .utils import CDFToolConfig
from utils.transformations_config import parse_transformation_configs
from utils.transformations_api import (
    to_transformation,
    get_existing_transformation_ext_ids,
    get_new_transformation_ids,
    upsert_transformations,
)
from cognite.client.exceptions import CogniteNotFoundError


def load_raw(ToolGlobals: CDFToolConfig, file: str, drop: bool, directory=None) -> None:
    """Load raw data from csv files into CDF Raw

    Args:
        file: name of file to load, if empty load all files
        drop: whether to drop existing data
    """
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/data/raw"
    client = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})
    # The name of the raw database to create is picked up from the inventory.py file, which
    # again is templated with cookiecutter based on the user's input.
    raw_db = ToolGlobals.config("raw_db")
    if raw_db == "":
        print(
            f"Could not find raw_db in inventory.py for example {ToolGlobals.example}."
        )
        ToolGlobals.failed = True
        return
    try:
        if drop:
            tables = client.raw.tables.list(raw_db)
            if len(tables) > 0:
                for table in tables:
                    client.raw.tables.delete(raw_db, table.name)
            client.raw.databases.delete(raw_db)
            print(f"Deleted {raw_db} for example {ToolGlobals.example}.")
    except:
        print(
            f"Failed to delete {raw_db} for example {ToolGlobals.example}. It may not exist."
        )
    try:
        # Creating the raw database and tables is actually not necessary as
        # the SDK will create them automatically when inserting data with insert_dataframe()
        # using the ensure_parent=True argument.
        # However, it is included to show how you can use the SDK.
        client.raw.databases.create(raw_db)
    except Exception as e:
        print(
            f"Failed to create {raw_db} for example {ToolGlobals.example}: {e.message}"
        )
        ToolGlobals.failed = True
        return
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder of the example.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".csv" in f:
                    files.append(f)
    if len(files) == 0:
        return
    print(f"Uploading {len(files)} .csv files to {raw_db} RAW database...")
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            dataframe = pd.read_csv(file, dtype=str)
            dataframe = dataframe.fillna("")
            try:
                client.raw.rows.insert_dataframe(
                    db_name=raw_db,
                    table_name=f[:-4],
                    dataframe=dataframe,
                    ensure_parent=True,
                )
            except Exception as e:
                print(f"Failed to upload {f} for example {ToolGlobals.example}")
                print(e)
                ToolGlobals.failed = True
                return
    print(f"Successfully uploaded {len(files)} raw csv files to {raw_db} RAW database.")


def load_files(
    ToolGlobals: CDFToolConfig, file: str, drop: bool, directory=None
) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/data/files"
    try:
        client = ToolGlobals.verify_client(capabilities={"filesAcl": ["READ", "WRITE"]})
        files = []
        if file is not None and len(file) > 0:
            files.append(file)
        else:
            # Pick up all the files in the files folder of the example.
            for _, _, filenames in os.walk(directory):
                for f in filenames:
                    files.append(f)
        if len(files) == 0:
            return
        print(f"Uploading {len(files)} files/documents to CDF...")
        for f in files:
            client.files.upload(
                path=f"{directory}/{f}",
                data_set_id=ToolGlobals.data_set_id,
                name=f,
                external_id=ToolGlobals.example + "_" + f,
                overwrite=drop,
            )
        print(
            f"Uploaded successfully {len(files)} files/documents from example {ToolGlobals.example}"
        )
    except Exception as e:
        print(f"Failed to upload files for example {ToolGlobals.example}")
        print(e)
        ToolGlobals.failed = True
        return


def load_timeseries(
    ToolGlobals: CDFToolConfig, file: str, drop: bool, directory=None
) -> None:
    load_timeseries_metadata(ToolGlobals, file, drop, directory=directory)
    if directory is not None:
        directory = f"{directory}/datapoints"
    load_timeseries_datapoints(ToolGlobals, file, directory=directory)


def load_timeseries_metadata(
    ToolGlobals: CDFToolConfig, file: str, drop: bool, directory=None
) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/data/timeseries"
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .json files in the data folder of the example.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".json" in f:
                    files.append(f)
    # Read timeseries metadata
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            ts = json.load(file)
            for t in ts:
                ts = TimeSeries()
                for k, v in t.items():
                    ts.__setattr__(k, v)
                timeseries.append(ts)
    if len(timeseries) == 0:
        return
    drop_ts: list[str] = []
    for t in timeseries:
        # Set the context info for this CDF project
        t.data_set_id = ToolGlobals.data_set_id
        if drop:
            drop_ts.append(t.external_id)
    try:
        if drop:
            client.time_series.delete(external_id=drop_ts, ignore_unknown_ids=True)
            print(
                f"Deleted {len(drop_ts)} timeseries for example {ToolGlobals.example}."
            )
    except Exception as e:
        print(
            f"Failed to delete {t.external_id} for example {ToolGlobals.example}. It may not exist."
        )
    try:
        client.time_series.create(timeseries)
    except Exception as e:
        print(f"Failed to upload timeseries for example {ToolGlobals.example}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Loaded {len(timeseries)} timeseries from {len(files)} files.")


def load_timeseries_datapoints(
    ToolGlobals: CDFToolConfig, file: str, directory=None
) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/data/timeseries/datapoints"
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder of the example.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".csv" in f:
                    files.append(f)
    if len(files) == 0:
        return
    print(f"Uploading {len(files)} .csv file(s) as datapoints to CDF timeseries...")
    try:
        for f in files:
            with open(f"{directory}/{f}", "rt") as file:
                dataframe = pd.read_csv(file, parse_dates=True, index_col=0)
            print(f"Uploading {f} as datapoints to CDF timeseries...")
            client.time_series.data.insert_dataframe(dataframe)
        print(f"Uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries.")
    except Exception as e:
        print(f"Failed to upload datapoints for example {ToolGlobals.example}.")
        print(e)
        ToolGlobals.failed = True
        return


def load_transformations(
    ToolGlobals: CDFToolConfig, file: str, drop: bool, directory=None
) -> None:
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/transformations"
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
    )
    tmp = ""
    if file:
        # Only load the supplied filename.
        os.mkdir(f"{directory}/tmp")
        os.system(f"cp {directory}/{file} {directory}/tmp/")
        tmp = "tmp/"
    configs = parse_transformation_configs(f"{directory}/{tmp}")
    if len(tmp) > 0:
        os.system(f"rm -rf {directory}/tmp")
    cluster = ToolGlobals.environ("CDF_CLUSTER")
    transformations = [
        to_transformation(client, conf_path, configs[conf_path], cluster)
        for conf_path in configs
    ]
    transformations_ext_ids = [t.external_id for t in configs.values()]
    try:
        if drop:
            client.transformations.delete(external_id=transformations_ext_ids)
    except CogniteNotFoundError:
        pass
    try:
        existing_transformations_ext_ids = get_existing_transformation_ext_ids(
            client, transformations_ext_ids
        )
        new_transformation_ext_ids = get_new_transformation_ids(
            transformations_ext_ids, existing_transformations_ext_ids
        )
        _, updated_transformations, created_transformations = upsert_transformations(
            client,
            transformations,
            existing_transformations_ext_ids,
            new_transformation_ext_ids,
        )
    except Exception as e:
        print(f"Failed to upsert transformations for example {ToolGlobals.example}.")
        print(e)
        ToolGlobals.failed = True
        return
    print(
        f"Updated {len(updated_transformations)} transformations for example {ToolGlobals.example}."
    )
    print(
        f"Created {len(created_transformations)} transformations for example {ToolGlobals.example}."
    )


def load_readwrite_group(
    ToolGlobals: CDFToolConfig,
    capabilities: List[Dict[str, Any]],
    source_id="readwrite",
) -> None:
    client = ToolGlobals.verify_client(
        capabilities={"groupsAcl": ["LIST", "READ", "CREATE", "DELETE"]}
    )
    try:
        groups = client.iam.groups.list(all=True)
    except Exception as e:
        print(f"Failed to retrieve groups.")
        ToolGlobals.failed = True
        return
    old_group_id = None
    for group in groups:
        if group.source_id == source_id:
            old_group_id = group.id
            break
    try:
        group = client.iam.groups.create(
            Group(
                name=source_id,
                source_id=source_id,
                capabilities=capabilities,
            )
        )
    except Exception as e:
        print(f"Failed to create group {source_id}.")
        ToolGlobals.failed = True
        return
    if old_group_id:
        try:
            client.iam.groups.delete(id=old_group_id)
        except Exception as e:
            print(f"Failed to delete group {old_group_id}.")
            ToolGlobals.failed = True
