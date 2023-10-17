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
import re
import yaml
import pandas as pd
from typing import List, Dict, Any
from cognite.client.data_classes.time_series import TimeSeries, TimeSeriesProperty
from cognite.client.data_classes.iam import Group
from .utils import CDFToolConfig
from scripts.transformations_config import parse_transformation_configs
from scripts.transformations_api import (
    to_transformation,
    get_existing_transformation_ext_ids,
    get_new_transformation_ids,
    upsert_transformations,
)
from cognite.client.exceptions import CogniteNotFoundError


class TimeSeriesLoad:
    @staticmethod
    def load(props: list[dict], file: str = "unknown") -> [TimeSeries]:
        try:
            return [TimeSeries(**prop) for prop in props]
        except:
            raise ValueError(f"Failed to load timeseries from yaml files: {file}.")


def load_raw(
    ToolGlobals: CDFToolConfig,
    file: str,
    raw_db: str = "default",
    drop: bool = False,
    dry_run: bool = False,
    directory=None,
) -> None:
    """Load raw data from csv files into CDF Raw

    Args:
        file: name of file to load, if empty load all files
        drop: whether to drop existing data
    """
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(capabilities={"rawAcl": ["READ", "WRITE"]})

    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".csv" in f:
                    files.append(f)
    if len(files) == 0:
        return
    print(
        f"Uploading {len(files)} .csv files to RAW database using {raw_db} if not set in filename..."
    )
    for f in files:
        try:
            (_, db, _) = re.match(r"(\d+)\.(\w+)\.(\w+)\.csv", f).groups()
        except:
            db = raw_db
        with open(f"{directory}/{f}", "rt") as file:
            dataframe = pd.read_csv(file, dtype=str)
            dataframe = dataframe.fillna("")
            try:
                if not dry_run:
                    client.raw.tables.delete(db, f[:-4])
                    client.raw.rows.insert_dataframe(
                        db_name=db,
                        table_name=f[:-4],
                        dataframe=dataframe,
                        ensure_parent=True,
                    )
                    print("Deleted table: " + f[:-4])
                    print(f"Uploaded {f} to {db} RAW database.")
                else:
                    print("Would have deleted table: " + f[:-4])
                    print(f"Would have uploaded {f} to {db} RAW database.")
            except Exception as e:
                print(f"Failed to upload {f}")
                print(e)
                ToolGlobals.failed = True
                return


def load_files(
    ToolGlobals: CDFToolConfig,
    id_prefix: str = "example",
    file: str = None,
    drop: bool = False,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    try:
        client = ToolGlobals.verify_client(capabilities={"filesAcl": ["READ", "WRITE"]})
        files = []
        if file is not None and len(file) > 0:
            files.append(file)
        else:
            # Pick up all the files in the files folder.
            for _, _, filenames in os.walk(directory):
                for f in filenames:
                    files.append(f)
        if len(files) == 0:
            return
        print(f"Uploading {len(files)} files/documents to CDF...")
        for f in files:
            if not dry_run:
                client.files.upload(
                    path=f"{directory}/{f}",
                    data_set_id=ToolGlobals.data_set_id,
                    name=f,
                    external_id=id_prefix + "_" + f,
                    overwrite=drop,
                )
        if not dry_run:
            print(f"Uploaded successfully {len(files)} files/documents.")
        else:
            print(f"Would have uploaded {len(files)} files/documents.")
    except Exception as e:
        print(f"Failed to upload files")
        print(e)
        ToolGlobals.failed = True
        return


def load_timeseries(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool = False,
    dry_run: bool = False,
    directory: str = None,
) -> None:
    load_timeseries_metadata(
        ToolGlobals, file, drop, dry_run=dry_run, directory=directory
    )
    if directory is not None:
        directory = f"{directory}/datapoints"
    load_timeseries_datapoints(ToolGlobals, file, dry_run=dry_run, directory=directory)


def load_timeseries_metadata(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .json files in the data folder.
        for _, _, filenames in os.walk(directory):
            for f in filenames:
                if ".yaml" in f:
                    files.append(f)
    # Read timeseries metadata
    timeseries: list[TimeSeries] = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            timeseries.extend(
                TimeSeriesLoad.load(
                    yaml.safe_load(file.read()), file=f"{directory}/{f}"
                ),
            )
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
            if not dry_run:
                client.time_series.delete(external_id=drop_ts, ignore_unknown_ids=True)
                print(f"Deleted {len(drop_ts)} timeseries.")
            else:
                print(f"Would have deleted {len(drop_ts)} timeseries.")
    except Exception as e:
        print(f"Failed to delete {t.external_id}. It may not exist.")
    try:
        if not dry_run:
            client.time_series.create(timeseries)
        else:
            print(f"Would have created {len(timeseries)} timeseries.")
    except Exception as e:
        print(f"Failed to upload timeseries.")
        print(e)
        ToolGlobals.failed = True
        return
    print(f"Created {len(timeseries)} timeseries from {len(files)} files.")


def load_timeseries_datapoints(
    ToolGlobals: CDFToolConfig, file: str, dry_run: bool = False, directory=None
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
    client = ToolGlobals.verify_client(
        capabilities={"timeseriesAcl": ["READ", "WRITE"]}
    )
    files = []
    if file:
        # Only load the supplied filename.
        files.append(file)
    else:
        # Pick up all the .csv files in the data folder.
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
            if not dry_run:
                print(f"Uploading {f} as datapoints to CDF timeseries...")
                client.time_series.data.insert_dataframe(dataframe)
            else:
                print(f"Would have uploaded {f} as datapoints to CDF timeseries...")
        if not dry_run:
            print(
                f"Uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries."
            )
        else:
            print(
                f"Would have uploaded {len(files)} .csv file(s) as datapoints to CDF timeseries."
            )
    except Exception as e:
        print(f"Failed to upload datapoints.")
        print(e)
        ToolGlobals.failed = True
        return


def load_transformations(
    ToolGlobals: CDFToolConfig,
    file: str,
    drop: bool,
    dry_run: bool = False,
    directory=None,
) -> None:
    if directory is None:
        raise ValueError("directory must be specified")
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
            if not dry_run:
                client.transformations.delete(external_id=transformations_ext_ids)
            else:
                print(
                    f"Would have deleted {len(transformations_ext_ids)} transformations."
                )
    except CogniteNotFoundError:
        pass
    try:
        existing_transformations_ext_ids = get_existing_transformation_ext_ids(
            client, transformations_ext_ids
        )
        new_transformation_ext_ids = get_new_transformation_ids(
            transformations_ext_ids, existing_transformations_ext_ids
        )
        if not dry_run:
            (
                _,
                updated_transformations,
                created_transformations,
            ) = upsert_transformations(
                client,
                transformations,
                existing_transformations_ext_ids,
                new_transformation_ext_ids,
            )
        else:
            print(
                f"Would have updated and created {len(transformations)} transformations."
            )
    except Exception as e:
        print(f"Failed to upsert transformations.")
        print(e)
        ToolGlobals.failed = True
        return
    if not dry_run:
        print(f"Updated {len(updated_transformations)} transformations.")
        print(f"Created {len(created_transformations)} transformations.")


def load_readwrite_group(
    ToolGlobals: CDFToolConfig,
    capabilities: List[Dict[str, Any]],
    dry_run: bool = False,
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
        if not dry_run:
            group = client.iam.groups.create(
                Group(
                    name=source_id,
                    source_id=source_id,
                    capabilities=capabilities,
                )
            )
        else:
            print(f"Would have created group {source_id}.")
    except Exception as e:
        print(f"Failed to create group {source_id}.")
        ToolGlobals.failed = True
        return
    if old_group_id:
        try:
            if not dry_run:
                client.iam.groups.delete(id=old_group_id)
            else:
                print(f"Would have deleted group {old_group_id}.")
        except Exception as e:
            print(f"Failed to delete group {old_group_id}.")
            ToolGlobals.failed = True
