#!/usr/bin/env python

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

from .utils import CDFToolConfig
from scripts.transformations_config import parse_transformation_configs
from typing import Sequence
import json
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.data_classes import (
    Transformation,
    TransformationList,
    TransformationDestination,
)
from cognite.client.data_classes.transformations.common import DataModelInfo
import tempfile
import os


def run_transformations(ToolGlobals: CDFToolConfig, directory: str = None):
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/transformations"
    ToolGlobals.failed = False
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
    )
    configs = parse_transformation_configs(f"{directory}")
    transformations_ext_ids = [t.external_id for t in configs.values()]
    try:
        for t in transformations_ext_ids:
            client.transformations.run(transformation_external_id=t, wait=False)
        print(
            f"Started {len(transformations_ext_ids)} transformation jobs for example {ToolGlobals.example}."
        )
    except Exception as e:
        print(
            f"Failed to start transformation jobs for example {ToolGlobals.example}. They may not exist."
        )
        print(e)
        ToolGlobals.failed = True


def dump_transformations(
    ToolGlobals: CDFToolConfig,
    external_ids: Sequence[str] = None,
    target_dir: str = None,
    ignore_unknown_ids: bool = True,
):
    """Dump transformations from CDF"""

    print("Verifying access rights...")
    client: CogniteClient = ToolGlobals.verify_client(
        capabilities={
            "transformationsAcl": ["READ"],
        }
    )
    print(f"Loading {len(external_ids) if external_ids else 'all'} transformations...")
    try:
        if external_ids is None:
            transformations = client.transformations.list()
        else:
            transformations = client.transformations.retrieve_multiple(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
    except Exception as e:
        print(f"Failed to retrieve transformations.")
        print(e)
        return
    # Clean up and write
    print("Writing...")
    for t in transformations:
        t2 = t.dump()
        try:
            t2.pop("id")
            t2.pop("created_time")
            t2.pop("last_updated_time")
            t2.pop("owner")
            t2.pop("last_finished_job")
            t2.pop("source_session")
            t2.pop("destination_session")
        except KeyError:
            ...
        query = t2.pop("query")
        with open(
            f"{target_dir}/{t2.get('external_id') or tempfile.TemporaryFile(dir=target_dir).name}.json",
            "wt",
        ) as file:
            json.dump(t2, file, indent=2)
        with open(
            f"{target_dir}/{t2.get('external_id') or tempfile.TemporaryFile(dir=target_dir).name}.sql",
            "wt",
        ) as file:
            for l in query.splitlines():
                file.write(l + "\n")
    print(f"Done writing {len(transformations)} transformations to {target_dir}.")


def load_transformations_dump(
    ToolGlobals: CDFToolConfig,
    file: str = None,
    drop: bool = False,
    directory: str = None,
) -> None:
    """Load transformations from dump folder.

    This code only gives a partial support for transformations by loading the actual sql query and the
    necessary config. Schedules, authentication, etc is not supported.
    """
    if directory is None:
        directory = f"./examples/{ToolGlobals.example}/transformations/dump"
    client = ToolGlobals.verify_client(
        capabilities={"transformationsAcl": ["READ", "WRITE"]}
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
    transformations: TransformationList = []
    for f in files:
        with open(f"{directory}/{f}", "rt") as file:
            tf = json.load(file)
            if type(tf) == dict:
                tf = [tf]
            # This code basically runs through the propertoes of the Transformation class and
            # converts the nested dicts into the right objects.
            # Full serialization and deserialization of the entire transformations API is not supported.
            for t in tf:
                ta = Transformation()
                for k, v in t.items():
                    if k == "destination":
                        # The destination dict is converted into a TransformationDestination object.
                        ta.destination = TransformationDestination(v["type"])
                        v.pop("type")
                        for k2, v2 in v.items():
                            # The data_model dict is converted into a DataModelInfo object.
                            if k2 == "data_model":
                                ta.destination.data_model = DataModelInfo(
                                    space=v2.get("space"),
                                    version=v2.get("version"),
                                    external_id=v2.get("external_id"),
                                    destination_type=v2.get("destination_type"),
                                )
                            else:
                                # Here we just pick up the values for each attribute in the dict and sets
                                # the corresponding property on the object.
                                ta.destination.__setattr__(k2, v2)
                    else:
                        # Here we just pick up the values for each attribute in the dict and sets
                        # the corresponding property on the object.
                        ta.__setattr__(k, v)
                transformations.append(ta)
    ext_ids = [t.external_id for t in transformations]
    print(f"Found {len(transformations)} transformations in {directory}.")
    try:
        if drop:
            client.transformations.delete(external_id=ext_ids, ignore_unknown_ids=True)
            print(f"Deleted {len(ext_ids)} transformations.")
    except CogniteNotFoundError:
        pass
    for t in transformations:
        with open(f"{directory}/{t.external_id}.sql", "rt") as file:
            t.query = file.read()
            t.data_set_id = ToolGlobals.data_set_id
    try:
        client.transformations.create(transformations)
        print(f"Created {len(transformations)} transformation.")
    except Exception as e:
        print(f"Failed to create transformations.")
        print(e)
        ToolGlobals.failed = True
