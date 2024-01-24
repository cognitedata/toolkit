from __future__ import annotations

import json
import os
import tempfile

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DataModelList,
    ViewList,
)
from cognite.client.utils.useful_types import SequenceNotStr

from .utils import CDFToolConfig


def dump_datamodels_all(
    ToolGlobals: CDFToolConfig,
    target_dir: str = "tmp",
    include_global: bool = False,
) -> None:
    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    try:
        print("  spaces...")
        spaces = client.data_modeling.spaces.list(limit=None, include_global=include_global)
    except Exception as e:
        print("  Failed to retrieve all spaces.")
        print(e)
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(limit=None, include_global=True)
    except Exception as e:
        print("Failed to retrieve all containers.")
        print(e)
        return None
    try:
        print("  views...")
        views = client.data_modeling.views.list(limit=None, space=None, include_global=include_global)
    except Exception as e:
        print("  Failed to retrieve all views.")
        print(e)
        return None
    try:
        print("  data models...")
        data_models: DataModelList = client.data_modeling.data_models.list(
            limit=-1, include_global=include_global, inline_views=True
        )
    except Exception as e:
        print("  Failed to retrieve all data models.")
        print(e)
        return None
    print("Writing...")
    for s in spaces:
        os.makedirs(f"{target_dir}/{s.space}")
    for d in data_models:
        with open(
            f"{target_dir}/{d.space}/{d.external_id}.model.json",
            "w",
        ) as file:
            json.dump(d.dump(camel_case=True), file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.space}/{v.external_id}.view.json",
            "w",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}//{c.space}/{c.external_id}.container.json",
            "w",
        ) as file:
            json.dump(c.dump(camel_case=True), file, indent=4)


def dump_datamodel(
    ToolGlobals: CDFToolConfig,
    space_name: str,
    model_name: str,
    version: str = "1",
    target_dir: str = "tmp",
) -> None:
    """Dump data model from CDF"""

    print("Verifying access rights...")
    client = ToolGlobals.verify_client(
        capabilities={
            "dataModelsAcl": ["READ", "WRITE"],
            "dataModelInstancesAcl": ["READ", "WRITE"],
        }
    )
    print(f"Loading data model ({model_name}) in space ({space_name})...")
    try:
        print("  space...")
        client.data_modeling.spaces.retrieve(space_name)
    except Exception as e:
        print(f"Failed to retrieve space {space_name}.")
        print(e)
    try:
        print("  containers...")
        containers = client.data_modeling.containers.list(space=space_name, limit=None, include_global=True)
    except Exception as e:
        print(f"Failed to retrieve containers for data model {model_name}.")
        print(e)
        return None
    try:
        print("  data model...")
        data_model = client.data_modeling.data_models.retrieve((space_name, model_name, version), inline_views=False)
        data_model = data_model.data[0].dump(camel_case=True)
    except Exception as e:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        print(e)
        return
    if len(data_model) == 0:
        print(f"Failed to retrieve data model {model_name} in space {space_name}.")
        return

    try:
        print("  views...")
        views = client.data_modeling.views.retrieve((space_name, model_name))
    except Exception as e:
        print(f"Failed to retrieve views from {model_name} in space {space_name}.")
        print(e)
        return
    if len(views.data) == 0:
        print(f"{model_name} in space {space_name} does not have any views in this space.")
        views = ViewList([])
    else:
        views = views.data[0].views
    print("Writing...")
    with open(
        f"{target_dir}/data_model.json",
        "w",
    ) as file:
        json.dump(data_model, file, indent=4)
    for v in views:
        with open(
            f"{target_dir}/{v.external_id}.view.json",
            "w",
        ) as file:
            json.dump(v.dump(camel_case=True), file, indent=4)
    for c in containers:
        with open(
            f"{target_dir}/{c.external_id}.container.json",
            "w",
        ) as file:
            json.dump(c.dump(camel_case=True), file, indent=4)


def dump_transformations(
    ToolGlobals: CDFToolConfig,
    external_ids: SequenceNotStr[str] | None = None,
    target_dir: str | None = None,
    ignore_unknown_ids: bool = True,
) -> None:
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
        print("Failed to retrieve transformations.")
        print(e)
        return None
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
            "w",
        ) as file:
            json.dump(t2, file, indent=2)
        with open(
            f"{target_dir}/{t2.get('external_id') or tempfile.TemporaryFile(dir=target_dir).name}.sql",
            "w",
        ) as file:
            for line in query.splitlines():
                file.write(line + "\n")
    print(f"Done writing {len(transformations)} transformations to {target_dir}.")
    return None
