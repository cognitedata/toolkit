import os
import sys
from typing import Dict, List, Optional, Iterator, Tuple, Union, TypeVar

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationDestination,
    TransformationNotification,
    TransformationSchedule,
    TransformationUpdate,
)
from cognite.client.data_classes.transformations.common import (
    DataModelInfo,
    Edges,
    EdgeType,
    Instances,
    Nodes,
    SequenceRows,
    ViewInfo,
)
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteDuplicatedError,
    CogniteNotFoundError,
)

from .transformation_types import (
    ActionType,
    AuthConfig,
    DestinationConfig,
    DestinationConfigType,
    InstanceEdgesDestinationConfig,
    InstanceNodesDestinationConfig,
    InstancesDestinationConfig,
    QueryConfig,
    RawDestinationAlternativeConfig,
    RawDestinationConfig,
    ReadWriteAuthentication,
    ScheduleConfig,
    SequenceRowsDestinationConfig,
    TransformationConfig,
)


TupleResult = List[Tuple[str, str]]
StandardResult = List[str]


T = TypeVar("T")


def chunk_items(items: List[T], n: int = 5) -> Iterator[List[T]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def to_transformation(
    client: CogniteClient,
    conf_path: str,
    config: TransformationConfig,
    cluster: str = "europe-west1-1",
) -> Transformation:
    return Transformation(
        name=config.name,
        external_id=config.external_id,
        destination=to_destination(config.destination),
        conflict_mode=to_action(config.action),
        is_public=config.shared,
        ignore_null_fields=config.ignore_null_fields,
        query=to_query(conf_path, config.query),
        source_oidc_credentials=to_read_oidc(config.authentication, cluster),
        destination_oidc_credentials=to_write_oidc(config.authentication, cluster),
        data_set_id=to_data_set_id(
            client, config.data_set_id, config.data_set_external_id
        ),
        tags=config.tags,
    )


def to_data_set_id(
    client: CogniteClient,
    data_set_id: Optional[int],
    data_set_external_id: Optional[str],
) -> Optional[int]:
    err = ""
    if data_set_external_id:
        try:
            data_set = client.data_sets.retrieve(external_id=data_set_external_id)
        except CogniteAPIError as e:
            err = f" ({e})"
            data_set = None
        if data_set:
            return data_set.id
        else:
            sys.exit(
                f"Invalid data set external id, please verify if it exists or you have the required capability: {data_set_external_id}{err}"
            )
    return data_set_id


def to_action(action: ActionType) -> str:
    return "abort" if action == ActionType.create else action.value


def to_destination(destination: DestinationConfigType) -> TransformationDestination:
    if isinstance(destination, DestinationConfig):
        return TransformationDestination(destination.type.value)
    elif isinstance(destination, RawDestinationConfig):
        return TransformationDestination.raw(
            destination.raw_database, destination.raw_table
        )
    elif isinstance(destination, RawDestinationAlternativeConfig):
        return TransformationDestination.raw(destination.database, destination.table)
    elif isinstance(destination, SequenceRowsDestinationConfig):
        return SequenceRows(destination.external_id)

    elif isinstance(destination, InstanceNodesDestinationConfig):
        view = None
        if destination.view:
            view = ViewInfo(
                destination.view.space,
                destination.view.external_id,
                destination.view.version,
            )
        return Nodes(view, destination.instance_space)

    elif isinstance(destination, InstanceEdgesDestinationConfig):
        view = None
        if destination.view:
            view = ViewInfo(
                destination.view.space,
                destination.view.external_id,
                destination.view.version,
            )
        edge_type = None
        if destination.edge_type:
            edge_type = EdgeType(
                destination.edge_type.space, destination.edge_type.external_id
            )
        return Edges(view, destination.instance_space, edge_type)

    elif isinstance(destination, InstancesDestinationConfig):
        data_model = None
        if destination.data_model:
            data_model = DataModelInfo(
                destination.data_model.space,
                destination.data_model.external_id,
                destination.data_model.version,
                destination.data_model.destination_type,
                destination.data_model.destination_relationship_from_type,
            )
        return Instances(data_model, destination.instance_space)
    else:
        return TransformationDestination(destination.value)


def to_query(conf_path: str, query: Union[str, QueryConfig]) -> str:
    try:
        dir_path = os.path.dirname(conf_path)
        if isinstance(query, QueryConfig):
            sql_path = os.path.join(dir_path, query.file)
            with open(sql_path, "r") as f:
                return f.read()
        return query
    except:
        sys.exit("Please provide a valid path for sql file.")


def stringify_scopes(scopes: Optional[List[str]]) -> Optional[str]:
    if scopes:
        return " ".join(scopes)
    return None


def get_default_scopes(scopes: Optional[str], cluster: str) -> str:
    return scopes if scopes else f"https://{cluster}.cognitedata.com/.default"


def is_oidc_defined(auth_config: AuthConfig) -> bool:
    return (
        auth_config.client_id
        and auth_config.client_secret
        and auth_config.token_url
        and auth_config.cdf_project_name
    )


def get_oidc(auth_config: AuthConfig, cluster: str) -> Optional[OidcCredentials]:
    stringified_scopes = stringify_scopes(auth_config.scopes)
    scopes = (
        stringified_scopes
        if auth_config.audience
        else get_default_scopes(stringified_scopes, cluster)
    )
    return (
        OidcCredentials(
            client_id=auth_config.client_id,
            client_secret=auth_config.client_secret,
            scopes=scopes,
            token_uri=auth_config.token_url,
            cdf_project_name=auth_config.cdf_project_name,
            audience=auth_config.audience,
        )
        if is_oidc_defined(auth_config)
        else None
    )


def to_read_oidc(
    authentication: Union[AuthConfig, ReadWriteAuthentication], cluster: str
) -> Optional[OidcCredentials]:
    return (
        get_oidc(authentication, cluster)
        if isinstance(authentication, AuthConfig)
        else get_oidc(authentication.read, cluster)
    )


def to_write_oidc(
    authentication: Union[AuthConfig, ReadWriteAuthentication], cluster: str
) -> Optional[OidcCredentials]:
    return (
        get_oidc(authentication, cluster)
        if isinstance(authentication, AuthConfig)
        else get_oidc(authentication.write, cluster)
    )


def get_existing_transformation_ext_ids(
    client: CogniteClient, all_ext_ids: List[str]
) -> List[str]:
    return [
        t.external_id
        for t in client.transformations.retrieve_multiple(
            external_ids=all_ext_ids, ignore_unknown_ids=True
        )
    ]


def get_new_transformation_ids(
    all_ext_ids: List[str], existig_ext_ids: List[str]
) -> List[str]:
    return list(set(all_ext_ids) - set(existig_ext_ids))


def upsert_transformations(
    client: CogniteClient,
    transformations: List[Transformation],
    existing_ext_ids: List[str],
    new_ext_ids: List[str],
) -> Tuple[StandardResult, StandardResult, StandardResult]:
    try:
        items_to_update = [
            tr for tr in transformations if tr.external_id in existing_ext_ids
        ]
        items_to_create = [
            tr for tr in transformations if tr.external_id in new_ext_ids
        ]

        for u in chunk_items(items_to_update):
            client.transformations.update(u)
            # Partial update for data set id to be able to clear data set id field when requested.
            dataset_update = [
                # Clear data set id if it is not provided, else set data set id with a new value
                TransformationUpdate(external_id=du.external_id).data_set_id.set(
                    du.data_set_id
                )
                for du in u
            ]
            client.transformations.update(dataset_update)

        for c in chunk_items(items_to_create):
            client.transformations.create(c)

        return (
            [],
            [t.external_id for t in items_to_update],
            [t.external_id for t in items_to_create],
        )
    except (CogniteDuplicatedError, CogniteNotFoundError, CogniteAPIError) as e:
        raise e
    return [], [], []
