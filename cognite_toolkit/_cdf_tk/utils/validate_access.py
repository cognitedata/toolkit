from collections.abc import Sequence
from typing import Literal

from cognite.client.data_classes.capabilities import (
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    FilesAcl,
    TimeSeriesAcl,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.utils import humanize_collection


class ValidateAccess:
    def __init__(self, client: ToolkitClient, default_operation: str) -> None:
        self.client = client
        self.default_operation = default_operation

    def data_model(
        self, action: Sequence[Literal["read", "write"]], space: str | None = None, operation: str | None = None
    ) -> list[str] | None:
        """Validate access to data models.

        Args:
            action (Sequence[Literal["read", "write"]]): The actions to validate access for.
            space (str | None): The space ID to check access for. If None, checks access for all spaces.
            operation (str | None): The operation being performed, used for error messages.

        Returns:
            list[str] | None: Returns a list of space IDs if access is limited to these spaces, or None if access is granted to all spaces.

        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected number of data model scopes.
            AuthorizationError: If the user does not have permission to perform the specified action on the given space.
        """
        operation = operation or self.default_operation
        model_scopes, actions_str = self._set_up_read_write(
            action, DataModelsAcl.Action.Read, DataModelsAcl.Action.Write, operation, "data models"
        )
        if len(model_scopes) != 1:
            raise ValueError(f"Unexpected number of data model scopes: {len(model_scopes)}. Expected 1 scope.")
        model_scope = model_scopes[0]
        if isinstance(model_scope, DataModelsAcl.Scope.All):
            return None
        elif isinstance(model_scope, DataModelsAcl.Scope.SpaceID):
            if space is not None and space not in model_scope.space_ids:
                raise AuthorizationError(
                    f"You have no permission to {actions_str} the {space!r} space. This is required to {operation}."
                )
            elif space is not None and space in model_scope.space_ids:
                return None
            else:
                return model_scope.space_ids
        else:
            raise ValueError(f"Unexpected data model scope type: {type(model_scope)}. Expected SpaceID or All.")

    def instances(
        self, action: Sequence[Literal["read", "write"]], space: str | None = None, operation: str | None = None
    ) -> list[str] | None:
        """Validate access to data model instances.

        Args:
            action (Sequence[Literal["read", "write"]]): The actions to validate access for.
            space (str | None): The space ID to check access for. If None, checks access for all spaces.
            operation (str | None): The operation being performed, used for error messages.

        Returns:
            list[str] | None: Returns a list of space IDs if access is limited to these spaces, or None if access is granted to all spaces.

        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected number of data model instance scopes.
            AuthorizationError: If the user does not have permission to perform the specified action on the given space.
        """
        operation = operation or self.default_operation
        instance_scopes, action_str = self._set_up_read_write(
            action, DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write, operation, "instances"
        )
        if len(instance_scopes) != 1:
            raise ValueError(
                f"Unexpected number of data model instance scopes: {len(instance_scopes)}. Expected 1 scope."
            )
        instance_scope = instance_scopes[0]
        if isinstance(instance_scope, DataModelInstancesAcl.Scope.SpaceID):
            if space is not None and space not in instance_scope.space_ids:
                raise AuthorizationError(
                    f"You have no permission to {action_str} instances in space {space!r}. This is required to {operation} instances."
                )
            elif space is not None and space in instance_scope.space_ids:
                return None
            else:
                return instance_scope.space_ids
        elif isinstance(instance_scope, DataModelInstancesAcl.Scope.All):
            return None
        else:
            raise ValueError(
                f"Unexpected data model instance scope type: {type(instance_scope)}. Expected SpaceID or All."
            )

    def timeseries(
        self, action: Sequence[Literal["read", "write"]], dataset_id: int | None = None, operation: str | None = None
    ) -> dict[str, list[str]] | None:
        """Validate access to time series.
        Args:
            action (Sequence[Literal["read", "write"]]): The actions to validate access for.
            dataset_id (int | None): The dataset ID to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error messages.
        Returns:
            dict[str, list[str]] | None: Returns a dictionary with keys 'dataset', 'asset root', and 'time series' if access is limited to these scopes, or None if access is granted to all time series.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected timeseries scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given dataset or time series.
        """
        operation = operation or self.default_operation
        timeseries_scopes, actions_str = self._set_up_read_write(
            action, TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write, operation, "time series"
        )

        if isinstance(timeseries_scopes[0], TimeSeriesAcl.Scope.All):
            return None
        if dataset_id is not None:
            for scope in timeseries_scopes:
                if isinstance(scope, TimeSeriesAcl.Scope.DataSet) and dataset_id in scope.ids:
                    return None
            raise AuthorizationError(
                f"You have no permission to {actions_str} time series in dataset {dataset_id}. This is required to {operation}."
            )
        output: dict[str, list[str]] = {}
        for scope in timeseries_scopes:
            if isinstance(scope, TimeSeriesAcl.Scope.DataSet):
                output["dataset"] = self.client.lookup.data_sets.external_id(scope.ids)
            elif isinstance(scope, TimeSeriesAcl.Scope.AssetRootID):
                output["asset root"] = self.client.lookup.assets.external_id(scope.root_ids)
            elif isinstance(scope, TimeSeriesAcl.Scope.ID):
                output["time series"] = self.client.lookup.time_series.external_id(scope.ids)
            else:
                raise ValueError(
                    f"Unexpected timeseries scope type: {type(scope)}. Expected DataSet, AssetRootID or ID."
                )

        return output

    def files(
        self, action: Sequence[Literal["read", "write"]], dataset_id: int | None = None, operation: str | None = None
    ) -> dict[str, list[str]] | None:
        """Validate access to files.

        Args:
            action (Sequence[Literal["read", "write"]]): The actions to validate access for
            dataset_id (int | None): The dataset ID to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error messages.
        Returns:
            dict[str, list[str]] | None: Returns a dictionary with the key 'dataset' if access is limited to a dataset scope, or None if access is granted to all files.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected file scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given
                dataset.
        """
        operation = operation or self.default_operation
        file_scopes, actions_str = self._set_up_read_write(
            action, FilesAcl.Action.Read, FilesAcl.Action.Write, operation, "files"
        )
        if isinstance(file_scopes[0], FilesAcl.Scope.All):
            return None
        if dataset_id is not None:
            for scope in file_scopes:
                if isinstance(scope, FilesAcl.Scope.DataSet) and dataset_id in scope.ids:
                    return None
            raise AuthorizationError(
                f"You have no permission to {actions_str} files in dataset {dataset_id}. This is required to {operation}."
            )
        output: dict[str, list[str]] = {}
        for scope in file_scopes:
            if isinstance(scope, FilesAcl.Scope.DataSet):
                output["dataset"] = self.client.lookup.data_sets.external_id(scope.ids)
            else:
                raise ValueError(f"Unexpected file scope type: {type(scope)}. Expected DataSet or All.")
        return output

    def _set_up_read_write(
        self,
        action: Sequence[Literal["read", "write"]],
        read: Capability.Action,
        write: Capability.Action,
        operation: str,
        name: str,
    ) -> tuple[list[Capability.Scope], str]:
        actions_str = humanize_collection(action, bind_word="and")
        actions = [{"read": read, "write": write}[a] for a in action]
        scopes = self.client.token.get_scope(actions)
        if scopes is None:
            raise AuthorizationError(
                f"You have no permission to {actions_str} {name}. This is required to {operation}."
            )
        return scopes, actions_str
