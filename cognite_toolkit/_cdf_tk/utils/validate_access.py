from collections.abc import Sequence
from typing import Literal, TypeAlias, overload

from cognite.client.data_classes.capabilities import (
    AssetsAcl,
    Capability,
    DataModelInstancesAcl,
    DataModelsAcl,
    ExtractionPipelinesAcl,
    FilesAcl,
    TimeSeriesAcl,
    TransformationsAcl,
    WorkflowOrchestrationAcl,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection

Action: TypeAlias = Literal["read", "write"]


class ValidateAccess:
    def __init__(self, client: ToolkitClient, default_operation: str) -> None:
        self.client = client
        self.default_operation = default_operation

    def data_model(
        self, action: Sequence[Action], spaces: set[str] | None = None, operation: str | None = None
    ) -> list[str] | None:
        """Validate access to data models.

        Args:
            action (Sequence[Action]): The actions to validate access for.
            spaces (Set[str] | None): The space IDs to check access for. If None, checks access for all spaces.
            operation (str | None): The operation being performed, used for error messages.

        Returns:
            list[str] | None: Returns a list of space IDs if access is limited to these spaces, or None if access is granted to all spaces.

        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected number of data model scopes.
            AuthorizationError: If the user does not have permission to perform the specified action on the given space.
        """
        operation = operation or self.default_operation
        model_scopes = self._get_scopes(
            action, DataModelsAcl.Action.Read, DataModelsAcl.Action.Write, operation, "data models"
        )
        if len(model_scopes) != 1:
            raise ValueError(f"Unexpected number of data model scopes: {len(model_scopes)}. Expected 1 scope.")
        model_scope = model_scopes[0]
        if isinstance(model_scope, DataModelsAcl.Scope.All):
            return None
        elif isinstance(model_scope, DataModelsAcl.Scope.SpaceID):
            if spaces is None:
                return model_scope.space_ids
            if missing := spaces - set(model_scope.space_ids):
                raise AuthorizationError(
                    f"You have no permission to {humanize_collection(action)} the {humanize_collection(missing)!r} "
                    f"space(s). This is required to {operation}."
                )
            return None
        else:
            raise ValueError(f"Unexpected data model scope type: {type(model_scope)}. Expected SpaceID or All.")

    def instances(
        self, action: Sequence[Action], spaces: set[str] | None = None, operation: str | None = None
    ) -> list[str] | None:
        """Validate access to data model instances.

        Args:
            action (Sequence[Action]): The actions to validate access for.
            spaces (Set[str] | None): The space IDs to check access for. If None, checks access for all spaces.
            operation (str | None): The operation being performed, used for error messages.

        Returns:
            list[str] | None: Returns a list of space IDs if access is limited to these spaces, or None if access is granted to all spaces.

        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected number of data model instance scopes.
            AuthorizationError: If the user does not have permission to perform the specified action on the given space.
        """
        operation = operation or self.default_operation
        instance_scopes = self._get_scopes(
            action, DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write, operation, "instances"
        )
        if len(instance_scopes) != 1:
            raise ValueError(
                f"Unexpected number of data model instance scopes: {len(instance_scopes)}. Expected 1 scope."
            )
        instance_scope = instance_scopes[0]
        if isinstance(instance_scope, DataModelInstancesAcl.Scope.SpaceID):
            if spaces is None:
                return instance_scope.space_ids
            if missing := spaces - set(instance_scope.space_ids):
                raise AuthorizationError(
                    f"You have no permission to {humanize_collection(action)} instances in the "
                    f"{humanize_collection(missing)!r} space(s). This is required to {operation} instances."
                )
            return None
        elif isinstance(instance_scope, DataModelInstancesAcl.Scope.All):
            return None
        else:
            raise ValueError(
                f"Unexpected data model instance scope type: {type(instance_scope)}. Expected SpaceID or All."
            )

    def dataset_data(
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None = None,
        operation: str | None = None,
        missing: Literal["raise", "warn"] = "raise",
    ) -> list[int] | None:
        """Validate access to dataset data.

        Dataset data resources are:
        - Assets
        - Events
        - Time series
        - Files
        - Sequences
        - Relationships
        - Labels
        - 3D models

        Args:
            action (Sequence[Action]): The actions to validate access for
            dataset_ids (Set[int] | None): The dataset IDs to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error and warning messages.
            missing (Literal["raise", "warn"]): Whether to raise an error or warn when access is missing for specified datasets.

        Returns:
            list[int] | None: Returns a list of dataset IDs if access is limited to these datasets, or None if access is granted to all datasets.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected dataset data scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given dataset.
        """
        raise NotImplementedError()

    @overload
    def dataset_configurations(
        self,
        action: Sequence[Action],
        dataset_ids: set[int],
        operation: str | None = None,
    ) -> None: ...

    @overload
    def dataset_configurations(
        self,
        action: Sequence[Action],
        dataset_ids: None = None,
        operation: str | None = None,
    ) -> dict[Literal["transformations", "workflows", "extraction pipelines"], list[int]]: ...

    def dataset_configurations(
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None = None,
        operation: str | None = None,
        missing_access: Literal["raise", "warn"] = "raise",
    ) -> dict[Literal["transformations", "workflows", "extraction pipelines"], list[int]] | None:
        """Validate access configuration resources.

        Configuration resources are:
        - Transformations
        - Workflows
        - Extraction pipelines

        Args:
            action (Sequence[Action]): The actions to validate access for
            dataset_ids (Set[int] | None): The dataset IDs to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error and warning messages.
            missing_access (Literal["raise", "warn"]): Whether to raise an error or warn when access is missing for specified datasets.

        Returns:
            dict[Literal["transformations", "workflows", "extraction pipelines"], list[int] | None]:
                If dataset_ids is None, returns a dictionary with keys as configuration resource names and values as lists of dataset IDs the user has access to.
                If dataset_ids is provided, returns None if the user has access to all specified datasets for all configuration resources.

        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected dataset configuration scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given dataset.
        """
        acls = [
            ("transformations", TransformationsAcl.Action.Read, TransformationsAcl.Action.Write),
            ("workflows", WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write),
            ("extraction pipelines", ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write),
        ]
        # MyPy does not understand that with the acl above, we get the correct return value.
        return self._dataset_access_check(  # type: ignore[return-value]
            action,
            dataset_ids=dataset_ids,
            operation=operation,
            acls=acls,
            missing_access=missing_access,
        )

    def _dataset_access_check(
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None,
        operation: str | None,
        missing_access: Literal["raise", "warn"],
        acls: Sequence[tuple[str, Capability.Action, Capability.Action]],
    ) -> dict[str, list[int]] | None:
        need_access_to = set(dataset_ids) if dataset_ids is not None else None
        no_access: list[str] = []
        output: dict[str, list[int]] = {}
        for name, read_action, write_action in acls:
            actions = [{"read": read_action, "write": write_action}[a] for a in action]
            scopes = self.client.token.get_scope(actions)
            if scopes is None:
                no_access.append(name)
                continue
            # First check for 'all' scope
            for scope in scopes:
                if isinstance(
                    scope,
                    TransformationsAcl.Scope.All
                    | WorkflowOrchestrationAcl.Scope.All
                    | ExtractionPipelinesAcl.Scope.All,
                ):
                    break
            else:
                # No 'all' scope found, check dataset scopes
                for scope in scopes:
                    if isinstance(
                        scope,
                        TransformationsAcl.Scope.DataSet
                        | WorkflowOrchestrationAcl.Scope.DataSet
                        | ExtractionPipelinesAcl.Scope.DataSet,
                    ):
                        if need_access_to is None:
                            output[name] = scope.ids
                            break
                        missing_data_set = need_access_to - set(scope.ids)
                        if missing_data_set:
                            no_access.append(name)
                            break
        operation = operation or self.default_operation
        if no_access:
            message = f"You have no permission to {humanize_collection(action)} {humanize_collection(no_access)}."
            if missing_access == "raise":
                raise AuthorizationError(f"{message} This is required to {operation}.")
            else:
                HighSeverityWarning(f"{message}. You will have limited functionality to {operation}.").print_warning()
        elif dataset_ids is not None:
            return None
        return output

    def timeseries(
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None = None,
        operation: str | None = None,
    ) -> dict[str, list[str]] | None:
        """Validate access to time series.
        Args:
            action (Sequence[Action]): The actions to validate access for.
            dataset_ids (Set[int] | None): The dataset IDs to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error messages.
        Returns:
            dict[str, list[str]] | None: Returns a dictionary with keys 'dataset', 'asset root', and 'time series' if access is limited to these scopes, or None if access is granted to all time series.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected timeseries scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given dataset or time series.
        """
        operation = operation or self.default_operation
        timeseries_scopes = self._get_scopes(
            action, TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write, operation, "time series"
        )

        if isinstance(timeseries_scopes[0], TimeSeriesAcl.Scope.All):
            return None
        if dataset_ids is not None:
            missing = set(dataset_ids)
            for scope in timeseries_scopes:
                if isinstance(scope, TimeSeriesAcl.Scope.DataSet):
                    missing = missing - set(scope.ids)
                    if not missing:
                        return None
            raise AuthorizationError(
                f"You have no permission to {humanize_collection(action)} time series in dataset {humanize_collection(missing)}. This is required to {operation}."
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
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None = None,
        operation: str | None = None,
    ) -> dict[str, list[str]] | None:
        """Validate access to files.

        Args:
            action (Sequence[Action]): The actions to validate access for
            dataset_ids (Set[int] | None): The dataset IDs to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error messages.
        Returns:
            dict[str, list[str]] | None: Returns a dictionary with the key 'dataset' if access is limited to a dataset scope, or None if access is granted to all files.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected file scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given
                dataset.
        """
        operation = operation or self.default_operation
        file_scopes = self._get_scopes(action, FilesAcl.Action.Read, FilesAcl.Action.Write, operation, "files")
        if isinstance(file_scopes[0], FilesAcl.Scope.All):
            return None
        if dataset_ids is not None:
            missing = set(dataset_ids)
            for scope in file_scopes:
                if isinstance(scope, FilesAcl.Scope.DataSet):
                    missing = missing - set(scope.ids)
                    if not missing:
                        return None
            raise AuthorizationError(
                f"You have no permission to {humanize_collection(action)} files in dataset "
                f"{humanize_collection(missing)}. This is required to {operation}."
            )
        output: dict[str, list[str]] = {}
        for scope in file_scopes:
            if isinstance(scope, FilesAcl.Scope.DataSet):
                output["dataset"] = self.client.lookup.data_sets.external_id(scope.ids)
            else:
                raise ValueError(f"Unexpected file scope type: {type(scope)}. Expected DataSet or All.")
        return output

    def assets(
        self,
        action: Sequence[Action],
        dataset_ids: set[int] | None = None,
        operation: str | None = None,
    ) -> dict[str, list[str]] | None:
        """Validate access to assets.
        Args:
            action (Sequence[Action]): The actions to validate access for
            dataset_ids (Set[int] | None): The dataset IDs to check access for. If None, checks access for all datasets.
            operation (str | None): The operation being performed, used for error messages.
        Returns:
            dict[str, list[str]] | None: Returns a dictionary with the key 'dataset'
                if access is limited to a dataset scope, or None if access is granted to all assets.
        Raises:
            ValueError: If the client.token.get_scope() returns an unexpected asset scope type.
            AuthorizationError: If the user does not have permission to perform the specified action on the given
                dataset.
        """
        operation = operation or self.default_operation
        asset_scopes = self._get_scopes(action, AssetsAcl.Action.Read, AssetsAcl.Action.Write, operation, "assets")
        if isinstance(asset_scopes[0], AssetsAcl.Scope.All):
            return None
        if dataset_ids is not None:
            missing = set(dataset_ids)
            for scope in asset_scopes:
                if isinstance(scope, AssetsAcl.Scope.DataSet):
                    missing = missing - set(scope.ids)
                    if not missing:
                        return None
            raise AuthorizationError(
                f"You have no permission to {humanize_collection(action)} assets in dataset(s) "
                f"{humanize_collection(missing)}. This is required to {operation}."
            )
        output: dict[str, list[str]] = {}
        for scope in asset_scopes:
            if isinstance(scope, AssetsAcl.Scope.DataSet):
                output["dataset"] = self.client.lookup.data_sets.external_id(scope.ids)
            else:
                raise ValueError(f"Unexpected asset scope type: {type(scope)}. Expected DataSet or All.")
        return output

    def _get_scopes(
        self,
        action: Sequence[Action],
        read: Capability.Action,
        write: Capability.Action,
        operation: str,
        name: str,
    ) -> list[Capability.Scope]:
        """Helper method to get scopes for the given action.

        Args:
            action (Sequence[Action]): The actions to validate access for.
            read (Capability.Action): The read action.
            write (Capability.Action): The write action.
            operation (str): The operation being performed, used for error messages.
            name (str): The name of the resource being accessed, used for error messages.

        Returns:
            list[Capability.Scope]: The scopes for the given action.
        """
        actions = [{"read": read, "write": write}[a] for a in action]
        scopes = self.client.token.get_scope(actions)
        if scopes is None:
            raise AuthorizationError(
                f"You have no permission to {humanize_collection(action, bind_word='and')} {name}. "
                f"This is required to {operation}."
            )
        return scopes
