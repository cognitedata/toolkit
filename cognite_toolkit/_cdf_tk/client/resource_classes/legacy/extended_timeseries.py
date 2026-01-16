import sys
from collections.abc import Sequence
from typing import Any, cast

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.time_series import TimeSeries, TimeSeriesList

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ExtendedTimeSeries(TimeSeries):
    """Extended TimeSeries with pending instance ID support.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The externally supplied ID for the time series.
        instance_id (NodeId | None): The Instance ID for the time series. (Only applicable for time series created in DMS)
        name (str | None): The display short name of the time series.
        is_string (bool | None): Whether the time series is string valued or not.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        unit (str | None): The physical unit of the time series.
        unit_external_id (str | None): The physical unit of the time series (reference to unit catalog). Only available for numeric time series.
        asset_id (int | None): Asset ID of equipment linked to this time series.
        is_step (bool | None): Whether the time series is a step series or not.
        description (str | None): Description of the time series.
        security_categories (Sequence[int] | None): The required security categories to access this time series.
        data_set_id (int | None): The dataSet ID for the item.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        pending_instance_id (NodeId | None): The pending instance ID for the time series. This is used in the migration of times series to CogniteTimeSeries
            to specify which instance this time series should be linked with by the TimeSeries syncer service.
        legacy_name (str | None): This field is not used by the API and will be removed October 2024.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        name: str | None = None,
        is_string: bool | None = None,
        metadata: dict[str, str] | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        asset_id: int | None = None,
        is_step: bool | None = None,
        description: str | None = None,
        security_categories: Sequence[int] | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        pending_instance_id: NodeId | None = None,
        legacy_name: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            instance_id=instance_id,
            name=name,
            is_string=is_string,
            metadata=metadata,
            unit=unit,
            unit_external_id=unit_external_id,
            asset_id=asset_id,
            is_step=is_step,
            description=description,
            security_categories=security_categories,
            data_set_id=data_set_id,
            legacy_name=legacy_name,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)
        self.pending_instance_id = pending_instance_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.pending_instance_id, dict):
            instance.pending_instance_id = NodeId.load(instance.pending_instance_id)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the object to a dictionary"""
        output = super().dump(camel_case=camel_case)
        if self.pending_instance_id is not None:
            output["pendingInstanceId" if camel_case else "pending_instance_id"] = self.pending_instance_id.dump(
                camel_case=camel_case, include_instance_type=False
            )
        return output


class ExtendedTimeSeriesList(TimeSeriesList):
    _RESOURCE = ExtendedTimeSeries  # type: ignore[assignment]
