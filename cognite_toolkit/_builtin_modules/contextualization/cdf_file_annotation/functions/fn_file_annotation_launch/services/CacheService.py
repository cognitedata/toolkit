import abc
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite, Row
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
)
from services.ConfigService import Config, ViewPropertyConfig
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import entity


class ICacheService(abc.ABC):
    """
    Manages a persistent cache of entities to pass into diagram detect (e.g., assets, files)
    stored in a CDF RAW table. This avoids repeatedly fetching the same data for files
    that share the same operational context.
    """

    @abc.abstractmethod
    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> list[dict]:
        pass

    @abc.abstractmethod
    def _update_cache(self) -> list[dict]:
        pass

    @abc.abstractmethod
    def _validate_cache(self) -> bool:
        pass


class GeneralCacheService(ICacheService):
    """
    Manages a persistent cache of entities to pass into diagram detect (e.g., assets, files)
    stored in a CDF RAW table. This avoids repeatedly fetching the same data for files
    that share the same operational context.
    """

    def __init__(
        self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger
    ):
        self.client = client
        self.config = config
        self.logger = logger

        self.db_name: str = config.launch_function.cache_service.raw_db
        self.tbl_name: str = config.launch_function.cache_service.raw_table_cache
        self.cache_time_limit: int = (
            config.launch_function.cache_service.cache_time_limit
        )  # in hours

        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.target_entities_view: ViewPropertyConfig = (
            config.data_model_views.target_entities_view
        )

    def get_entities(
        self,
        data_model_service: IDataModelService,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> list[dict]:
        """
        Returns file and asset entities for use in diagram detect job
        Ensures that the cache is up to date and valid
        """
        entities: list[dict] = []
        if secondary_scope_value:
            key = f"{primary_scope_value}_{secondary_scope_value}"
        else:
            key = f"{primary_scope_value}"

        cdf_raw = self.client.raw.rows
        row: Row | None = cdf_raw.retrieve(
            db_name=self.db_name, table_name=self.tbl_name, key=key
        )

        if row and row.columns:
            last_update_time_str = row.columns["LastUpdateTimeUtcIso"]
            if self._validate_cache(last_update_time_str) == False:
                self.logger.debug("Refreshing RAW entities cache")
                entities = self._update_cache(
                    data_model_service, key, primary_scope_value, secondary_scope_value
                )
            else:
                asset_entity: list[dict] = row.columns["AssetEntities"]
                file_entity: list[dict] = row.columns["FileEntities"]
                entities = asset_entity + file_entity
        else:
            entities = self._update_cache(
                data_model_service, key, primary_scope_value, secondary_scope_value
            )

        return entities

    def _update_cache(
        self,
        data_model_service: IDataModelService,
        key: str,
        primary_scope_value: str,
        secondary_scope_value: str | None,
    ) -> list[dict]:
        """
        Creates (or overwrites) the cache for a given group. It fetches all relevant
        contextualization entities for the files in the group from the data model
        and stores them in the cache table.
        """
        asset_instances: NodeList
        file_instances: NodeList
        asset_instances, file_instances = data_model_service.get_instances_entities(
            primary_scope_value, secondary_scope_value
        )

        asset_entities: list[dict] = []
        file_entities: list[dict] = []
        asset_entities, file_entities = self._convert_instances_to_entities(
            asset_instances, file_instances
        )

        current_time_seconds = datetime.now(timezone.utc).isoformat()
        new_row = RowWrite(
            key=key,
            columns={
                "AssetEntities": asset_entities,
                "FileEntities": file_entities,
                "LastUpdateTimeUtcIso": current_time_seconds,
            },
        )
        self.client.raw.rows.insert(
            db_name=self.db_name,
            table_name=self.tbl_name,
            row=new_row,
        )

        entities = asset_entities + file_entities
        return entities

    def _validate_cache(self, last_update_datetime_str: str) -> bool:
        """
        Checks if the retrieved cache is still valid by comparing its creation
        timestamp with the 'cacheTimeLimit' from the configuration.
        """
        last_update_datetime_utc = datetime.fromisoformat(last_update_datetime_str)
        current_datetime_utc = datetime.now(timezone.utc)
        time_difference: timedelta = current_datetime_utc - last_update_datetime_utc

        cache_validity_period = timedelta(hours=self.cache_time_limit)
        self.logger.debug(f"Cache time limit: {cache_validity_period}")
        self.logger.debug(f"Time difference: {time_difference}")

        if time_difference > cache_validity_period:
            return False

        return True

    def _convert_instances_to_entities(
        self, asset_instances: NodeList, file_instances: NodeList
    ) -> tuple[list[dict], list[dict]]:
        """
        Convert the asset and file nodes into an entity
        """
        target_entities_search_property: str = (
            self.config.launch_function.target_entities_search_property
        )
        target_entities: list[dict] = []
        for instance in asset_instances:
            instance_properties = instance.properties.get(
                self.target_entities_view.as_view_id()
            )
            if target_entities_search_property in instance_properties:
                asset_entity = entity(
                    external_id=instance.external_id,
                    name=instance_properties.get("name"),
                    space=instance.space,
                    search_property=instance_properties.get(
                        target_entities_search_property
                    ),
                    annotation_type_external_id=self.target_entities_view.annotation_type,
                )
                target_entities.append(asset_entity.to_dict())
            else:
                asset_entity = entity(
                    external_id=instance.external_id,
                    name=instance_properties.get("name"),
                    space=instance.space,
                    search_property=instance_properties.get("name"),
                    annotation_type_external_id=self.target_entities_view.annotation_type,
                )
                target_entities.append(asset_entity.to_dict())

        file_search_property: str = self.config.launch_function.file_search_property
        file_entities: list[dict] = []
        for instance in file_instances:
            instance_properties = instance.properties.get(self.file_view.as_view_id())
            file_entity = entity(
                external_id=instance.external_id,
                name=instance_properties.get("name"),
                space=instance.space,
                search_property=instance_properties.get(file_search_property),
                annotation_type_external_id=self.file_view.annotation_type,
            )
            file_entities.append(file_entity.to_dict())

        return target_entities, file_entities
