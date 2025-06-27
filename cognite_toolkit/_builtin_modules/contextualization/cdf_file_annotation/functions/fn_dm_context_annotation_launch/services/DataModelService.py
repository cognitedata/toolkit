import abc
from datetime import datetime, timezone, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeId,
    NodeList,
    NodeApply,
    NodeApplyResultList,
    instances,
    InstancesApplyResult,
)
from cognite.client.data_classes.filters import (
    Filter,
    Equals,
    In,
    Range,
    Exists,
)

from services.ConfigService import (
    Config,
    ViewPropertyConfig,
    build_filter_from_query,
    get_limit_from_query,
)
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import AnnotationStatus


class IDataModelService(abc.ABC):
    """
    Interface for interacting with data model instances in CDF
    """

    @abc.abstractmethod
    def get_files_for_annotation_reset(self) -> NodeList | None:
        pass

    @abc.abstractmethod
    def get_files_to_annotate(self) -> NodeList | None:
        pass

    @abc.abstractmethod
    def get_files_to_process(
        self,
    ) -> tuple[NodeList, dict[NodeId, Node]] | tuple[None, None]:
        pass

    @abc.abstractmethod
    def update_annotation_state(
        self,
        list_node_apply: list[NodeApply],
    ) -> NodeApplyResultList:
        pass

    @abc.abstractmethod
    def create_annotation_state(
        self,
        list_node_apply: list[NodeApply],
    ) -> NodeApplyResultList:
        pass

    @abc.abstractmethod
    def get_instances_entities(
        self, primary_scope_value: str, secondary_scope_value: str | None
    ) -> tuple[NodeList, NodeList]:
        pass


class GeneralDataModelService(IDataModelService):
    """
    Implementation used for real runs
    """

    def __init__(
        self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger
    ):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger

        self.annotation_state_view: ViewPropertyConfig = (
            config.data_model_views.annotation_state_view
        )
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.target_entities_view: ViewPropertyConfig = (
            config.data_model_views.target_entities_view
        )

        self.get_files_to_annotate_retrieve_limit: int | None = get_limit_from_query(
            config.prepare_function.get_files_to_annotate_query
        )
        self.get_files_to_process_retrieve_limit: int | None = get_limit_from_query(
            config.launch_function.data_model_service.get_files_to_process_query
        )

        self.filter_files_to_annotate: Filter = build_filter_from_query(
            config.prepare_function.get_files_to_annotate_query
        )
        self.filter_files_to_process: Filter = build_filter_from_query(
            config.launch_function.data_model_service.get_files_to_process_query
        )
        self.filter_target_entities: Filter = build_filter_from_query(
            config.launch_function.data_model_service.get_target_entities_query
        )
        self.filter_file_entities: Filter = build_filter_from_query(
            config.launch_function.data_model_service.get_file_entities_query
        )

    def get_files_for_annotation_reset(self) -> NodeList | None:
        """
        Query for files based on the getFilesForAnnotationReset config parameters
        NOTE: Not building the filter in the object instantiation because the filter will only ever be used once throughout all runs of prepare
              Furthermore, there is an implicit guarantee that a filter will be returned b/c launch checks if the query exists.
        """
        if not self.config.prepare_function.get_files_for_annotation_reset_query:
            return None

        filter_files_for_annotation_reset: Filter = build_filter_from_query(
            self.config.prepare_function.get_files_for_annotation_reset_query
        )
        result: NodeList | None = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.file_view.as_view_id(),
            space=self.file_view.instance_space,
            limit=-1,  # NOTE: this should always be kept at -1 so that all files defined in the query will get reset
            filter=filter_files_for_annotation_reset,
        )
        return result

    def get_files_to_annotate(self) -> NodeList | None:
        """
        Query for files that are marked "ToAnnotate" in tags and don't have 'AnnotataionInProcess' and 'Annotated' in tags.
        More specific details of the query come from the getFilesToAnnotate config parameter.
        """
        result: NodeList | None = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.file_view.as_view_id(),
            space=self.file_view.instance_space,
            limit=self.get_files_to_annotate_retrieve_limit,  # NOTE: the amount of instances that are returned may or may not matter depending on how the memory constraints of azure/aws functions
            filter=self.filter_files_to_annotate,
        )

        return result

    def get_files_to_process(
        self,
    ) -> tuple[NodeList, dict[NodeId, Node]] | tuple[None, None]:
        """
        Query for FileAnnotationStateInstances based on the getFilesToProcess config parameter.
        Extract the NodeIds of the file that is referenced in mpcAnnotationState.
        Retrieve the files with the NodeIds.
        """
        annotation_state_filter = self._get_annotation_state_filter()
        annotation_state_instances: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.annotation_state_view.as_view_id(),
            space=self.annotation_state_view.instance_space,
            limit=self.get_files_to_process_retrieve_limit,
            filter=annotation_state_filter,
        )

        if not annotation_state_instances:
            return None, None

        file_to_state_map: dict[NodeId, Node] = {}
        list_file_node_ids: list[NodeId] = []

        for node in annotation_state_instances:
            file_reference = node.properties.get(
                self.annotation_state_view.as_view_id()
            ).get("linkedFile")
            if (
                not self.file_view.instance_space
                or self.file_view.instance_space == file_reference["space"]
            ):
                file_node_id = NodeId(
                    space=file_reference["space"],
                    external_id=file_reference["externalId"],
                )

                file_to_state_map[file_node_id] = node
                list_file_node_ids.append(file_node_id)

        file_instances: NodeList = self.client.data_modeling.instances.retrieve_nodes(
            nodes=list_file_node_ids,
            sources=self.file_view.as_view_id(),
        )

        return file_instances, file_to_state_map

    def _get_annotation_state_filter(self) -> Filter:
        """
        filter = (getFilesToProcess filter || (annotationStatus == Processing && now() - lastUpdatedTime) > 1440 minutes)
        - getFilesToProcess filter comes from extraction pipeline
        - (annotationStatus == Processing && now() - lastUpdatedTime) > 1440 minutes -> hardcoded -> reprocesses any file that has
        NOTE: Implementation of a more complex query that can't be handled in config should come from an implementation of the interface.
        """
        filterA: Filter = self.filter_files_to_process

        annotation_status_property = self.annotation_state_view.as_property_ref(
            "annotationStatus"
        )
        annotation_last_updated_property = self.annotation_state_view.as_property_ref(
            "sourceUpdatedTime"
        )
        latest_permissible_time_utc = datetime.now(timezone.utc) - timedelta(
            minutes=1440
        )
        latest_permissible_time_utc = latest_permissible_time_utc.isoformat(
            timespec="milliseconds"
        )
        filterB = Equals(
            annotation_status_property, AnnotationStatus.PROCESSING
        ) & Range(annotation_last_updated_property, lt=latest_permissible_time_utc)

        filter = filterA | filterB
        return filter

    def update_annotation_state(
        self, list_node_apply: list[NodeApply]
    ) -> NodeApplyResultList:
        """
        Updates annotation state nodes from the node applies passed into the function
        """
        update_results: InstancesApplyResult = (
            self.client.data_modeling.instances.apply(
                nodes=list_node_apply,
                replace=False,  # ensures we don't delete other properties in the view
            )
        )
        return update_results.nodes

    def create_annotation_state(
        self, list_node_apply: list[NodeApply]
    ) -> NodeApplyResultList:
        """
        Creates annotation state nodes from the node applies passed into the function
        """
        update_results: InstancesApplyResult = (
            self.client.data_modeling.instances.apply(
                nodes=list_node_apply,
                auto_create_direct_relations=True,
                replace=True,  # ensures we reset the properties of the node
            )
        )
        return update_results.nodes

    def get_instances_entities(
        self, primary_scope_value: str, secondary_scope_value: str | None
    ) -> tuple[NodeList, NodeList]:
        """
        Return the entities that can be used in diagram detect
        1. grab assets that meet the filter requirement
        2. grab files that meet the filter requirement
        """
        target_filter: Filter = self._get_target_entities_filter(
            primary_scope_value, secondary_scope_value
        )
        file_filter: Filter = self._get_file_entities_filter(
            primary_scope_value, secondary_scope_value
        )

        target_entities: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.target_entities_view.as_view_id(),
            space=self.target_entities_view.instance_space,
            filter=target_filter,
            limit=-1,  # NOTE: this should always be kept at -1 so that all entities are retrieved
        )
        file_entities: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.file_view.as_view_id(),
            space=self.file_view.instance_space,
            filter=file_filter,
            limit=-1,  # NOTE: this should always be kept at -1 so that all entities are retrieved
        )
        return target_entities, file_entities

    def _get_target_entities_filter(
        self, primary_scope_value: str, secondary_scope_value: str | None
    ) -> Filter:
        """
        Create a filter that...
            - grabs assets in the primary_scope_value and secondary_scope_value provided with detectInDiagram in the tags property
            or
            - grabs assets in the primary_scope_value with ScopeWideDetect in the tags property (hard coded) -> provides an option to include entities outside of the secondary_scope_value
        """
        filterA: Filter = Equals(
            property=self.target_entities_view.as_property_ref(
                self.config.launch_function.primary_scope_property
            ),
            value=primary_scope_value,
        )
        filterB: Filter = self.filter_target_entities
        # NOTE: ScopeWideDetect is an optional string that allows annotating across scopes
        filterC: Filter = In(
            property=self.target_entities_view.as_property_ref("tags"),
            values=["ScopeWideDetect"],
        )
        if not primary_scope_value:
            target_filter = filterB | filterC
        elif secondary_scope_value:
            filterD: Filter = Equals(
                property=self.target_entities_view.as_property_ref(
                    self.config.launch_function.secondary_scope_property
                ),
                value=secondary_scope_value,
            )
            target_filter = (filterA & filterD & filterB) | (filterA & filterC)
        else:
            target_filter = (filterA & filterB) | (filterA & filterC)
        return target_filter

    def _get_file_entities_filter(
        self, primary_scope_value: str, secondary_scope_value: str | None
    ) -> Filter:
        """
        Create a filter that...
            - grabs assets in the primary_scope_value and secondary_scope_value provided with DetectInDiagram in the tags property
            or
            - grabs assets in the primary_scope_value with ScopeWideDetect in the tags property (hard coded) -> provides an option to include entities outside of the secondary_scope_value
        """
        filterA: Filter = Equals(
            property=self.file_view.as_property_ref(
                self.config.launch_function.primary_scope_property
            ),
            value=primary_scope_value,
        )
        filterB: Filter = self.filter_file_entities
        filterC: Filter = Exists(
            property=self.file_view.as_property_ref(
                self.config.launch_function.file_search_property
            ),
        )
        # NOTE: ScopeWideDetect is an optional string that allows annotating across scopes
        filterD: Filter = In(
            property=self.file_view.as_property_ref("tags"),
            values=["ScopeWideDetect"],
        )
        if not primary_scope_value:
            file_filter = (filterB & filterC) | (filterD)
        elif secondary_scope_value:
            filterE: Filter = Equals(
                property=self.file_view.as_property_ref(
                    self.config.launch_function.secondary_scope_property
                ),
                value=secondary_scope_value,
            )
            file_filter = (filterA & filterB & filterE & filterC) | (filterA & filterD)
        else:
            file_filter = (filterA & filterB & filterC) | (filterA & filterD)

        return file_filter
