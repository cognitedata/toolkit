import time
import abc
from typing import cast
from datetime import datetime, timezone
from collections import defaultdict
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.contextualization import FileReference
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeList,
    NodeApply,
)

from services.ConfigService import Config, ViewPropertyConfig
from services.CacheService import ICacheService
from services.AnnotationService import IAnnotationService
from services.DataModelService import IDataModelService
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import (
    BatchOfPairedNodes,
    AnnotationStatus,
    AnnotationState,
    PerformanceTracker,
    FileProcessingBatch,
)


class AbstractLaunchService(abc.ABC):
    """
    Orchestrates the file annotation launch process. This service prepares files for annotation,
    manages batching and caching, and initiates diagram detection jobs.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
        cache_service: ICacheService,
        annotation_service: IAnnotationService,
    ):
        self.client = client
        self.config = config
        self.logger = logger
        self.tracker = tracker
        self.data_model_service = data_model_service
        self.cache_service = cache_service
        self.annotation_service = annotation_service

    @abc.abstractmethod
    def prepare(self) -> str | None:
        """
        Peronally think it's cleaner having this operate as a separate cognite function -> but due to mpc function constraints it wouldn't make sense for our project to go down this route (Jack)
        """
        pass

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralLaunchService(AbstractLaunchService):
    """
    Orchestrates the file annotation launch process. This service prepares files for annotation,
    manages batching and caching, and initiates diagram detection jobs.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        data_model_service: IDataModelService,
        cache_service: ICacheService,
        annotation_service: IAnnotationService,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            data_model_service,
            cache_service,
            annotation_service,
        )

        self.max_batch_size: int = config.launch_function.batch_size
        self.page_range: int = config.launch_function.annotation_service.page_range
        self.annotation_state_view: ViewPropertyConfig = (
            config.data_model_views.annotation_state_view
        )
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view

        self.in_memory_cache: list[dict] = []
        self._cached_primary_scope: str | None = None
        self._cached_secondary_scope: str | None = None

        self.primary_scope_property: str = (
            self.config.launch_function.primary_scope_property
        )
        self.secondary_scope_property: str | None = (
            self.config.launch_function.secondary_scope_property
        )

        self.reset_files: bool = False
        if self.config.prepare_function.get_files_for_annotation_reset_query:
            self.reset_files = True

    # NOTE: I believe this code should be encapsulated as a separate CDF function named prepFunction. Due to the amount of cdf functions we can spin up, we're coupling this within the launchFunction.
    def prepare(self):
        """
        Retrieves files marked "ToAnnotate" in the tags property and creates a 1-to-1 ratio of FileAnnotationState instances to files
        """
        self.logger.info(
            message=f"Starting Prepare Function",
            section="START",
        )
        try:
            if self.reset_files:
                file_nodes_to_reset: NodeList | None = (
                    self.data_model_service.get_files_for_annotation_reset()
                )
                if not file_nodes_to_reset:
                    self.logger.info(
                        "No files found with the getFilesForAnnotationReset query provided in the config file"
                    )
                else:
                    self.logger.info(f"Resetting {len(file_nodes_to_reset)} files")
                    reset_node_apply: list[NodeApply] = []
                    for file_node in file_nodes_to_reset:
                        file_node_apply: NodeApply = file_node.as_write()
                        tags_property: list[str] = cast(
                            list[str], file_node_apply.sources[0].properties["tags"]
                        )
                        if "AnnotationInProcess" in tags_property:
                            tags_property.remove("AnnotationInProcess")
                        if "Annotated" in tags_property:
                            tags_property.remove("Annotated")
                        if "AnnotationFailed" in tags_property:
                            tags_property.remove("AnnotationFailed")

                        reset_node_apply.append(file_node_apply)
                    update_results = self.data_model_service.update_annotation_state(
                        reset_node_apply
                    )
                    self.logger.info(
                        f"Removed the AnnotationInProcess/Annotated/AnnotationFailed tag of {len(update_results)} files"
                    )
                self.reset_files = False
        except Exception as e:
            self.logger.error(message=f"Ran into the following error:\n{str(e)}")
            return

        try:
            file_nodes: NodeList | None = (
                self.data_model_service.get_files_to_annotate()
            )
            if not file_nodes:
                self.logger.info(
                    message=f"No files found to prepare",
                    section="END",
                )
                return "Done"
            self.logger.info(f"Preparing {len(file_nodes)} files")
        except Exception as e:
            self.logger.error(
                message=f"Ran into the following error:\n{str(e)}", section="END"
            )
            return

        annotation_state_instances: list[NodeApply] = []
        file_apply_instances: list[NodeApply] = []
        for file_node in file_nodes:
            node_id = {"space": file_node.space, "externalId": file_node.external_id}
            annotation_instance = AnnotationState(
                annotationStatus=AnnotationStatus.NEW,
                linkedFile=node_id,
            )
            if not self.annotation_state_view.instance_space:
                msg = "Need an instance space in DataModelViews/AnnotationStateView config to store the annotation state"
                self.logger.error(msg)
                raise ValueError(msg)
            annotation_instance_space: str = self.annotation_state_view.instance_space

            annotation_node_apply: NodeApply = annotation_instance.to_node_apply(
                node_space=annotation_instance_space,
                annotation_state_view=self.annotation_state_view.as_view_id(),
            )
            annotation_state_instances.append(annotation_node_apply)

            file_node_apply: NodeApply = file_node.as_write()
            tags_property: list[str] = cast(
                list[str], file_node_apply.sources[0].properties["tags"]
            )
            if "AnnotationInProcess" not in tags_property:
                tags_property.append("AnnotationInProcess")
                file_apply_instances.append(file_node_apply)

        try:
            create_results = self.data_model_service.create_annotation_state(
                annotation_state_instances
            )
            self.logger.info(
                message=f"Created {len(create_results)} annotation state instances"
            )
            update_results = self.data_model_service.update_annotation_state(
                file_apply_instances
            )
            self.logger.info(
                message=f"Added 'AnnotationInProcess' to the tag property for {len(update_results)} files",
                section="END",
            )
        except Exception as e:
            self.logger.error(
                message=f"Ran into the following error:\n{str(e)}", section="END"
            )
            raise

        self.tracker.add_files(success=len(file_nodes))

    def run(self):
        """
        The main entry point for the launch service. It prepares the files and then
        processes them in organized, context-aware batches.
        """
        self.logger.info(
            message=f"Starting Launch Function",
            section="START",
        )
        try:
            file_nodes, file_to_state_map = (
                self.data_model_service.get_files_to_process()
            )
            if not file_nodes or not file_to_state_map:
                self.logger.info(message=f"No files found to launch")
                return "Done"
            self.logger.info(
                message=f"Launching {len(file_nodes)} files", section="END"
            )
        except Exception as e:
            self.logger.error(
                message=f"Ran into the following error: \n{str(e)}", section="END"
            )
            return

        processing_batches: list[FileProcessingBatch] = (
            self._organize_files_for_processing(file_nodes)
        )

        total_files_processed = 0
        for batch in processing_batches:
            primary_scope_value = batch.primary_scope_value
            secondary_scope_value = batch.secondary_scope_value
            msg = f"{self.primary_scope_property}: {primary_scope_value}"
            if secondary_scope_value:
                msg += f", {self.secondary_scope_property}: {secondary_scope_value}"
            self.logger.info(message=f"Processing {len(batch.files)} files in {msg}")
            self._ensure_cache_for_batch(primary_scope_value, secondary_scope_value)

            current_batch = BatchOfPairedNodes(file_to_state_map=file_to_state_map)
            for file_node in batch.files:
                file_reference: FileReference = current_batch.create_file_reference(
                    file_node_id=file_node.as_id(),
                    page_range=self.page_range,
                    annotation_state_view_id=self.annotation_state_view.as_view_id(),
                )
                current_batch.add_pair(file_node, file_reference)
                total_files_processed += 1
                if current_batch.size() == self.max_batch_size:
                    self.logger.info(
                        message=f"Processing batch - Max batch size ({self.max_batch_size}) reached"
                    )
                    self._process_batch(current_batch)
            if not current_batch.is_empty():
                self.logger.info(
                    message=f"Processing remaining {current_batch.size()} files in batch"
                )
                self._process_batch(current_batch)
            self.logger.info(message=f"Finished processing for {msg}", section="END")

        self.tracker.add_files(success=total_files_processed)
        return None

    def _organize_files_for_processing(
        self, list_files: NodeList
    ) -> list[FileProcessingBatch]:
        """
        Groups files based on the 'primary_scope_property' and 'secondary_scope_property'
        defined in the configuration. This strategy allows us to load a relevant entity cache
        once for a group of files that share the same operational context, significantly
        reducing redundant CDF queries.
        """
        organized_data: dict[str, dict[str, list[Node]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for file_node in list_files:
            node_props = file_node.properties[self.file_view.as_view_id()]
            primary_value = node_props.get(self.primary_scope_property)
            secondary_value = "__NONE__"
            if self.secondary_scope_property:
                secondary_value = node_props.get(self.secondary_scope_property)
            organized_data[primary_value][secondary_value].append(file_node)

        final_processing_batches: list[FileProcessingBatch] = []
        for primary_property in sorted(organized_data.keys()):
            groups = organized_data[primary_property]
            for secondary_property in sorted(groups.keys()):
                files_in_batch = groups[secondary_property]
                if secondary_property == "__NONE__":
                    actual_secondary_property = None
                else:
                    actual_secondary_property = secondary_property
                final_processing_batches.append(
                    FileProcessingBatch(
                        primary_scope_value=primary_property,
                        secondary_scope_value=actual_secondary_property,
                        files=files_in_batch,
                    )
                )
                self.logger.debug(
                    message=f"Created batch of {len(files_in_batch)} files for {self.primary_scope_property}: {primary_property}, {self.secondary_scope_property}: {secondary_property}",
                    section="END",
                )
        return final_processing_batches

    def _ensure_cache_for_batch(
        self, primary_scope_value: str, secondary_scope_value: str | None
    ):
        """
        Ensure self.in_memory_cache is populated for the given site and unit.
        Checks if there's a mismatch in site, unit, or if the in_memory_cache is empty
        """
        if (
            self._cached_primary_scope != primary_scope_value
            or self._cached_secondary_scope != secondary_scope_value
            or not self.in_memory_cache
        ):
            self.logger.info(f"Refreshing in memory cache")
            try:
                self.in_memory_cache = self.cache_service.get_entities(
                    self.data_model_service, primary_scope_value, secondary_scope_value
                )
                self._cached_primary_scope = primary_scope_value
                self._cached_secondary_scope = secondary_scope_value
            except Exception as e:
                self.logger.error(f"Error refreshing cache")
                raise

    def _process_batch(self, batch: BatchOfPairedNodes):
        """
        Processes a single batch of files. For each file, it starts a diagram
        detection job and then updates the corresponding 'AnnotationState' node
        with the job ID and a 'Processing' status.
        """
        if batch.is_empty():
            return

        self.logger.info(
            f"Running diagram detect on {batch.size()} files with {len(self.in_memory_cache)} entities"
        )

        try:
            job_id: int = self.annotation_service.run_diagram_detect(
                files=batch.file_references, entities=self.in_memory_cache
            )
            update_properties = {
                "annotationStatus": AnnotationStatus.PROCESSING,
                "sourceUpdatedTime": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "diagramDetectJobId": job_id,
            }
            batch.batch_states.update_node_properties(
                new_properties=update_properties,
                view_id=self.annotation_state_view.as_view_id(),
            )
            update_results = self.data_model_service.update_annotation_state(
                batch.batch_states.apply
            )
            self.logger.info(
                message=f" Updated the annotation state instances:\n- annotation status set to 'Processing'\n- job id set to {job_id}",
                section="END",
            )
        except CogniteAPIError as e:
            if e.code == 429:
                self.logger.debug(f"{str(e)}")
                self.logger.info(
                    "Reached the max amount of jobs that can be processed by the server at once.\nSleeping for 15 minutes",
                    "END",
                )
                time.sleep(900)
                return
            else:
                self.logger.error(f"Ran into the following error:\n{str(e)}")
                raise Exception(e)
        finally:
            batch.clear_pair()
