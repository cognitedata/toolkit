import time
import abc
from typing import cast
from datetime import datetime, timezone
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeId,
    NodeApply,
    NodeApplyList,
    NodeOrEdgeData,
)

from services.ConfigService import Config, ViewPropertyConfig
from services.LoggerService import CogniteFunctionLogger
from services.RetrieveService import IRetrieveService
from services.ApplyService import IApplyService
from services.ReportService import IReportService
from utils.DataStructures import (
    BatchOfNodes,
    PerformanceTracker,
    AnnotationStatus,
)


class AbstractFinalizeService(abc.ABC):
    """
    Orchestrates the file annotation finalize process.
    This service retrieves the results of the diagram detect jobs from the launch function and then applies annotations to the file.
    Additionally, it captures the file and asset annotations into separate RAW tables.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        retrieve_service: IRetrieveService,
        apply_service: IApplyService,
        report_service: IReportService,
    ):
        self.client: CogniteClient = client
        self.config: Config = config
        self.logger: CogniteFunctionLogger = logger
        self.tracker: PerformanceTracker = tracker
        self.retrieve_service: IRetrieveService = retrieve_service
        self.apply_service: IApplyService = apply_service
        self.report_service: IReportService = report_service

    @abc.abstractmethod
    def run(self) -> str | None:
        pass


class GeneralFinalizeService(AbstractFinalizeService):
    """
    Orchestrates the file annotation finalize process.
    This service retrieves the results of the diagram detect jobs from the launch function and then applies annotations to the file.
    Additionally, it captures the file and asset annotations into separate RAW tables.
    """

    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        tracker: PerformanceTracker,
        retrieve_service: IRetrieveService,
        apply_service: IApplyService,
        report_service: IReportService,
    ):
        super().__init__(
            client,
            config,
            logger,
            tracker,
            retrieve_service,
            apply_service,
            report_service,
        )

        self.annotation_state_view: ViewPropertyConfig = (
            config.data_model_views.annotation_state_view
        )
        self.file_view: ViewPropertyConfig = config.data_model_views.file_view
        self.page_range: int = config.launch_function.annotation_service.page_range
        self.max_retries: int = config.finalize_function.max_retry_attempts
        self.clean_old_annotations: bool = (
            config.finalize_function.clean_old_annotations
        )

    def run(self):
        """
        Retrieves the result of a diagram detect job and then pushes the annotation to mpcFile.
        Specifically,
        1. Get a unique jobId and all instances of mpcAnnotationState that share that jobId
            2. If an error occurs
                - Retrieve another job
            3. If no error occurs
                - Continue
        4. Check the status of the job
            5. If the job is complete
                - Iterate through all items in the diagram detect job results push the annotation to mpcFile
                    6. If a file does have annotations
                        - Push the annotations to the file
                        - Update status of FileAnnotationState to "Annotated"
                        - Add annotations to the annotations report
                    7. If a file doesn't have any annotations or an error occurs
                        - Update status of mpcAnnotationState to "Retry" or "Fail"
            8. If the job isn't complete
                - Update status of FileAnnotationState to "Processing"
                - End the run
        """
        self.logger.info(
            message="Starting Finalize Function",
            section="START",
        )
        try:
            job_id, file_to_state_map = self.retrieve_service.get_job_id()
            if not job_id or not file_to_state_map:
                self.logger.info(message="No diagram detect jobs found", section="END")
                return "Done"
            else:
                self.logger.info(
                    message=f"Retrieved job id ({job_id}) and claimed {len(file_to_state_map.values())} files"
                )
        except CogniteAPIError as e:
            if e.code == 400:
                self.logger.info(
                    message=f"Retrieved job id that has already been claimed. Grabbing another job.",
                    section="END",
                )
            else:
                self.logger.error(
                    message=f"Ran into the following error:\n\t{e}", section="END"
                )
            return

        try:
            job_results: dict | None = (
                self.retrieve_service.get_diagram_detect_job_result(job_id)
            )
        except Exception as e:
            self.logger.info(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files - job id ({job_id}) is a bad gateway",
                section="END",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.RETRY,
                failed=True,
            )

        if job_results is None:
            self.logger.info(
                message=f"Unfinalizing {len(file_to_state_map.keys())} files - job id ({job_id}) is not complete yet",
                section="END",
            )
            self._update_batch_state(
                batch=BatchOfNodes(nodes=list(file_to_state_map.values())),
                status=AnnotationStatus.PROCESSING,
            )
            self.logger.info(message="Sleeping for 30 seconds")
            time.sleep(30)
            return

        self.logger.info(
            message=f"Applying annotations to {len(job_results['items'])} files",
            section="END",
        )
        count_retry = 0
        count_failed = 0
        annotation_state_node_applies: list[NodeApply] = []
        failed_file_ids: list[NodeId] = []

        for diagram_detect_item in job_results["items"]:
            file_id: NodeId = NodeId.load(diagram_detect_item["fileInstanceId"])
            annotation_state_node: Node = file_to_state_map[file_id]

            current_attempt_count: int = cast(
                int,
                annotation_state_node.properties[
                    self.annotation_state_view.as_view_id()
                ]["attemptCount"],
            )
            next_attempt_count = current_attempt_count + 1
            job_node_to_update: NodeApply | None = None
            if (
                diagram_detect_item.get("annotations")
                and len(diagram_detect_item["annotations"]) > 0
            ):
                try:
                    self.logger.info(
                        f"Applying annotations to file NodeId - {str(file_id)}"
                    )
                    if self.clean_old_annotations:
                        self.logger.info("Deleting old annotations")
                        doc_annotations_delete, tag_annotations_delete = (
                            self.apply_service.delete_annotations_for_file(
                                file_node=file_id
                            )
                        )
                        self.logger.info(
                            f"\t- deleted {len(doc_annotations_delete)} document annotations\n- deleted {len(tag_annotations_delete)} tag annoations"
                        )
                        self.report_service.delete_annotations(
                            doc_annotations_delete, tag_annotations_delete
                        )

                    doc_annotations, tag_annotations = (
                        self.apply_service.apply_annotations(
                            diagram_detect_item, file_id
                        )
                    )
                    doc_msg = (
                        f"added/updated {len(doc_annotations)} document annotations"
                    )
                    tag_msg = f"added/updated {len(tag_annotations)} tag annotations"

                    page_count: int = diagram_detect_item["pageCount"]
                    annotated_page_count: int = self._check_all_pages_annotated(
                        annotation_state_node, page_count
                    )
                    if annotated_page_count == page_count:
                        job_node_to_update = self._process_annotation_state(
                            node=annotation_state_node,
                            status=AnnotationStatus.ANNOTATED,
                            attempt_count=next_attempt_count,
                            annotated_page_count=annotated_page_count,
                            page_count=page_count,
                            annotation_message=f"{doc_msg} and {tag_msg}",
                        )
                    else:
                        job_node_to_update = self._process_annotation_state(
                            node=annotation_state_node,
                            status=AnnotationStatus.NEW,
                            attempt_count=current_attempt_count,  # NOTE: using current_attempt_count since don't want to increment this if not fully annotated
                            annotated_page_count=annotated_page_count,
                            page_count=page_count,
                            annotation_message=f"{doc_msg} and {tag_msg}",
                        )

                    self.report_service.add_annotations(
                        doc_rows=doc_annotations, tag_rows=tag_annotations
                    )
                    self.logger.info(f"\t- {doc_msg}\n- {tag_msg}")

                except Exception as e:
                    msg = str(e)
                    if next_attempt_count >= self.max_retries:
                        job_node_to_update = self._process_annotation_state(
                            node=annotation_state_node,
                            status=AnnotationStatus.FAILED,
                            attempt_count=next_attempt_count,
                            annotation_message=msg,
                        )
                        count_failed += 1
                        self.logger.info(
                            f"\t- set the annotation status to {AnnotationStatus.FAILED}\n- ran into the following error: {msg}"
                        )
                        failed_file_ids.append(file_id)
                    else:
                        job_node_to_update = self._process_annotation_state(
                            node=annotation_state_node,
                            status=AnnotationStatus.RETRY,
                            attempt_count=next_attempt_count,
                            annotation_message=msg,
                        )
                        count_retry += 1
                        self.logger.info(
                            f"\t- set the annotation status to 'Retry'\n- ran into the following error: {msg}"
                        )
            else:
                msg = f"found 0 annotations in diagram_detect_item for file {str(file_id)}"
                if next_attempt_count >= self.max_retries:
                    job_node_to_update = self._process_annotation_state(
                        node=annotation_state_node,
                        status=AnnotationStatus.FAILED,
                        attempt_count=next_attempt_count,
                        annotation_message=msg,
                    )
                    count_failed += 1
                    self.logger.info(
                        f"\t- set the annotation status to 'Failed'\n- {msg}"
                    )
                    failed_file_ids.append(file_id)
                else:
                    job_node_to_update = self._process_annotation_state(
                        node=annotation_state_node,
                        status=AnnotationStatus.RETRY,
                        attempt_count=next_attempt_count,
                        annotation_message=msg,
                    )
                    count_retry += 1
                    self.logger.info(
                        f"\t- set the annotation status to 'Retry'\n- {msg}"
                    )
            if job_node_to_update:
                annotation_state_node_applies.append(job_node_to_update)

        if failed_file_ids:
            file_applies: NodeApplyList = (
                self.client.data_modeling.instances.retrieve_nodes(
                    nodes=failed_file_ids, sources=self.file_view.as_view_id()
                ).as_write()
            )
            for node_apply in file_applies:
                node_apply.existing_version = None
                tags_property: list[str] = cast(
                    list[str], node_apply.sources[0].properties["tags"]
                )
                if "AnnotationInProcess" in tags_property:
                    index = tags_property.index("AnnotationInProcess")
                    tags_property[index] = "AnnotationFailed"
                elif "Annotated" in tags_property:
                    self.logger.debug(
                        f"Annotated is in the tags property of {node_apply.as_id()}\nTherefore, this set of pages does not contain any annotations while the prior pages do"
                    )
                elif "AnnotationFailed" not in tags_property:
                    self.logger.error(
                        f"AnnotationFailed and AnnotationInProcess not found in tag property of {node_apply.as_id()}"
                    )
            try:
                self.client.data_modeling.instances.apply(
                    nodes=file_applies, replace=False
                )
            except CogniteAPIError as e:
                self.logger.error(
                    f"Ran into the following error:\n\t{str(e)}\nTrying again in 30 seconds"
                )
                time.sleep(30)
                self.client.data_modeling.instances.apply(
                    nodes=file_applies, replace=False
                )

        if annotation_state_node_applies:
            node_count = len(annotation_state_node_applies)
            count_annotated = node_count - count_retry - count_failed
            self.logger.info(
                message=f"Updating {node_count} annotation state instances",
                section="START",
            )
            try:
                self.apply_service.update_nodes(
                    list_node_apply=annotation_state_node_applies
                )
                self.logger.info(
                    f"\t- {count_annotated} set to Annotated\n- {count_retry} set to retry\n- {count_failed} set to failed"
                )
            except Exception as e:
                self.logger.error(
                    message=f"Error during batch update of individual annotation states: \n{e}",
                    section="END",
                )

        self.tracker.add_files(
            success=count_annotated, failed=(count_failed + count_retry)
        )

    def _process_annotation_state(
        self,
        node: Node,
        status: str,
        attempt_count: int,
        annotated_page_count: int | None = None,
        page_count: int | None = None,
        annotation_message: str | None = None,
    ) -> NodeApply:
        """
        Create a node apply from the node passed into the function.
        The annotatedPageCount and pageCount properties won't be set if this is the first time the job has been run for the specific node.
        Thus, we set it here and include logic to handle the scneario where it is set.
        NOTE: Always want to use the latest page count from the diagram detect results
        e.g.) let page_range = 50
            - If the pdf has less than 50 pages, say 3 pages, then...
                - annotationStatus property will get set to 'complete'
                - annotatedPageCount and pageCount properties will be set to 3.
            - Elif the pdf has more than 50 pages, say 80, then...
                - annotationStatus property will get set to 'new'
                - annotatedPageCount set to 50
                - pageCount set to 80
                - attemptCount doesn't get incremented
            - If an error occurs, the annotated_page_count and page_count won't be passed
                - Don't want to touch the pageCount and annotatedPageCount properties in this scenario
        """
        if not annotated_page_count or not page_count:
            update_properties = {
                "annotationStatus": status,
                "sourceUpdatedTime": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "annotationMessage": annotation_message,
                "attemptCount": attempt_count,
                "diagramDetectJobId": None,  # clear the job id
            }
        else:
            update_properties = {
                "annotationStatus": status,
                "sourceUpdatedTime": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "annotationMessage": annotation_message,
                "attemptCount": attempt_count,
                "diagramDetectJobId": None,  # clear the job id
                "annotatedPageCount": annotated_page_count,
                "pageCount": page_count,
            }

        node_apply = NodeApply(
            space=node.space,
            external_id=node.external_id,
            existing_version=None,  # update the node regardless of existing version
            sources=[
                NodeOrEdgeData(
                    source=self.annotation_state_view.as_view_id(),
                    properties=update_properties,
                )
            ],
        )

        return node_apply

    def _check_all_pages_annotated(self, node: Node, page_count: int) -> int:
        """
        The annotatedPageCount and pageCount properties won't be set if this is the first time the job has been run for the specific node.

        - if annotated_page_count is not set (first run):
            - if page_range >= to the page count:
                - annotated_page_count = page_count b/c all of the pages were passed into the FileReference during LaunchService
            - else:
                - annotated_page_count = page_range b/c there are more pages to annotate
        - else the annotation_page_count property is set:
            - if (annotated_page_count + page_range) >= page_count:
                -  annotated_page_count = page_count b/c all of the pages were passed into the FileReference during LaunchService
            else:
                - annotated_page_count = self.page_range + annotated_page_count b/c there are more pages to annotate
        """
        annotated_page_count: int | None = cast(
            int,
            node.properties[self.annotation_state_view.as_view_id()].get(
                "annotatedPageCount"
            ),
        )

        if not annotated_page_count:
            if self.page_range >= page_count:
                annotated_page_count = page_count
            else:
                annotated_page_count = self.page_range
            self.logger.info(
                f"Annotated pages 1-to-{annotated_page_count} out of {page_count} total pages"
            )
        else:
            start_page = annotated_page_count + 1
            if (annotated_page_count + self.page_range) >= page_count:
                annotated_page_count = page_count
            else:
                annotated_page_count += self.page_range
            self.logger.info(
                f"Annotated pages {start_page}-to-{annotated_page_count} out of {page_count} total pages"
            )

        return annotated_page_count

    def _update_batch_state(
        self,
        batch: BatchOfNodes,
        status: AnnotationStatus,
        failed: bool = False,
    ):
        """
        Updates the properties of FileAnnnotationState
        1. If failed set to True
            - update the status and delete the diagram detect jobId of the nodes
        2. If there's an annoatation message and attempt count
            - if status is "Processing":
                - Update the status of the nodes
                - Set 'sourceUpdateTime' to the time it was claimed so that the jobs first in line for pickup again
            - else:
                - Update the status of the nodes
        """
        if len(batch.nodes) == 0:
            return

        self.logger.info(
            message=f"Updating {len(batch.nodes)} annotation state instances"
        )
        if failed:
            update_properties = {
                "annotationStatus": status,
                "sourceUpdatedTime": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "diagramDetectJobId": None,
            }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.annotation_state_view.as_view_id(),
            )
        else:
            if status == AnnotationStatus.PROCESSING:
                claimed_time = batch.nodes[0].properties[
                    self.annotation_state_view.as_view_id()
                ]["sourceUpdatedTime"]
                update_properties = {
                    "annotationStatus": status,
                    "sourceUpdatedTime": claimed_time,
                }
            else:
                update_properties = {
                    "annotationStatus": status,
                    "sourceUpdatedTime": datetime.now(timezone.utc)
                    .replace(microsecond=0)
                    .isoformat(),
                }
            batch.update_node_properties(
                new_properties=update_properties,
                view_id=self.annotation_state_view.as_view_id(),
            )
        try:
            update_results = self.apply_service.update_nodes(
                list_node_apply=batch.apply
            )
            self.logger.info(f"- set annotation status to {status}")
        except Exception as e:
            self.logger.error(
                f"Ran into the following error:\n\t{str(e)}\nTrying again in 30 seconds",
                section="END",
            )
            time.sleep(30)
            update_results = self.apply_service.update_nodes(
                list_node_apply=batch.apply
            )
            self.logger.info(f"- set annotation status to {status}")
