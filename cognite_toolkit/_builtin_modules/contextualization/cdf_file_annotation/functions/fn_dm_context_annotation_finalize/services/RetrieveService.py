import abc
from typing import cast
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling import (
    Node,
    NodeId,
    NodeList,
    NodeApplyList,
    instances,
)

from cognite.client.data_classes.filters import (
    Filter,
    Equals,
)

from services.ConfigService import (
    Config,
    ViewPropertyConfig,
    build_filter_from_query,
)
from services.LoggerService import CogniteFunctionLogger
from utils.DataStructures import AnnotationStatus


class IRetrieveService(abc.ABC):
    """
    Interface for retrieving diagram detect jobs
    """

    @abc.abstractmethod
    def get_diagram_detect_job_result(self, job_id: int) -> dict | None:
        pass

    @abc.abstractmethod
    def get_job_id(self) -> tuple[int, dict[NodeId, Node]] | tuple[None, None]:
        pass


class GeneralRetrieveService(IRetrieveService):
    """
    Interface for retrieving diagram detect jobs
    """

    def __init__(
        self, client: CogniteClient, config: Config, logger: CogniteFunctionLogger
    ):
        self.client = client
        self.config = config
        self.logger: CogniteFunctionLogger = logger

        self.annotation_state_view: ViewPropertyConfig = (
            self.config.data_model_views.annotation_state_view
        )
        self.file_view: ViewPropertyConfig = self.config.data_model_views.file_view

        self.filter_jobs: Filter = build_filter_from_query(
            config.finalize_function.retrieve_service.get_job_id_query
        )
        self.job_api: str = (
            f"/api/v1/projects/{self.client.config.project}/context/diagram/detect"
        )

    def get_diagram_detect_job_result(self, job_id: int) -> dict | None:
        url = f"{self.job_api}/{job_id}"
        result = None
        response = self.client.get(url)
        if response.status_code == 200:
            job_results: dict = response.json()
            if job_results.get("status") == "Completed":
                result = job_results
                return result
            else:
                self.logger.debug(f"{job_id} - Job not complete")
        else:
            self.logger.debug(
                f"{job_id} - Request to get job result failed with {response.status_code} code"
            )
        return None

    def get_job_id(self) -> tuple[int, dict[NodeId, Node]] | tuple[None, None]:
        """
        To ensure threads are protected, we do the following...
        1. Query for an available job id
        2. Find all annotation state nodes with that job id
        3. Claim those nodes by providing the existing version in the node apply request
        4. if an error is returned because of existing version mismatch
            - That means another thread claimed that job id already
            - Raise the error from this function call so that the thread can claim another job id
        5. if no error occurs
            - That means this thread was first to claim that job id
            - return the job id and file to state map
        """
        sort_by_time = []
        sort_by_time.append(
            instances.InstanceSort(
                property=self.annotation_state_view.as_property_ref(
                    "sourceUpdatedTime"
                ),
                direction="descending",
            )
        )

        annotation_state_instance: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.annotation_state_view.as_view_id(),
            space=self.annotation_state_view.instance_space,
            limit=-1,
            filter=self.filter_jobs,
            sort=sort_by_time,
        )

        if len(annotation_state_instance) == 0:
            return None, None

        job_node: Node = annotation_state_instance.pop(-1)
        job_id: int = cast(
            int,
            job_node.properties[self.annotation_state_view.as_view_id()][
                "diagramDetectJobId"
            ],
        )

        filter_job_id = Equals(
            property=self.annotation_state_view.as_property_ref("diagramDetectJobId"),
            value=job_id,
        )
        list_job_nodes: NodeList = self.client.data_modeling.instances.list(
            instance_type="node",
            sources=self.annotation_state_view.as_view_id(),
            space=self.annotation_state_view.instance_space,
            limit=-1,  # Grab all instances of annotation_state_node
            filter=filter_job_id,
            sort=sort_by_time,
        )
        try:
            self._attempt_to_claim(list_job_nodes.as_write())
        except CogniteAPIError as e:
            raise e  # NOTE: let the main loop handle error -> if error occurs should be version error

        # NOTE: could bundle this with the attempt to claim loop. Chose not to since the run time gains is negligible and improves readability.
        file_to_state_map: dict[NodeId, Node] = {}
        for node in list_job_nodes:
            file_reference = node.properties.get(
                self.annotation_state_view.as_view_id()
            ).get("linkedFile")
            file_node_id = NodeId(
                space=file_reference["space"], external_id=file_reference["externalId"]
            )
            file_to_state_map[file_node_id] = node

        return job_id, file_to_state_map

    def _attempt_to_claim(self, list_job_nodes_to_claim: NodeApplyList) -> None:
        """
        (Optimistic locking based off the node version)
        Attempt to 'claim' the annotation state nodes by updating the annotation status property.
        This relies on how the API applies changes to nodes. Specifically... if an existing version is provided in the nodes
        that are used for the .apply() endpoint, a version conflict will occur if another thread has already claimed the job.

        NOTE: The optimistic locking works most of the time. However, a race condition can occur due to a
              read-after-write consistency gap, especially when a thread fails a claim and immediately retries getting a new job.

              Scenario:
              1. Thread A successfully claims a job, updating nodes to version=2 and status="Finalizing".
              2. Thread B fails its first claim on the same job due to a version conflict (expected behavior).
              3. Thread B immediately re-queries for nodes with status="Processing".
                 Due to a minuscule replication lag in the underlying database, the query's filter may still
                 see the just-claimed nodes as "Processing" and return them.
              4. However, the full node data retrieved in this new query result will correctly have version=2.
              5. This allows Thread B's second `apply()` call to succeed because it is now providing the correct, latest version, bypassing the lock
                 and leading to duplicate processing.

        The 'elif' check below solves this by validating it on the client-side. It verifies the status from the retrieved properties.
        If a node was fetched by a filter for "Processing" but its properties already show "Finalizing", we have detected this race condition and
        must manually raise an error to prevent the duplicate claim.
        """
        for node_apply in list_job_nodes_to_claim:
            if (
                node_apply.sources[0].properties["annotationStatus"]
                == AnnotationStatus.PROCESSING
            ):
                node_apply.sources[0].properties[  # type: ignore
                    "annotationStatus"
                ] = AnnotationStatus.FINALIZING
            elif (
                node_apply.sources[0].properties["annotationStatus"]
                == AnnotationStatus.FINALIZING
            ):
                self.logger.debug("Lock bypassed. Caught on the client-side.")
                raise CogniteAPIError(message="Job has already been claimed", code=400)

        update_results = self.client.data_modeling.instances.apply(
            nodes=list_job_nodes_to_claim
        )

        return
