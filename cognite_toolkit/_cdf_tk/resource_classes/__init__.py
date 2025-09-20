"""Toolkit resource classes are pydantic models that represent the YAML file format the Cognite Toolkit uses.

This is means that we have three set of resource classes we use in Toolkit:

1. Toolkit resource classes (this module): Represent the YAML configuration format the Toolkit uses.
2. Write resource classes (from cognite-sdk): Represent the write/request format the Cognite resources.
3. Read resource classes (from cognite-sdk): Represent the read/response format the Cognite resources.
"""

from .asset import AssetYAML
from .base import BaseModelResource, ToolkitResource
from .containers import ContainerYAML
from .data_model import DataModelYAML
from .datapoint_subscription import DatapointSubscriptionYAML
from .dataset import DataSetYAML
from .event import EventYAML
from .extraction_pipeline import ExtractionPipelineYAML
from .extraction_pipeline_config import ExtractionPipelineConfigYAML
from .filemetadata import FileMetadataYAML
from .function_schedule import FunctionScheduleYAML
from .functions import FunctionsYAML
from .groups import GroupYAML
from .hosted_extractor_destination import HostedExtractorDestinationYAML
from .hosted_extractor_job import HostedExtractorJobYAML
from .hosted_extractor_mapping import HostedExtractorMappingYAML
from .hosted_extractor_source import HostedExtractorSourceYAML
from .instance import EdgeYAML, NodeYAML
from .labels import LabelsYAML
from .location import LocationYAML
from .raw_database_table import DatabaseYAML, TableYAML
from .relationship import RelationshipYAML
from .robotics import RobotCapabilityYAML, RobotDataPostProcessingYAML, RobotFrameYAML, RobotLocationYAML, RobotMapYAML
from .search_config import SearchConfigYAML
from .securitycategories import SecurityCategoriesYAML
from .sequence import SequenceYAML
from .space import SpaceYAML
from .streamlit_ import StreamlitYAML
from .threedmodels import ThreeDModelYAML
from .timeseries import TimeSeriesYAML
from .transformation_notification import TransformationNotificationYAML
from .transformation_schedule import TransformationScheduleYAML
from .transformations import TransformationYAML
from .views import ViewYAML
from .workflow import WorkflowYAML
from .workflow_trigger import WorkflowTriggerYAML
from .workflow_version import WorkflowVersionYAML

__all__ = [
    "AssetYAML",
    "BaseModelResource",
    "ContainerYAML",
    "DataModelYAML",
    "DataModelYAML",
    "DataSetYAML",
    "DatabaseYAML",
    "DatapointSubscriptionYAML",
    "EdgeYAML",
    "EventYAML",
    "ExtractionPipelineConfigYAML",
    "ExtractionPipelineYAML",
    "FileMetadataYAML",
    "FunctionScheduleYAML",
    "FunctionsYAML",
    "GroupYAML",
    "HostedExtractorDestinationYAML",
    "HostedExtractorJobYAML",
    "HostedExtractorMappingYAML",
    "HostedExtractorSourceYAML",
    "LabelsYAML",
    "LocationYAML",
    "NodeYAML",
    "RelationshipYAML",
    "RobotCapabilityYAML",
    "RobotDataPostProcessingYAML",
    "RobotFrameYAML",
    "RobotLocationYAML",
    "RobotMapYAML",
    "SearchConfigYAML",
    "SecurityCategoriesYAML",
    "SequenceYAML",
    "SpaceYAML",
    "StreamlitYAML",
    "TableYAML",
    "ThreeDModelYAML",
    "TimeSeriesYAML",
    "ToolkitResource",
    "TransformationNotificationYAML",
    "TransformationScheduleYAML",
    "TransformationYAML",
    "ViewYAML",
    "WorkflowTriggerYAML",
    "WorkflowVersionYAML",
    "WorkflowYAML",
]
