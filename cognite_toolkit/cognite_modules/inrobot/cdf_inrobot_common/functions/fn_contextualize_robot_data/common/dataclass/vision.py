from dataclasses import dataclass
from typing import List, Optional

from common.dataclass.common import Dataclass


@dataclass
class File(Dataclass):
    fileId: int
    fileExternalId: Optional[str] = None


@dataclass
class RequestCustomModelPrediction(Dataclass):
    items: List[File]
    modelFile: File
    threshold: float


@dataclass
class Vertex(Dataclass):
    x: float
    y: float


@dataclass
class Region(Dataclass):
    shape: str  # points, rectangle, polyline, polygon
    vertices: List[Vertex]


@dataclass
class VisionAnnotation(Dataclass):
    text: str
    confidence: Optional[float] = None
    region: Optional[Region] = None


@dataclass
class Item(Dataclass):
    fileId: int
    annotations: Optional[List[VisionAnnotation]] = None
    fileExternalId: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None


@dataclass
class FailedBatchSchema(Dataclass):
    errorMessage: Optional[str] = None
    items: Optional[List[File]] = None


@dataclass
class ResponseCustomModelPrediction(Dataclass):
    status: str  # "Queued" "Running" "Completed" "Failed"
    createdTime: int
    startTime: int
    statusTime: int
    jobId: int
    items: List[Item]
    modelFile: File
    threshold: Optional[float] = None
    failedItems: Optional[List[FailedBatchSchema]] = None
