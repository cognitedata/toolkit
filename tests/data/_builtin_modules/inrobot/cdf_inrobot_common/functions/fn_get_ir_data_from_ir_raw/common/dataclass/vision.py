from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from common.dataclass.common import Dataclass


@dataclass
class File(Dataclass):
    fileId: int
    fileExternalId: Optional[str] = None


@dataclass
class RequestCustomModelPrediction(Dataclass):
    items: list[File]
    modelFile: File
    threshold: float


@dataclass
class Vertex(Dataclass):
    x: float
    y: float


@dataclass
class Region(Dataclass):
    shape: str  # points, rectangle, polyline, polygon
    vertices: list[Vertex]


@dataclass
class VisionAnnotation(Dataclass):
    text: str
    confidence: Optional[float] = None
    region: Optional[Region] = None


@dataclass
class Item(Dataclass):
    fileId: int
    annotations: Optional[list[VisionAnnotation]] = None
    fileExternalId: Optional[str] = None
    width: Optional[float] = None
    height: Optional[float] = None


@dataclass
class FailedBatchSchema(Dataclass):
    errorMessage: Optional[str] = None
    items: Optional[list[File]] = None


@dataclass
class ResponseCustomModelPrediction(Dataclass):
    status: str  # "Queued" "Running" "Completed" "Failed"
    createdTime: int
    startTime: int
    statusTime: int
    jobId: int
    items: list[Item]
    modelFile: File
    threshold: Optional[float] = None
    failedItems: Optional[list[FailedBatchSchema]] = None
