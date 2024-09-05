"""360 image extractor base class."""

import io
import logging
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Union

import numpy as np
import py360convert
from cognite.client.data_classes import Event, FileMetadata, Label
from PIL import Image

logger = logging.getLogger(__name__)

EVENT_TYPE = "scan"
EVENT_SUB_TYPE = "terrestial"
CUBEMAP_RESOLUTION = 2048


@dataclass
class VectorXYZ:
    """Vector of floats:  (x,y,z)."""

    x: float = 0
    y: float = 0
    z: float = 0

    def to_string(self) -> str:
        """Convert vector so string."""
        return f"{self.x:.4f}, {self.y:.4f}, {self.z:.4f}"


def translation_to_mm_str_with_offset(translation: VectorXYZ, translation_offset_mm: VectorXYZ, translation_unit: str):
    """Transfrom translation to string in to mm with offset."""
    unit_clean = translation_unit.lower().replace(" ", "")
    if unit_clean == "m":
        scale = 1000
    elif unit_clean == "cm":
        scale = 10
    elif unit_clean == "mm":
        scale = 1
    else:
        raise ValueError(
            f"Translaiton unit not recognized: {translation_unit}. " 'Translation unit should be "m", "cm" or "mm".'
        )

    return VectorXYZ(
        x=(translation.x * scale - translation_offset_mm.x),
        y=(translation.y * scale - translation_offset_mm.y),
        z=(translation.z * scale - translation_offset_mm.z),
    ).to_string()


def convert_rotation_angle(rotation_angle: str, rotation_angle_unit: str) -> str:
    """Check rotation angle unit and convert to radians."""
    if rotation_angle_unit == "deg":
        return rotation_angle
    elif rotation_angle_unit == "rad":
        return str(np.rad2deg(float(rotation_angle)))
    else:
        raise ValueError(
            f"Rotation angle unit not recognized: {rotation_angle_unit}. "
            'Rotation angle unit should be "rad" or "deg".'
        )


@dataclass
class ThreesixtyImageMetadata:
    """ThreesixtyImage metadata. The metadata of the 360 events."""

    station_id: str
    station_name: str
    rotation_angle: str  # Format: "14.837"
    rotation_axis: str  # Format: "0.0990, 0.0113, 0.9950"
    translation: str  # Format: "343109.0000, 83408.0000, 30860.0000"
    timestamp: int  # Format: milliseconds since epoch
    site_id: str = ""
    site_name: str = ""


@dataclass
class ThreesixtyImage:
    """All information about the station."""

    station_number: str
    tran_unit: str
    rot_angle_unit: str
    images: dict[str, Union[str, np.ndarray]]
    threesixty_image_metadata: ThreesixtyImageMetadata


@dataclass
class ImageWithFileMetadata:
    """Image with file metadata."""

    file_metadata: FileMetadata
    content: bytes


class CogniteThreeSixtyImageExtractor:
    """Base class for 360 image extractors.

    360 images are represented by ThreesixtyImage objects. The extractor use the station objects
    to create 360 files and events in CDF.
    """

    def __init__(self, data_set_id: int, mime_type: str = "image/jpeg"):
        """Initialize ThreeSixtyImageExtractor."""
        self.data_set_id: int = data_set_id
        self.mime_type = mime_type
        self.labels: list[Label] = []

    class Faces(Enum):
        left = 0
        front = 1
        right = 2
        back = 3
        top = 4
        bottom = 5

    def create_threesixty_image(
        self,
        content: np.ndarray,
        site_id: str,
        site_name: str,
        station_number: Any,
        rotation_angle: str,
        rotation_axis: VectorXYZ,
        rotation_angle_unit: str,
        translation: VectorXYZ,
        translation_unit: str,
        translation_offset_mm: VectorXYZ,
        timestamp: int = 0,
    ) -> tuple[Event, list[ImageWithFileMetadata]]:
        """Append station measurement to station list for truview."""
        if timestamp == 0 or timestamp is None:
            timestamp = int(time.time() * 1000)
        if not isinstance(timestamp, int):
            timestamp = int(timestamp)
        station = ThreesixtyImage(
            station_number=station_number,
            rot_angle_unit="deg",
            tran_unit="mm",
            images=self._get_cubemap_images(content),
            threesixty_image_metadata=ThreesixtyImageMetadata(
                site_id=site_id,
                site_name=site_name,
                station_name=site_name + " " + station_number,
                rotation_angle=convert_rotation_angle(rotation_angle, rotation_angle_unit),
                rotation_axis=rotation_axis.to_string(),
                translation=translation_to_mm_str_with_offset(translation, translation_offset_mm, translation_unit),
                station_id=site_id + "-" + station_number,
                timestamp=timestamp,
            ),
        )

        event = self._create_cdf_events(station)
        files = self._create_cdf_files(station)
        return event, files

    def _create_cdf_events(self, three_sixty_image: ThreesixtyImage) -> Event:
        """Create 360 image eventfrom

        One event per 360 image is created.
        """
        logger.info("Create events.")
        event = Event(
            three_sixty_image.threesixty_image_metadata.station_id
            + str(three_sixty_image.threesixty_image_metadata.timestamp)
        )
        event.metadata = asdict(three_sixty_image.threesixty_image_metadata)
        event.data_set_id = self.data_set_id
        event.description = "Scan position " + three_sixty_image.threesixty_image_metadata.station_name
        event.type = EVENT_TYPE
        event.subtype = EVENT_SUB_TYPE
        event.start_time = three_sixty_image.threesixty_image_metadata.timestamp
        return event

    def _image_to_byte_array(self, image: Image):
        img_bytes = io.BytesIO()
        image.save(img_bytes, format="PNG")
        img_bytes = img_bytes.getvalue()  # type: ignore
        return img_bytes

    def _create_cdf_files(
        self, three_sixty_image: ThreesixtyImage, resolution: int = 2048
    ) -> list[ImageWithFileMetadata]:
        """Create 360 image files from three sixty image.

        Six files per 360 image is created, one file per face.
        """
        logger.info("Create 360 files.")
        files = []
        if three_sixty_image.threesixty_image_metadata.station_id:
            for i, face in enumerate(self.Faces):
                file_metadata = FileMetadata(
                    three_sixty_image.threesixty_image_metadata.station_id
                    + str(three_sixty_image.threesixty_image_metadata.timestamp)
                    + "-"
                    + str(resolution)
                    + "-"
                    + face.name
                )
                file_metadata.labels = self.labels
                metadata = {}
                metadata["site_id"] = three_sixty_image.threesixty_image_metadata.site_id
                metadata["site_name"] = three_sixty_image.threesixty_image_metadata.site_name
                metadata["station_id"] = three_sixty_image.threesixty_image_metadata.station_id
                metadata["station_name"] = three_sixty_image.threesixty_image_metadata.station_name
                metadata["timestamp"] = str(three_sixty_image.threesixty_image_metadata.timestamp)
                metadata["image_type"] = "cubemap"
                metadata["image_resolution"] = str(resolution)
                metadata["face"] = face.name
                metadata["processed"] = "false"
                file_metadata.metadata = metadata
                file_metadata.name = (
                    three_sixty_image.threesixty_image_metadata.station_name
                    + "-"
                    + str(three_sixty_image.threesixty_image_metadata.timestamp)  # For avoiding duplicate filenames
                    + "-"
                    + face.name
                    + ".jpg"
                )
                file_metadata.mime_type = self.mime_type
                file_metadata.data_set_id = self.data_set_id
                cubemap_img = three_sixty_image.images[face.name]
                content = self._image_to_byte_array(Image.fromarray(cubemap_img))
                files.append(ImageWithFileMetadata(content=content, file_metadata=file_metadata))
        else:
            logger.error("No station ID found.")
        return files

    def _cube_dice2h(self, cube_dice):
        w = cube_dice.shape[0] // 3
        assert cube_dice.shape[0] == w * 3 and cube_dice.shape[1] == w * 4
        cube_h = np.zeros((w, w * 6, cube_dice.shape[2]), dtype=cube_dice.dtype)
        # Order: F R B L U D
        sxy = [(1, 1), (2, 1), (3, 1), (0, 1), (1, 0), (1, 2)]
        for i, (sx, sy) in enumerate(sxy):
            face = cube_dice[(sy * w) : (sy + 1) * w, (sx * w) : (sx + 1) * w]
            cube_h[:, (i * w) : (i + 1) * w] = face
        return cube_h

    def _get_cubemap_images(self, content: Union[str, np.ndarray]) -> Union[dict[str, str], dict[str, np.ndarray]]:
        """Create cubemap dict from equirectangular image.

        Args:
        content: equirectangular
        Returns:
        cubemaps (dict[str,np.ndarray]: {<face>: image})
        """
        if not isinstance(content, np.ndarray):
            logger.error(
                f"Unsupported input type: Equirectangular extractor"
                f"only supports np.ndarray images. Got type {type(content)}"
            )
            raise TypeError(
                f"Unsupported input type: Equirectangular extractor "
                f"only supports np.ndarray images. Got type {type(content)}"
            )

        cubemaps: dict[str, np.ndarray] = {}
        try:
            logger.info("Creating cubemap images from equirectangular image.")
            im = py360convert.e2c(content, face_w=CUBEMAP_RESOLUTION)
            cube_h = self._cube_dice2h(im)
            cube_dict = py360convert.cube_h2dict(cube_h)

            # Translate keys from py360convert to TruView naming convention.
            dice_map = {
                "F": "front",
                "R": "right",
                "B": "back",
                "L": "left",
                "U": "top",
                "D": "bottom",
            }
            for key, im in cube_dict.items():
                cubemaps[dice_map[key]] = cube_dict[key]
        except Exception as e:
            raise Exception(f"Failed to create cubemap image from equirectangular image: {e}.")
        return cubemaps
