from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

RobotType = Literal["SPOT", "ANYMAL", "DJI_DRONE", "TAUROB", "UNKNOWN"]
MapType = Literal["WAYPOINTMAP", "THREEDMODEL", "TWODMAP", "POINTCLOUD"]


class Point3D(BaseModelObject):
    """A point in 3D space."""

    x: float
    y: float
    z: float


class Quaternion(Point3D):
    """A quaternion representing orientation."""

    w: float


class Transform(BaseModelObject):
    """Transform of the parent frame to the current frame.

    Args:
        parent_frame_external_id: The external id of the parent frame.
        translation: Transform translation (Point3D).
        orientation: Transform orientation as quaternion (Quaternion).
    """

    parent_frame_external_id: str
    translation: Point3D
    orientation: Quaternion
