from dataclasses import asdict, dataclass


@dataclass
class Dataclass:
    def to_dict(self, ignore_none=False):
        if ignore_none:
            return asdict(self, dict_factory=lambda x: {k: v for (k, v) in x if v})
        return asdict(self)


@dataclass
class Vec3f(Dataclass):
    x: float
    y: float
    z: float


@dataclass(frozen=True)
class PayloadType:
    IR_CAMERA = "ir_camera_payload"
    PTZ_CAMERA = "ptz_camera_payload"
    THREESIXTY_CAMERA = "threesixty_camera_payload"


@dataclass(frozen=True)
class PayloadActionType:
    IR_SCAN = "ir_scan"
    SPILL_DETECTION = "spill_detection"
    VALVE_READING = "valve_reading"
    GAUGE_READING = "gauge_reading"
    MULTI_GAUGE_READING = "multi_gauge_reading"
    THREESIXTY_CAPTURE = "threesixty_capture"