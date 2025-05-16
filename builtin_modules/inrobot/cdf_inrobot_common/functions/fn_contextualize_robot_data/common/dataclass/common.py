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
