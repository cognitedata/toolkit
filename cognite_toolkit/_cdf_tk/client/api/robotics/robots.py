from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import overload

from cognite.client._api_client import APIClient

from cognite_toolkit._cdf_tk.client.api.robotics.utlis import tmp_disable_gzip
from cognite_toolkit._cdf_tk.client.data_classes.robotics import Robot, RobotList, RobotWrite, _RobotUpdate


class RobotsAPI(APIClient):
    _RESOURCE_PATH = "/robotics/robots"

    @overload
    def __call__(self) -> Iterator[Robot]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[RobotList]: ...

    def __call__(self, chunk_size: int | None = None) -> Iterator[Robot] | Iterator[RobotList]:
        """Iterate over robots.

        Args:
            chunk_size: The number of robots to return in each chunk. None will return all robots.

        Yields:
            Robot or RobotList

        """
        return self._list_generator(method="GET", resource_cls=Robot, list_cls=RobotList, chunk_size=chunk_size)

    def __iter__(self) -> Iterator[Robot]:
        return self.__call__()

    @overload
    def create(self, robot: RobotWrite) -> Robot: ...

    @overload
    def create(self, robot: Sequence[RobotWrite]) -> RobotList: ...

    def create(self, robot: RobotWrite | Sequence[RobotWrite]) -> Robot | RobotList:
        """Create a new robot.

        Args:
            robot: RobotWrite or list of RobotWrite.

        Returns:
            Robot object.

        """
        with tmp_disable_gzip():
            return self._create_multiple(
                list_cls=RobotList,
                resource_cls=Robot,
                items=robot,
                input_resource_cls=RobotWrite,
            )

    def retrieve(self, data_set_id: int | Sequence[int]) -> RobotList:
        """Retrieve a robot.

        Args:
            data_set_id: Data set id of the robot.

        Returns:
            Robot object.

        """
        body = self._create_body(data_set_id)
        res = self._post(url_path=self._RESOURCE_PATH + "/byids", json=body)
        return RobotList._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _create_body(data_set_id: int | Sequence[int]) -> dict:
        ids = [data_set_id] if isinstance(data_set_id, int) else data_set_id
        body = {"items": [{"dataSetId": external_id} for external_id in ids]}
        return body

    @overload
    def update(self, robot: RobotWrite) -> Robot: ...

    @overload
    def update(self, robot: Sequence[RobotWrite]) -> RobotList: ...

    def update(self, robot: RobotWrite | Sequence[RobotWrite]) -> Robot | RobotList:
        """Update a robot.

        Args:
            robot: RobotWrite or list of RobotWrite.

        Returns:
            Robot object.

        """
        is_single = False
        if isinstance(robot, RobotWrite):
            robots = [robot]
            is_single = True
        elif isinstance(robot, Sequence):
            robots = list(robot)
        else:
            raise ValueError("robot must be a RobotWrite or a list of RobotWrite")

        property_spec = _RobotUpdate._get_update_properties()
        update = [
            {"dataSetId": r.data_set_id, **self._convert_resource_to_patch_object(r, property_spec)} for r in robots
        ]
        res = self._post(url_path=self._RESOURCE_PATH + "/update", json={"items": update})
        loaded = RobotList._load(res.json()["items"], cognite_client=self._cognite_client)
        return loaded[0] if is_single else loaded

    def delete(self, data_set_id: int | Sequence[int]) -> None:
        """Delete a robot.

        Args:
            data_set_id: Data set id of the robot.

        Returns:
            None

        """
        body = self._create_body(data_set_id)
        self._post(url_path=self._RESOURCE_PATH + "/delete", json=body)

    def list(self) -> RobotList:
        """List robots.

        Returns:
            RobotList

        """
        with tmp_disable_gzip():
            return self._list(method="GET", resource_cls=Robot, list_cls=RobotList)
