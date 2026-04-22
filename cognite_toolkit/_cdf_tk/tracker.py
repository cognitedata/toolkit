import os
import platform
import tempfile
import threading
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

from mixpanel import Consumer, Mixpanel, MixpanelException

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.constants import IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes import TrackingEvent
from cognite_toolkit._cdf_tk.utils import get_cicd_environment
from cognite_toolkit._cdf_tk.utils.user import UserInfo
from cognite_toolkit._version import __version__

_COGNITE_TOOLKIT_MIXPANEL_TOKEN: str = "9afc120ac61d408c81009ea7dd280a38"


class Tracker:
    def __init__(self, skip_tracking: bool = False) -> None:
        self.mp = Mixpanel(_COGNITE_TOOLKIT_MIXPANEL_TOKEN, consumer=Consumer(api_host="api-eu.mixpanel.com"))
        self.skip_tracking = skip_tracking
        self._distinct_id: str | None = None
        self._all_event_properties: dict[str, Any] | None = None

    def _get_all_event_properties(self, client: ToolkitClient | None = None) -> dict[str, Any]:
        """These properties are always included in every event we sent to Mixpanel"""
        if self._all_event_properties is not None and (
            client is None or self._all_event_properties["cluster"] != "offline"
        ):
            return self._all_event_properties

        cluster = "offline"
        project = "offline"
        organization = "offline"
        private_link = "offline"

        if client is not None:
            cluster = client.config.cdf_cluster or "unknown"
            private_link = "yes" if client.config.is_private_link else "no"
            try:
                result = client.project.organization()
            except (ToolkitAPIError, ValueError):
                organization = "unknown"
                project = client.config.project
            else:
                organization = result.organization
                project = result.name
        self._all_event_properties = {
            "toolkitVersion": __version__,
            "$os": platform.system(),
            "pythonVersion": platform.python_version(),
            "CICD": self._cicd,
            "project": project,
            "organization": organization,
            "cluster": cluster,
            "privateLink": private_link,
        }
        return self._all_event_properties

    def track(self, event: TrackingEvent, client: ToolkitClient | None) -> bool:
        distinct_id = self._get_distinct_id(client)
        event_properties = event.to_dict()
        event_properties.update(self._get_all_event_properties(client))

        return self._track(distinct_id, event.event_name, event_properties)

    def _track(self, distinct_id: str, event_name: str, event_information: dict[str, Any]) -> bool:
        if self.skip_tracking or "PYTEST_CURRENT_TEST" in os.environ:
            return False

        def track() -> None:
            with suppress(ConnectionError, MixpanelException):
                self.mp.track(distinct_id, event_name, event_information)

        if IN_BROWSER:
            track()
        else:
            threading.Thread(target=track, daemon=False).start()

        return True

    def _get_distinct_id(self, client: ToolkitClient | None = None) -> str:
        if "PYTEST_CURRENT_TEST" in os.environ:
            return "pytest"
        if self._distinct_id:
            return self._distinct_id
        distinct_id: str | None = None
        user_properties = {"$os": platform.system(), "CICD": self._cicd}
        if client:
            # First choice, use CDF.
            user_info = UserInfo.load(client)
            user_properties.update(user_info.model_dump(exclude_none=True, mode="json", exclude_unset=True))
            if user_info.id:
                distinct_id = user_info.id
                user_properties["mode"] = "online"

        if distinct_id is None:
            # Fallback to generate an ID and load from file.
            user_properties["mode"] = "offline"
            cache_file = Path(tempfile.gettempdir()) / "tk-distinct-id.bin"
            if cache_file.exists():
                distinct_id = cache_file.read_text()
            else:
                distinct_id = f"{self._cicd}-{platform.system()}-{platform.python_version()}-{uuid.uuid4()!s}"
                cache_file.write_text(distinct_id)

        with suppress(ConnectionError, MixpanelException):
            self.mp.people_set(distinct_id, user_properties)
        self._distinct_id = distinct_id
        return distinct_id

    @property
    def _cicd(self) -> str:
        return get_cicd_environment()
