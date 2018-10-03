from typing import Iterable

from ..helpers.db_connector import DbConnector
from ..helpers.interval_decorator import ExecutionInterval
from ..service.entities import Device


class DeviceStorage:
    """Keeps state of current enabled devices"""

    UPDATE_INTERVAL = 30

    def __init__(self) -> None:
        self._devices = set()  # type: Set[Device]
        self._fetch_devices()

    @property
    def devices(self) -> Iterable[Device]:
        """Return state devices as iterable"""

        for device in self._devices:
            yield device

    @ExecutionInterval(seconds=UPDATE_INTERVAL)
    def update_devices(self) -> None:
        """Periodically updates devices for specified update interval"""
        self._fetch_devices()

    def _fetch_devices(self):
        """True device fetcher, does DB query, but can't put ExecutionInterval directly"""

        device_query = """
            SELECT id
            FROM m_controldata_device
            WHERE enabled = true AND processable = true
            ORDER BY id
        """

        self._devices = {
            Device(device_id) for device_id, *_ in DbConnector.execute_sql(device_query)
        }
