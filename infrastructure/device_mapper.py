from typing import List, Set

from kazoo.exceptions import NoNodeError
from kazoo.recipe.watchers import ChildrenWatch

from .. import ZooKeeper
from ..entities import Device, Worker


class WorkerDeviceMapper(ZooKeeper):
    """Scheduler's coordination service

    Listens for node updates and updates it's state which is passed to Scheduler service
    Does all the work on infrastructure update handling
    """

    WORKERS_DEVICES_PATH = '/*/processing/worker_dev'
    WORKER_PATH = '/*/processing/workers'

    def __init__(self) -> None:
        super().__init__()
        self.__workers = set()  # type: Set[Worker]

        self._create_node(path=self.WORKER_PATH)

        @ChildrenWatch(self._zk, self.WORKER_PATH)
        def child_watch_func(children: List[str]) -> None:
            """Zoo listener for updating worker state (adding/remowing worker)"""

            self.__workers = {Worker(worker_id) for worker_id in children}
            self._get_worker_state_from_zookeeper()

    @property
    def workers(self) -> Set[Worker]:
        """Return existing workers and their assigned devices"""
        return self.__workers

    def update_worker_devices(self, workers: Set[Worker]) -> None:
        """Clean current zoo wrker_dev path and assign new devices"""

        self._remove_worker_device_map_and_children()
        for worker in workers:
            self._set_workers_devices(worker.identity, worker.devices)

    def _get_worker_state_from_zookeeper(self) -> None:
        """Fetches old worker state and their assigned devices from Zookeeper"""
        try:
            for worker_id in self._get_children(path=self.WORKERS_DEVICES_PATH):
                value = self._get_node(path='{}/{}'.format(self.WORKERS_DEVICES_PATH, worker_id))
                for worker in self.__workers:  # type: Worker
                    if worker == worker_id:
                        worker.devices = set(Device(id) for id in value)
        except NoNodeError:
            pass

    def _set_workers_devices(self, worker: str, devices: Set[Device]) -> None:
        """Sets devices for worker assigned in Scheduler service"""
        worker_path = '{}/{}'.format(self.WORKERS_DEVICES_PATH, worker)
        self._create_node(
            path='{}'.format(worker_path),
            value=[device.id_ for device in devices]
        )

    def _remove_worker_device_map_and_children(self) -> None:
        """Remove a worker_device_map path for clean Zookeeper state"""

        self._delete_node(path=self.WORKERS_DEVICES_PATH)
