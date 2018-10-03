import math
import time
from typing import List, Set, Tuple

from ..infrastructure.cache import Cache
from ..infrastructure.db import DeviceStorage
from ..infrastructure.device_mapper import (
    WorkerDeviceMapper
)
from .entities import Device, Worker


class Scheduler:
    """Reads worker and device states and on ony change, reasignes devices to workers"""

    UPDATE_INTERVAl = 30  # time in seconds
    WORKER_DEVIATION = 0.1  # index 1 how much % more load worker can have to preserve devices

    def __init__(self,
                 identity: str,
                 device_storage: DeviceStorage,
                 worker_mapper: WorkerDeviceMapper,
                 cache: Cache) -> None:

        self.identity = identity
        self.device_storage = device_storage
        self.worker_mapper = worker_mapper
        self.cache = cache
        self.last_update_time = time.monotonic()
        self._workers_map = set()  # type: Set[Worker]
        self._devices = set()  # type: Set[Device]

        self._initialize_state()

    def _initialize_state(self) -> None:
        self._workers_map = self.worker_mapper.workers
        self.device_storage.update_devices()
        self._devices = set(self.device_storage.devices)
        self._workers_map = self.balance_devices_per_worker(
            workers=self._workers_map, devices=self._devices, cache=self.cache,
            worker_load_deviation=self.WORKER_DEVIATION
        )

        # print(self._workers_map)

        self.worker_mapper.update_worker_devices(self._workers_map)

    def run(self) -> None:
        """Main Scheduler method that checks for updates in intervals"""
        devices_changed = False
        workers_changed = False

        self.device_storage.update_devices()

        time_now = time.monotonic()
        last_update = time_now - self.last_update_time
        update_time = last_update >= self.UPDATE_INTERVAl

        updated_devices = set(self.device_storage.devices)
        new_worker_state = self.worker_mapper.workers

        if updated_devices != self._devices:
            self._devices = updated_devices
            devices_changed = True

        if self._workers_map != new_worker_state:
            self._workers_map = new_worker_state
            workers_changed = True

        if devices_changed or workers_changed or update_time:
            self._workers_map = self.balance_devices_per_worker(
                workers=self._workers_map, devices=self._devices, cache=self.cache,
                worker_load_deviation=self.WORKER_DEVIATION
            )
            self.worker_mapper.update_worker_devices(self._workers_map)
            self.last_update_time = time_now

            for worker in self._workers_map:
                print('\n', worker)

    @staticmethod
    def fetch_cache_data(
            devices: Set[Device], cache: Cache) -> Tuple[Set[Device], int, float]:
        """Updated Device fields for load index calculation and resets cache

        Returns updated Device set, system msg count and system processing_time
        """

        for device in devices:
            _key = 'device:{}'.format(device.id_)
            try:
                device.msg_count, device.proc_time = cache.get_field_values(
                    _key, cache.COUNT_FIELD, cache.PROC_TIME_FIELD)
                cache.set_field_values(
                    _key, {cache.COUNT_FIELD: cache.RESET_VALUE,
                           cache.PROC_TIME_FIELD: cache.RESET_VALUE}
                )
            except AttributeError:
                pass
            device.load_index = 0

        system_msg_count = sum(device.msg_count for device in devices)
        interval = sum(device.proc_time for device in devices)

        return devices, system_msg_count, interval

    @classmethod
    def device_load_index_formula(
            cls, device: Device, decimal_points: int,
            interval: float, system_msg_count: int) -> None:
        """Updates load index for given Device"""

        proc_time_index = 0.7
        msg_count_index = 0.3
        try:
            load_index = round(
                (device.proc_time * proc_time_index / interval +
                 device.msg_count * msg_count_index / system_msg_count) /
                (proc_time_index + msg_count_index), decimal_points
            )
        except ZeroDivisionError:
            load_index = 0
        device.load_index = load_index

    @classmethod
    def balance_with_load_indexes(
            cls, workers: Set[Worker], devices: Set[Device], worker_deviation: float,
            interval: float, system_msg_count: int) -> Set[Worker]:
        """Balances workers with device load indexes"""

        # calculate decimal points depending on number of devices
        decimal_points = len([c for c in str(len(devices))])
        decimal_points = math.ceil(decimal_points + (decimal_points*5/4))

        # calculate how much load can worker have
        load_per_worker = round((1/len(workers)), decimal_points)

        # how much 'extra' load worker can get to try to keep it's devices
        deviation_per_worker_load = load_per_worker+(load_per_worker*worker_deviation)

        for device in devices:
            cls.device_load_index_formula(device, decimal_points, interval, system_msg_count)

        for worker in sorted(workers, reverse=True):
            worker_new_devices = set()  # type: Set[Device]
            worker.load_index = 0
            for device in sorted(devices, reverse=True):
                if device in worker:
                    if deviation_per_worker_load > (device.load_index + worker.load_index):
                        worker_new_devices.add(device)
                        worker.load_index += device.load_index
            devices -= worker_new_devices
            worker.devices = worker_new_devices

        # TODO: This part needs to be smarter, not just random
        # Heavy workers(like device or two with heavy load) shouldn't get small load devices
        # One worker should always be elected as 'reprocessing' worker which would just deal with
        # devices that are in reprocess state

        # TODO: logic that could recognize device as reprocessing

        # existing coord service works great with this! victory!
        for device in sorted(devices, reverse=True):
            worker = sorted(workers)[0]
            worker.devices.add(device)
            worker.load_index += device.load_index

        return workers

    @staticmethod
    def _sort_workers(worker: Worker) -> Tuple[int, int]:
        """Helper sorting method when we have no load indexes"""

        try:
            return len(worker), min(device.id_ for device in worker.devices)
        except ValueError:
            return len(worker), 0

    @staticmethod
    def get_devices_per_worker(worker_count: int, devices_count: int) -> List[int]:
        """Calculates how many devices can be assigned per worker

        Return example: 3 workers, 8 devices: [3, 3, 2]
        """
        devices_per_worker = []

        while worker_count:
            try:
                per_worker = math.ceil(devices_count/worker_count)
            except ZeroDivisionError:
                per_worker = 0
            devices_per_worker.append(per_worker)
            devices_count -= per_worker
            worker_count -= 1
        return devices_per_worker

    @classmethod
    def balance_with_count_per_worker(
            cls, workers: Set[Worker], devices: Set[Device]) -> Set[Worker]:
        """Worker rebalance method which assigns 'equal' number of devices per worker
        with minimal changes to worker's assigned devices
        """

        devices_per_worker = cls.get_devices_per_worker(len(workers), len(devices))
        devices = set(devices)
        ordered_workers = sorted(workers, key=lambda item: cls._sort_workers(item), reverse=True)

        for worker in ordered_workers:
            worker_device_count = max(devices_per_worker)
            worker_new_devices = set()  # type: Set[Device]
            worker.load_index = 0
            for device in sorted(devices):
                if worker_device_count == len(worker_new_devices):
                    break
                elif device in worker:
                    worker_new_devices.add(device)
            devices -= worker_new_devices
            try:
                devices_per_worker.remove(len(worker_new_devices))
            except ValueError:
                pass
            worker.devices = worker_new_devices

        for leftover_device in sorted(devices):
            worker = sorted(workers, key=lambda item: cls._sort_workers(item))[0]
            worker.add_device(leftover_device)

        return workers

    @classmethod
    def balance_devices_per_worker(
            cls, workers: Set[Worker], devices: Set[Device], cache: Cache,
            worker_load_deviation: float = 0.0) -> Set[Worker]:
        """Main balancing function that decides how we balance devices
        which depends on the state of cache
        """

        if not workers or not devices:
            return workers

        devices, system_msg_count, interval = cls.fetch_cache_data(devices, cache)
        workers = set(workers)

        if system_msg_count:
            workers = cls.balance_with_load_indexes(
                workers, devices, worker_load_deviation, interval, system_msg_count)
        else:
            workers = cls.balance_with_count_per_worker(workers, devices)

        return workers
