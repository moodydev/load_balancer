from typing import Any, Set, Union


class Device:
    def __init__(self, id_: int, load_index: float = 0.0) -> None:
        self.id_ = id_
        self.load_index = load_index
        # TODO reprocessing will be memory hog, need mechanism for effective dealing with this
        # some out of order data is ok, since scheduler will just reasign properly
        # good idea would be to increase memory treshold on those workers, and decrease on others with lighter load
        self.reprocessing = False
        self._msg_count = 0
        self._proc_time = 0.0

    def __repr__(self) -> str:
        return '(ID: {id_}, Load index: {load_index})'.format(**self.__dict__)

    def __lt__(self, other: Any) -> bool:
        if self.load_index == other.load_index:
            return self.id_ < other.id_

        return self.load_index < other.load_index

    def __eq__(self, other: Any) -> bool:
        return self.id_ == other.id_

    def __hash__(self) -> int:
        return hash(self.id_)

    @property
    def msg_count(self) -> int:
        return self._msg_count

    @msg_count.setter
    def msg_count(self, value: Union[None, str]) -> None:
        self._msg_count = int(value) if value else 0

    @property
    def proc_time(self) -> float:
        return self._proc_time

    @proc_time.setter
    def proc_time(self, value: Union[None, str]) -> None:
        self._proc_time = float(value) if value else 0


class Database:
    def __init__(self, name: str) -> None:
        self.name = name
        self.devices = set()  # type: Set[Device]
        self.is_active = False
        self.load_index = 0

    def __repr__(self) -> str:
        return 'DB(Name: {name}, Load Index: {load_index})'.format(**self.__dict__)


class Worker:
    def __init__(self, identity: str) -> None:
        self._identity = identity
        self._devices = set()  # type: Set[Device]
        self.databases = set()  # type: Set[Database]
        self.load_index = 0

    def __repr__(self) -> str:
        return 'Worker(ID: {} Load index: {}, Devices: {})'.format(
            self.identity, self.load_index, sorted(self.devices, reverse=True))

    def __lt__(self, other: Any) -> bool:
        if self.load_index == other.load_index:
            return len(self.devices) < len(other.devices)

        return self.load_index < other.load_index

    def __eq__(self, item: Any) -> bool:
        return hash(self) == hash(item)

    def __hash__(self) -> int:
        return hash(self._identity)

    def __contains__(self, device: Device) -> bool:
        return device in self.devices

    def __len__(self) -> int:
        return len(self.devices)

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def devices(self) -> Set[Device]:
        return self._devices

    @devices.setter
    def devices(self, new_devices: Set[Device]) -> None:
        self._devices = new_devices

    def add_device(self, device: Device) -> None:
        self._devices.add(device)
