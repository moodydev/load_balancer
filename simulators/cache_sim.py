from collections import Counter, defaultdict
from typing import Any, Dict, Tuple


class CacheSimulator:
    COUNT_FIELD = 'msg_count'
    PROC_TIME_FIELD = 'proc_time'
    RESET_VALUE = 0
    SYSTEM_FIELD = 'system'

    def __init__(self) -> None:
        self.cache = defaultdict(Counter)

    def start_transaction(self) -> None:
        pass

    def end_transaction(self) -> None:
        pass

    def set_field_values(self, _key: str, _dict: Dict[Any, Any]) -> None:
        for key, value in _dict.items():
            self.cache[_key][key] = value

    def get_field_values(self, _key: str, *args: str) -> Tuple:
        return tuple([self.cache[_key][arg] for arg in args])

    def increment_field(self, _key: str, _field: str, amount: float) -> None:
        self.cache[_key][_field] += amount

    def update_field(self, _key: str, _field: str, value: Any) -> None:
        self.cache[_key][_field] = value

    def clear(self) -> None:
        del self.cache
        self.cache = defaultdict(Counter)
