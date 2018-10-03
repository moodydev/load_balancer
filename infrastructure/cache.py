from typing import Any, Dict, Tuple

REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': True
}


class Cache:
    """In memory cache for storing data"""

    COUNT_FIELD = 'msg_count'
    PROC_TIME_FIELD = 'proc_time'
    RESET_VALUE = 0
    SYSTEM_FIELD = 'system'

    def __init__(self) -> None:
        try:
            import redis
        except ImportError:
            self.redis = None
            self.submitter = None
        else:
            self._redis = redis.StrictRedis(**REDIS)
            self._submitter = self._redis

    def start_transaction(self) -> None:
        """Starts Redis transaction that does single commit"""
        self._submitter = self._redis.pipeline()

    def end_transaction(self) -> None:
        """Ends transaction and commits all accummulated data"""

        self._submitter.execute()
        self._submitter = self._redis

    def set_field_values(self, _key: str, _dict: Dict[Any, Any]) -> None:
        """Set hash fields to Redis, can also be used to update single field"""
        self._submitter.hmset(_key, _dict)

    def get_field_values(self, _key: str, *args: str) -> Tuple:
        """Return values of field(s)"""
        return self._submitter.hmget(_key, *args)

    def increment_field(self, _key: str, _field: str, amount: float) -> None:
        """Increases given field by given amount"""
        self._submitter.hincrbyfloat(_key, _field, amount)

    def update_field(self, _key: str, _field: str, value: Any) -> None:
        """Update single field, without dict need"""
        self._submitter.hset(_key, _field, value)
