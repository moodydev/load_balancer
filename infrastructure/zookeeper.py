import pickle
from typing import Any, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import (
    ConnectionLoss, NodeExistsError, NoNodeError, NotEmptyError, SessionExpiredError
)
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.retry import KazooRetry

from django.conf import settings

from m_dataqualifier.processing_v2.helpers.retry import Retry


class ZooKeeper:
    """Helper ZooKeeper function that handles connection and node updates"""

    @Retry(exception_list=[ConnectionLoss, SessionExpiredError, KazooTimeoutError])
    def __init__(self) -> None:
        hosts = settings.ZOO_HOSTS
        retry = KazooRetry(max_tries=-1, max_delay=60)
        self._zk = KazooClient(hosts, connection_retry=retry, command_retry=retry)

        # establish the connection
        self._zk.start()

    def _set_node(
            self, path: str, value: Optional[Any] = None, ephemeral: bool = False) -> None:
        try:
            self._zk.retry(
                self._zk.set,
                path=path,
                value=pickle.dumps(value) or None
            )
        except NoNodeError:
            self._create_node(path, value, ephemeral)

    def _get_node(self, path: str) -> Any:
        # NoNodeError needs to be handled differently, so we dont handle it here
        value, *_ = self._zk.retry(
            self._zk.get,
            path=path,
            watch=False
        )
        return pickle.loads(value)

    def _get_children(self, path: str) -> Any:
        # NoNodeError needs to be handled differently, so we dont handle it here
        value = self._zk.retry(
            self._zk.get_children,
            path=path
        )
        return value

    def _delete_node(self, path: str, recursive: bool = True) -> bool:
        try:
            self._zk.retry(
                self._zk.delete,
                path=path,
                recursive=recursive
            )
            return True
        except NotEmptyError:
            return False

    def _create_node(
            self, path: str, value: Optional[Any] = None, ephemeral: bool = False) -> bool:
        try:
            self._zk.retry(
                self._zk.create,
                path=path,
                ephemeral=ephemeral,
                value=pickle.dumps(value) or None,
                makepath=True
            )
            return True
        except NodeExistsError:
            return False
