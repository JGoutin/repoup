"""Storage."""
from abc import ABC, abstractmethod
from os import makedirs, remove
from os.path import dirname, join
from tempfile import TemporaryDirectory
from typing import Optional, Set

from repoup.lib import AsyncContext, import_component


class StorageBase(ABC, AsyncContext):
    """Storage."""

    __slots__ = ["_tmp", "_tpm_obj"]

    def __init__(self) -> None:
        super().__init__()
        self._tpm_obj = TemporaryDirectory()
        self._tmp = self._tpm_obj.name

    @property
    def path(self) -> str:
        """Local path."""
        return self._tmp

    @abstractmethod
    def join(self, *parts: str, absolute: bool = False) -> str:
        """Join path with storage directory and returns path.

        Args:
            *parts: Path parts.
            absolute: If True, use absolute path

        Returns:
            Absolute storage path.
        """

    def tmp_join(self, *parts: str) -> str:
        """Join path with temporary directory and returns temporary local path.

        Args:
            *parts: Path parts.

        Returns:
            Absolute temporary path.
        """
        path = join(self._tmp, *parts)
        makedirs(dirname(path), exist_ok=True)
        return path

    @abstractmethod
    async def put_object(
        self, relpath: str, body: bytes, absolute: bool = False
    ) -> None:
        """Put file content.

        Args:
            relpath: Relative path.
            body: File content.
            absolute: If True, use absolute path
        """

    @abstractmethod
    async def get_object(self, relpath: str, absolute: bool = False) -> bytes:
        """Get file content.

        Args:
            relpath: Relative path.
            absolute: If True, use absolute path

        Returns:
            File content
        """

    @abstractmethod
    async def get_file(
        self, path: str, dst: Optional[str] = None, absolute: bool = False
    ) -> None:
        """Get file.

        Args:
            path: Relative path.
            dst: Destination relative path. If not specified, user "path".
            absolute: If True, use absolute path for "path".
        """

    @abstractmethod
    async def put_file(self, relpath: str, absolute: bool = False) -> None:
        """Put file.

        Args:
            relpath: Relative path.
            absolute: If True, use absolute path
        """

    @abstractmethod
    async def remove(self, path: str, absolute: bool = False) -> None:
        """Remove file from storage.

        This method must return successfully when the object to remove does not exist.

        Args:
            path: Absolute path.
            absolute: If True, use absolute path
        """

    async def remove_tmp(self, relpath: str) -> None:
        """Ensure a file is removed from the temporary directory if exists.

        Args:
            relpath: Relative path.
        """
        try:
            remove(self.tmp_join(relpath))
        except FileNotFoundError:
            return

    async def invalidate_cache(self, paths: Set[str]) -> None:
        """When the storage is behind a CDN, invalidate the cache of specified files.

        Args:
            paths: Absolute paths to invalidate.
        """


def get_storage(url: str) -> StorageBase:
    """Get storage object based on URL.

    Args:
        url: Storage URL.

    Returns:
        Storage object
    """
    scheme, path = url.split("://")
    return import_component("storage", scheme)(path)  # type: ignore
