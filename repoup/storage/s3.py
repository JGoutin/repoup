"""AWS S3 storage."""
from asyncio import sleep
from contextlib import contextmanager
from datetime import datetime
from os import getenv
from posixpath import join
from typing import Generator, Optional, Set

from aioboto3 import Session
from botocore.exceptions import ClientError

from repoup.exceptions import PackageNotFound
from repoup.storage import StorageBase

SESSION = Session()

_DISTRIBUTION_ID = getenv("CLOUDFRONT_DISTRIBUTION_ID")
_SAME_CALLER_REFERENCE = (
    "Your request contains a caller reference that was used for a "
    "previous invalidation batch for the same distribution"
)


@contextmanager
def _s3_exception_handler(key: str) -> Generator[None, None, None]:
    """Convert common S3 exceptions.

    Args:
        key: S3 key.

    Raises:
        xvc.worker.exceptions.NotFoundException: key not found.
    """
    try:
        yield
    except ClientError as exception:
        if exception.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise PackageNotFound(key) from None
        raise  # pragma: no cover


class Storage(StorageBase):
    """AWS S3 storage.

    Args:
        path: S3 path in the form "bucket/key".
    """

    __slots__ = ["_bucket", "_prefix", "_client"]

    def __init__(self, path: str) -> None:
        StorageBase.__init__(self)
        bucket, prefix = path.split("/", 1)
        self._bucket = bucket
        self._prefix = prefix

    async def __aenter__(self) -> "Storage":
        self._client = await self._exit_stack.enter_async_context(
            SESSION.client("s3", endpoint_url=getenv("S3_ENDPOINT_URL"))
        )
        return self

    def join(self, *parts: str, absolute: bool = False) -> str:
        """Join path with storage directory and returns path.

        Args:
            *parts: Path parts.
            absolute: If True, use absolute path

        Returns:
            Absolute storage path.
        """
        if absolute:
            return join(*parts)
        return join(self._prefix, *parts)

    async def put_object(self, path: str, body: bytes, absolute: bool = False) -> None:
        """Put file content.

        Args:
            path: Relative path.
            body: File content.
            absolute: If True, use absolute path
        """
        await self._client.put_object(
            Bucket=self._bucket, Key=self.join(path, absolute=absolute), Body=body
        )

    async def get_object(self, path: str, absolute: bool = False) -> bytes:
        """Get file content.

        Args:
            path: Relative path.
            absolute: If True, use absolute path

        Returns:
            File content
        """
        src = self.join(path, absolute=absolute)
        with _s3_exception_handler(src):
            return await (  # type: ignore
                await self._client.get_object(Bucket=self._bucket, Key=src)
            )["Body"].read()

    async def get_file(
        self, path: str, dst: Optional[str] = None, absolute: bool = False
    ) -> None:
        """Get file.

        Args:
            path: Relative path.
            dst: Destination relative path. If not specified, user "path".
            absolute: If True, use absolute path for "path".
        """
        src = self.join(path, absolute=absolute)
        with _s3_exception_handler(src):
            await self._client.download_file(
                self._bucket, src, self.tmp_join(dst or path)
            )

    async def put_file(
        self, path: str, dst: Optional[str] = None, absolute: bool = False
    ) -> None:
        """Put file.

        Args:
            path: Relative path.
            dst: Destination relative path. If not specified, use "path".
            absolute: If True, use absolute path
        """
        await self._client.upload_file(
            self.tmp_join(path), self._bucket, self.join(dst or path, absolute=absolute)
        )

    async def remove(self, path: str, absolute: bool = False) -> None:
        """Remove file from storage.

        Args:
            path: Absolute path.
            absolute: If True, use absolute path
        """
        # Only delete if exists to avoid issues when triggering this function
        # with "ObjectRemoved:*" S3 events
        if await self.exists(path, absolute=absolute):
            await self._client.delete_object(
                Bucket=self._bucket, Key=self.join(path, absolute=absolute)
            )

    async def exists(self, path: str, absolute: bool = False) -> bool:
        """Return True if file exists.

        Args:
            path: Relative path.
            absolute: If True, use absolute path for "path".
        """
        try:
            await self._client.head_object(
                Bucket=self._bucket, Key=self.join(path, absolute=absolute)
            )
        except ClientError as exception:
            if exception.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return False
            raise  # pragma: no cover
        return True

    async def invalidate_cache(self, paths: Set[str]) -> None:
        """Invalidate Cloudfront cache of specified files.

        Args:
            paths: Absolute paths to invalidate.
        """
        if _DISTRIBUTION_ID is None:
            return

        invalidation = dict(
            DistributionId=_DISTRIBUTION_ID,
            InvalidationBatch={
                "Paths": {
                    "Quantity": len(paths),
                    "Items": [f"/{path}" for path in paths],
                },
                "CallerReference": f"{int(datetime.utcnow().timestamp() * 1e3)}",
            },
        )
        async with SESSION.client("cloudfront") as cloudfront:
            while True:
                try:
                    await cloudfront.create_invalidation(**invalidation)
                    return
                except ClientError as exception:  # pragma: no cover
                    error = exception.response["Error"]
                    code = error["Code"]
                    if code == "Throttling":
                        await sleep(1)
                    elif code == "InvalidationBatchAlreadyExists" or (
                        code == "InvalidArgument"
                        and _SAME_CALLER_REFERENCE in error["Message"]
                    ):
                        return
                    else:
                        raise
