"""AWS S3 storage."""
from asyncio import sleep
from contextlib import contextmanager
from datetime import datetime
from os import getenv
from posixpath import join
from typing import Generator, List

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

    async def put_object(self, relpath: str, body: bytes) -> None:
        """Put file content.

        Args:
            relpath: Relative path.
            body: File content.
        """
        await self._client.put_object(
            Bucket=self._bucket, Key=self.join(relpath), Body=body
        )

    async def get_object(self, relpath: str, absolute: bool = False) -> bytes:
        """Get file content.

        Args:
            relpath: Relative path.
            absolute: If True, use absolute path

        Returns:
            File content
        """
        src = self.join(relpath, absolute=absolute)
        with _s3_exception_handler(src):
            return await (  # type: ignore
                await self._client.get_object(Bucket=self._bucket, Key=src)
            )["Body"].read()

    async def get_file(self, relpath: str, absolute: bool = False) -> None:
        """Get file.

        Args:
            relpath: Relative path.
            absolute: If True, use absolute path
        """
        src = self.join(relpath, absolute=absolute)
        with _s3_exception_handler(src):
            await self._client.download_file(self._bucket, src, self.tmp_join(relpath))

    async def put_file(self, relpath: str) -> None:
        """Put file.

        Args:
            relpath: Relative path.
        """
        await self._client.upload_file(
            self.tmp_join(relpath), self._bucket, self.join(relpath)
        )

    async def remove(self, path: str, absolute: bool = False) -> None:
        """Remove file from storage.

        Args:
            path: Absolute path.
            absolute: If True, use absolute path
        """
        await self._client.delete_object(
            Bucket=self._bucket, Key=self.join(path, absolute=absolute)
        )

    async def invalidate_cache(self, paths: List[str]) -> None:
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
