"""Pytest configuration."""
import os
import sys
from os.path import dirname
from typing import TYPE_CHECKING, Any, Generator, Tuple
from urllib.request import urlopen

import pytest

DIR_PATH = dirname(__file__)
sys.path.append(dirname(DIR_PATH))

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client  # noqa


try:
    import uvloop  # noqa
except ImportError:  # pragma: no cover
    pass
else:
    uvloop.install()

BUCKET = "bucket"


def is_responsive(url: str) -> bool:
    """Test if URL is responsive.

    Args:
        url: URL

    Returns:
        True if responsive.
    """
    try:
        with urlopen(url) as response:
            return response.status == 200  # type: ignore
    except ConnectionError:
        return False


@pytest.fixture(scope="session", autouse=True)
def s3_mock(docker_ip: Any, docker_services: Any) -> str:
    """Ensure that HTTP service is up and responsive."""
    url = f"http://{docker_ip}:{docker_services.port_for('s3mock', 9090)}"
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive(url)
    )
    os.environ["S3_ENDPOINT_URL"] = url
    return url


@pytest.fixture(scope="session")
def s3_client(s3_mock: str) -> "S3Client":
    """Returns S3 client to use with mocked S3.

    Args:
        s3_mock: Fixture.

    Returns:
        S3 client.
    """
    import boto3

    return boto3.client("s3", endpoint_url=s3_mock)


class StorageHelper:
    """Storage helper."""

    __slots__ = ["_client"]

    def __init__(self, client: "S3Client"):
        self._client = client
        self.clear()

    @property
    def keys(self) -> Tuple[str, ...]:
        """Keys in storage."""
        return tuple(
            entry["Key"]
            for entry in self._client.list_objects(Bucket=BUCKET).get("Contents", ())
        )

    def clear(self) -> None:
        """Clear storage content."""
        for key in self.keys:
            self._client.delete_object(Bucket=BUCKET, Key=key)

    def put(self, src: str, dst: str) -> None:
        """Put a file on storage.

        Args:
            src: Local source path.
            dst: Destination storage path.
        """
        self._client.upload_file(Filename=src, Bucket=BUCKET, Key=dst)

    def get(self, key: str) -> bytes:
        """Put a file on storage.

        Args:
            key: Storage path.
        """
        return self._client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


@pytest.fixture()
def storage_helper(s3_client: "S3Client") -> Generator[StorageHelper, None, None]:
    """Returns a storage helper to use with tests.

    Args:
        s3_client: Fixture.

    Returns:
        S3 client.
    """
    helper = StorageHelper(s3_client)
    yield helper
    helper.clear()
