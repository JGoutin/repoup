"""AWS Lambda entrypoint.

Use S3 as storage, Cloudfront as CDN and supports GPG key stored in SSM parameter store.

SSM parameters must be stored in a common fully qualified path defined
with SSM_PARAMETER_PATH environment variable.

The GPG key content must be passed to GPG_PRIVATE_KEY instead of the key path.

This lambda is intended to be triggered on S3 events "ObjectCreated:*" and
"ObjectRemoved:*".
"""
from asyncio import get_event_loop
from os import close, environ, write
from os.path import basename
from tempfile import mkdtemp, mkstemp
from typing import Any, Dict

from boto3 import client

import repoup.repository as rep
import repoup.storage.s3  # noqa

try:
    import uvloop  # noqa
except ImportError:  # pragma: no cover
    pass
else:
    uvloop.install()

LOOP = get_event_loop()


def _init_gpg() -> None:
    """Get GPG key and password from SSM parameter store."""
    try:
        ssm_path = environ["SSM_PARAMETER_PATH"]
    except KeyError:
        return
    params = {
        param["Name"].rsplit("/", 1)[1]: param["Value"]
        for param in client("ssm").get_parameters_by_path(
            Path=ssm_path, WithDecryption=True
        )["Parameters"]
    }

    try:
        key_content = params["GPG_PRIVATE_KEY"]
    except KeyError:
        return

    environ["GNUPGHOME"] = gpg_dir = mkdtemp(prefix=".gnupg-")
    fd, key_path = mkstemp(prefix="key-", suffix=".asc", dir=gpg_dir)
    try:
        write(fd, key_content.encode())
    finally:
        close(fd)
    rep.GPG_PRIVATE_KEY = key_path

    try:
        rep.GPG_PASSWORD = params["GPG_PASSWORD"]
    except KeyError:
        pass


_init_gpg()
del _init_gpg


async def _async_handler(action: str, bucket: str, key: str) -> str:
    """Async handler.

    Args:
        action: Action to perform.
        bucket: S3 bucket.
        key: S3 key to handle.

    Returns:
        Repository.
    """
    async with (await rep.get_repository(f"s3://{bucket}/{key}")) as repo:
        await getattr(repo, action)(key)
        return repo.url


def handler(event: Dict[str, Any], _: Any) -> None:
    """AWS Lambda entry point.

    Args:
        event: Event information.
        _: AWS lambda context.
    """
    record = event["Records"][0]
    event_name = record["eventName"]
    if event_name.startswith("ObjectCreated:"):
        action = "add"
    elif event_name.startswith("ObjectRemoved:"):
        action = "remove"
    else:
        return print(f"Ignoring unsupported event: {event_name}")
    s3 = record["s3"]
    key = s3["object"]["key"]
    url = LOOP.run_until_complete(_async_handler(action, s3["bucket"]["name"], key))
    print(
        f'{action.capitalize().rstrip("e")}ed package "{basename(key)}" '
        f'to repository "{url}"'
    )
