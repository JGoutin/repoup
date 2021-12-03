"""AWS Lambda entrypoint.

Use S3 as storage, Cloudfront as CDN and supports GPG key stored in SSM parameter store.

SSM parameters must be stored in a common fully qualified path defined
with SSM_PARAMETER_PATH environment variable.

The GPG key content must be passed to GPG_PRIVATE_KEY instead of the key path.

This lambda is intended to be triggered on S3 events "ObjectCreated:*" and
"ObjectRemoved:*".

By default, pass the extra variable "dir_name" to "get_repository". This variable can
be used to define repository url, for instance in RPM "BASEURL".
"""
from asyncio import get_event_loop
from os import chmod, environ
from os.path import basename, dirname, realpath
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

_loop = get_event_loop()


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

    rep.GPG_PRIVATE_KEY = key_path = realpath("gpg_key.asc")
    with open(key_path, "wt") as key:
        key.write(key_content)
    chmod(key_path, 0o600)

    try:
        rep.GPG_PASSWORD = params["GPG_PASSWORD"]
    except KeyError:
        pass


_init_gpg()
del _init_gpg


async def _async_handled(action: str, key: str) -> str:
    """Async handler.

    Args:
        action: Action to perform.
        key: S3 key to handle.

    Returns:
        Repository.
    """
    async with (await rep.get_repository(key, dir_name=basename(dirname(key)))) as repo:
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
    key = record["s3"]["object"]["key"]
    url = _loop.run_until_complete(_async_handled(action, key))
    print(f'{action.capitalize().rstrip("e")}ed package "{key}" to repository "{url}"')