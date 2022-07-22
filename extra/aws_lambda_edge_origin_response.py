"""Simple Cloudfront HTML repository view.

An AWS Lambda@Edge that can be used with Cloudfront as origin response to
provide a simple browsable HTML views to a package repository stored on S3.

The content of "/style/fileindex.css" is the following:
th,td {padding-right: 20px; text-align: left;}
"""
from html import escape
from typing import Any, Dict
from urllib.parse import quote

import boto3

_S3 = boto3.client("s3")
_BUCKET = "${S3_BUCKET}"
INDEX = (
    '<!DOCTYPE html><html><head><link rel="stylesheet" href="/style/fileindex.css">'
    '<meta charset="utf-8">'
    "<title>Repository - {uri}</title></head><body>"
    "<h1>Index of {uri}</h1><table>"
    "<tr><th>Name</th><th>Size</th><th>Last Modified</th></tr>"
    '<tr><td><a href="../">..</a></td><td></td><td></td></tr>'
    "{table}</table></body></html>"
)
HEADERS = {
    "content-type": [
        {
            "key": "Content-Type",
            "value": "text/html",
        }
    ],
    "strict-transport-security": [
        {
            "key": "Strict-Transport-Security",
            "value": "max-age=63072000; includeSubdomains; preload",
        }
    ],
    "x-content-type-options": [
        {
            "key": "X-Content-Type-Options",
            "value": "nosniff",
        }
    ],
    "x-frame-options": [
        {
            "key": "X-Frame-Options",
            "value": "DENY",
        }
    ],
    "x-xss-protection": [
        {
            "key": "X-XSS-Protection",
            "value": "1; mode=block",
        }
    ],
    "referrer-policy": [
        {
            "key": "Referrer-Policy",
            "value": "same-origin",
        }
    ],
    "content-security-policy": [
        {
            "key": "Content-Security-Policy",
            "value": (
                "default-src 'none'; frame-ancestors 'none'; style-src 'self'; "
                "img-src 'self'; base-uri 'none'; form-action 'none';"
            ),
        }
    ],
    "permissions-policy": [
        {
            "key": "Permissions-Policy",
            "value": (
                "geolocation=(),midi=(),sync-xhr=(),microphone=(),camera=(),"
                "magnetometer=(),gyroscope=(),fullscreen=(self),payment=()"
            ),
        }
    ],
}


def generates_index(response: Dict[str, Any], uri: str) -> None:
    """Generates HTML index and update response.

    Args:
        response: Response
        uri: Current URL
    """
    prefix = uri.lstrip("/")
    prefix_len = len(prefix)
    kwargs = dict(Bucket=_BUCKET, Prefix=prefix)

    entries: Dict[str, Dict[str, Any]] = dict()
    while True:
        resp = _S3.list_objects_v2(**kwargs)  # type: ignore
        try:
            contents = resp["Contents"]
        except KeyError:
            break

        for content in contents:
            name = content["Key"][prefix_len:]
            if not name:
                continue
            _set_entry(entries, name, content)

        token = resp.get("NextContinuationToken")
        if token:
            kwargs["ContinuationToken"] = token
            continue
        break

    if entries:
        response["body"] = INDEX.format(
            table="".join(
                f'<tr><td><a href="{escape(quote(name))}">{escape(name)}</a></td>'
                f'<td>{attrs["size"]}</td>'
                f'<td>{attrs["last_modified"].strftime("%b %d %Y %H:%M")}</td>'
                "</tr>"
                for name, attrs in entries.items()
            ),
            uri=f"/{prefix}",
        )
        response["status"] = 200
        response["statusDescription"] = "OK"
        response["headers"].update(HEADERS)


def _set_entry(entries: Dict[str, Dict[str, Any]], name: str, content: Any) -> None:
    """Set entry from S3 content.

    Args:
        entries: Entries dict.
        name: Entry name.
        content: S3 content.
    """
    last_modified = content["LastModified"]
    try:
        name, _ = name.split("/", 1)
        name += "/"
        size_repr = "-"
    except ValueError:
        size = content["Size"]
        size_repr = _get_size_repr(size)
    try:
        prev_last_modified = entries[name]["last_modified"]
    except KeyError:
        entries[name] = dict(size=size_repr, last_modified=last_modified)
    else:
        if prev_last_modified < last_modified:
            entries[name]["last_modified"] = last_modified


def _get_size_repr(size: int) -> str:
    """Get size representation with unit.

    Args:
        size: Size in bytes.

    Returns:
        Size representation.
    """
    if size < 1024:
        return f"{size} B"
    elif size < 1048576:
        return f"{size / 1024:.1f} KiB"
    elif size < 1073741824:
        return f"{size / 1048576:.1f} MiB"
    else:
        return f"{size / 1073741824:.1f} GiB"


def handler(event: Dict[str, Any], *_: Any) -> Dict[str, Any]:
    """AWS Lambda entry point.

    Args:
        event: Event information.
    """
    cf = event["Records"][0]["cf"]
    response = cf["response"]
    uri = cf["request"]["uri"]

    if response["status"] == "404" and uri.endswith("/"):
        print("Generates index for URI:", uri)
        generates_index(response, uri)

    return response  # type: ignore
