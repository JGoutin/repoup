"""Cloudfront origin-request Lambda@Edge.

This example rewrites RPM repositories URL to ensure OS variants releasever like
"7Server", "7.5.1804" go to the generic "7" releasever repository.
"""
import re
from typing import Any, Dict

# Match RPM repositories with BASEURL like "/<directory>/$releasever/$basearch"" with
# "<directory>" is a directory that can have multiple names like "stable", "testing".
# This regex should be adapted to match the real BASEURL of the target server.
_RHEL_RPM = re.compile(r"(?P<repo>^/[^/]+/[\d_]+)[^/]*")
match = _RHEL_RPM.match


def run(event: Dict[str, Any], _: Any) -> Any:
    """Lambda entry point.

    Args:
        event (dict): Event information.
        _: AWS lambda context.
    """
    request = event["Records"][0]["cf"]["request"]
    uri = request["uri"]
    print("Request URI:", uri)

    rhel_match = match(uri)
    if rhel_match:
        request["uri"] = uri = rhel_match.group("repo") + uri[rhel_match.end() :]
        print(f'Replacing request URI by "{uri}"')

    return request
