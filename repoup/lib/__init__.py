"""Common libraries."""
from asyncio import create_subprocess_exec
from asyncio.subprocess import DEVNULL, PIPE, STDOUT
from contextlib import AsyncExitStack
from importlib import import_module
from typing import Any, Dict, Optional


def import_component(component: str, element: str) -> Any:
    """Import the Python module of the specified component element.

    Args:
        component: Component name.
        element: Engine name.

    Returns:
        Python module.
    """
    element = f"repoup.{component}.{element}"
    try:
        module = import_module(element)
    except ImportError:
        from importlib.util import find_spec

        if find_spec(element) is not None:  # pragma: no cover
            raise
        raise NotImplementedError(f"Unsupported {component}: {element}")
    return getattr(module, component.capitalize())


async def run(
    *command: str,
    input: Optional[str] = None,  # noqa
    cwd: Optional[str] = None,
    check: bool = True,
) -> bytes:
    """Run a command in a subprocess.

    Args:
        *command: Command
        input: STDIN input.
        cwd: Working directory.
        check: If True, raise on return code.

    Returns:
        STDOUT.
    """
    if input:
        input_bytes: Optional[bytes] = input.encode()  # noqa
        stdin = PIPE
    else:
        input_bytes = None
        stdin = DEVNULL
    process = await create_subprocess_exec(
        *command, cwd=cwd, stdin=stdin, stderr=STDOUT, stdout=PIPE
    )
    stdout = (await process.communicate(input=input_bytes))[0]
    if check and process.returncode:
        raise RuntimeError(stdout.decode(errors="ignore"))
    return stdout


def filter_none(data: Dict[Any, Any]) -> Dict[Any, Any]:
    """Return a copy of a dict without None values.

    Args:
        data: dict to filter.

    Returns:
        Filtered dict.
    """
    return {key: value for key, value in data.items() if value is not None}


class AsyncContext:
    """Async context manager with exit stack."""

    __slots__ = ["_exit_stack"]

    def __init__(self) -> None:
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self) -> Any:  # pragma: no cover
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
