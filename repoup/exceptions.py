"""Exception."""


class RepositoryException(Exception):
    """Base repository exception."""

    __slots__ = ["_status_code"]

    def __init__(self, message: str, status_code: int = 500) -> None:
        self._status_code = int(status_code)
        Exception.__init__(self, message)

    @property
    def status(self) -> int:
        """Status code.

        Returns:
            int: Status code.
        """
        return self._status_code

    @property
    def message(self) -> str:
        """Message.

        Returns:
            str: Message.
        """
        return str(self.args[0])


class PackageNotFound(RepositoryException):
    """Package not found."""

    def __init__(self, path: str) -> None:
        RepositoryException.__init__(
            self, f"Package not found: {path}", status_code=404
        )


class PackageAlreadyExists(RepositoryException):
    """Package already exists."""

    def __init__(self, path: str) -> None:
        RepositoryException.__init__(
            self, f"Package already exists: {path}", status_code=409
        )


class InvalidPackage(RepositoryException):
    """Invalid package."""

    def __init__(self, message: str) -> None:
        RepositoryException.__init__(self, message, status_code=400)
