"""Packages repositories."""
from abc import ABC, abstractmethod
from asyncio import gather, to_thread
from os import getenv
from os.path import isfile, join, splitext, dirname
from shutil import move
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional, Set, List

from repoup.lib import AsyncContext, import_component, run
from repoup.storage import get_storage

#: GPG private key used to sign packages and metadata
GPG_PRIVATE_KEY = getenv("GPG_PRIVATE_KEY", None)

#: GPG password used to sign packages and metadata
GPG_PASSWORD = getenv("GPG_PASSWORD", None)


class RepositoryBase(ABC, AsyncContext):
    """Package repository to update.

    Args:
        url: Repository storage url.
        gpg_private_key: Path to GPG private key.
        gpg_password: GPG private key password.
        gpg_verify: If True, verify signature after signing.
        gpg_clear: Clear the key from GPG after repository update.
    """

    __slots__ = [
        "_storage",
        "_gpg_public_key",
        "_gpg_key",
        "_gpg_user_id",
        "_gpg_fingerprint",
        "_gpg_password",
        "_gpg_clear",
        "_gpg_verify",
        "_url",
        "_modified_paths",
        "_to_remove_paths",
        "_to_invalidate_paths",
        "_tpm_obj",
        "_tmp",
    ]

    _GPG = (
        getenv("GPG_EXECUTABLE", "gpg"),
        "--batch",
        "--no-tty",
        "--status-fd",
        "1",
        "--yes",
        "--with-colons",
    )
    _GPG_PRESET_PASSPHRASE = None

    def __init__(
        self,
        url: str,
        *,
        gpg_private_key: Optional[str] = None,
        gpg_password: Optional[str] = None,
        gpg_verify: bool = True,
        gpg_clear: bool = False,
    ) -> None:
        super().__init__()
        self._url = url
        self._modified_paths: Set[str] = set()
        self._to_invalidate_paths: Set[str] = set()
        self._to_remove_paths: Set[str] = set()

        self._tpm_obj = TemporaryDirectory()
        self._tmp = self._tpm_obj.name

        self._gpg_key = gpg_private_key or GPG_PRIVATE_KEY
        self._gpg_password = gpg_password or GPG_PASSWORD
        self._gpg_clear = gpg_clear
        self._gpg_verify = gpg_verify

    async def __aenter__(self) -> "RepositoryBase":
        self._storage = await self._exit_stack.enter_async_context(
            get_storage(self._url)
        )
        await gather(self._gpg_init(), self._load())
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._save()
        transactions = [self._clear()]
        self._invalidate_dirs()
        if self._to_invalidate_paths:
            transactions.append(
                self._storage.invalidate_cache(self._to_invalidate_paths)
            )
        if self._gpg_key is not None and self._gpg_clear:
            transactions.append(self._gpg_clear_key())
        await gather(*transactions)
        await super().__aexit__(exc_type, exc_val, exc_tb)

    @property
    def url(self) -> str:
        """Repository URL."""
        return self._url

    @property
    def gpg_user_id(self) -> Optional[str]:
        """GPG signing key user ID."""
        try:
            return self._gpg_user_id
        except AttributeError:
            return None

    @property
    def modified(self) -> Set[str]:
        """Relative paths of files modified during the update transaction."""
        return self._modified_paths

    @property
    def removed(self) -> Set[str]:
        """Relative paths of files removed during the update transaction."""
        return self._to_remove_paths

    @abstractmethod
    async def add(
        self, path: str, remove_source: bool = True, sign: bool = True
    ) -> List[str]:
        """Add a package if not already present in the repository.

        Args:
            path: Absolute package path.
            remove_source: If True, remove the source file once moved in the repository.
            sign: If True, sign the package before adding it to the repository.

        Returns:
            Resulting package path(s) once added to the repository.
        """

    @abstractmethod
    async def remove(self, filename: str) -> None:
        """Add a package if present in the repository.

        Args:
            filename: Package filename.
        """

    @abstractmethod
    async def _load(self) -> None:
        """Load current repository if exists."""

    @abstractmethod
    async def _save(self) -> None:
        """Save updated repository."""

    async def _clear(self) -> None:
        """Remove deprecated repository files."""
        await gather(
            *(
                self._storage.remove(path)
                for path in (self._to_remove_paths - self._modified_paths)
            )
        )

    def _mark_for_deletion(self, *path: str) -> None:
        """Mark one or more files for deletion after repository update.

        Before deletion, path that are marked as "modified" are dropped from the list of
        files to delete.

        Args:
            path: Relative path.
        """
        self._to_remove_paths |= set(path)

    def _mark_as_modified(self, *path: str, invalidate: bool = True) -> None:
        """Mark one or more files as modified.

        Args:
            path: Relative path.
            invalidate: If True, mark the file for invalidation. Likely only required
                if the file name is immutable across versions of its content.
        """
        self._modified_paths |= set(path)
        if invalidate:
            self._to_invalidate_paths |= {self._storage.join(value) for value in path}

    async def _exec(
        self, *command: str, input: Optional[str] = None, check: bool = True  # noqa
    ) -> bytes:
        """Execute a command inside the repository directory.

        Args:
            *command: Command
            input: STDIN input.
            check: If True, raise on return code.

        Returns:
            STDOUT.
        """
        return await run(*command, cwd=self._storage.path, input=input, check=check)

    async def _gpg_exec(
        self,
        *command: str,
        input: Optional[str] = None,  # noqa
    ) -> bytes:
        """Execute a GPG command inside the repository directory.

        Args:
            *command: Command
            input: STDIN input.

        Returns:
            STDOUT.
        """
        try:
            return await self._exec(*self._GPG, *command, input=input)
        except FileNotFoundError:  # pragma: no cover
            raise FileNotFoundError(
                "GnuPG v2 is required. If installed, you can configure the executable "
                "path using the GPG_EXECUTABLE environment variable."
            ) from None

    @classmethod
    async def _gpg_preset_passphrase_exec(
        cls,
        key_grip: str,
        password: str,
    ) -> None:
        """Preset the passphrase of a GPG key in the GPG agent.

        Args:
            key_grip: GPG key grip.
            password: GPG key password.
        """
        if cls._GPG_PRESET_PASSPHRASE is None:
            # Try with various paths because generally not available in PATH and stored
            # in various places depending on the OS
            for candidate in (
                "/usr/lib/gnupg/gpg-preset-passphrase",
                "/usr/libexec/gpg-preset-passphrase",
                "gpg-preset-passphrase",
            ):
                if isfile(candidate):
                    cls._GPG_PRESET_PASSPHRASE = candidate
                    break
            else:  # pragma: no cover
                raise FileNotFoundError(
                    'Unable to find the "gpg-preset-passphrase" executable.'
                )

            # Try to start the GPG agent if not already started.
            await run("gpg-agent", "--daemon", "--allow-preset-passphrase", check=False)

        await run(cls._GPG_PRESET_PASSPHRASE, "--preset", key_grip, input=password)

    async def _sign_asc(self, path: str, extension: str = "asc") -> None:
        """Generate a signature file using GNUPG.

        Generates an armored detached signature and put in on storage.

        Args:
            path: Relative path of file to sign in temporary directory.
            extension: Signature file extension.
        """
        if self._gpg_key is None:
            return

        sig_path = f"{path}.{extension}"
        await self._gpg_exec(
            "--default-key",
            self._gpg_user_id,
            "--detach-sign",
            "--armor",
            "--output",
            sig_path,
            path,
        )

        if self._gpg_verify:
            await self._gpg_exec("--verify", sig_path, path)
        await self._storage.put_file(sig_path)
        self._mark_as_modified(sig_path)

    async def _sign(self, relpath: str) -> None:
        """Sign a file inplace using GNUPG.

        Args:
            relpath: Relative path of file to sign in temporary directory.
        """
        if self._gpg_key is None:
            return

        tmp = f"{relpath}.tmp"
        move(self._storage.tmp_join(relpath), self._storage.tmp_join(tmp))
        await self._gpg_exec(
            "--default-key",
            self._gpg_user_id,
            "--clearsign",
            "--armor",
            "--output",
            relpath,
            tmp,
        )
        await self._storage.remove_tmp(tmp)

        if self._gpg_verify:
            await self._gpg_exec("--verify", relpath)

    async def _gpg_init(self) -> None:
        """Initialize GPG."""
        if self._gpg_key is None:
            return

        grp = self._gpg_parse_key_info(
            await self._gpg_exec(
                "--with-keygrip",
                "--import-options",
                "show-only",
                "--import",
                self._gpg_key,
            )
        )

        if self._gpg_password is not None:
            await self._gpg_preset_passphrase_exec(grp, self._gpg_password)

        await self._gpg_exec("--import", self._gpg_key)
        public_key = await self._gpg_exec("--armor", "--export", self._gpg_user_id)

        self._gpg_public_key = join(self._tmp, f"{self._gpg_user_id}.pub")
        with open(self._gpg_public_key, "wb") as public_key_file:
            await to_thread(public_key_file.write, public_key)

    def _gpg_parse_key_info(self, gpg_output: bytes) -> str:
        """Get GPG key information.

        Args:
            gpg_output: GPG output.

        Returns:
            Key grip.
        """
        grp = uid = fpr = None
        for line in gpg_output.splitlines():
            if line.startswith(b"grp:"):
                grp = line.split(b":")[9].decode()
            elif line.startswith(b"fpr:"):
                fpr = line.split(b":")[9].decode()
            elif line.startswith(b"uid:"):
                uid = line.split(b":")[9].decode()
            if grp and uid and fpr:
                break
        else:  # pragma: no cover
            raise RuntimeError("Unable to find GPG key information.")
        self._gpg_user_id = uid
        self._gpg_fingerprint = fpr
        return grp

    async def _gpg_clear_key(self) -> None:
        """Clear the key from GPG."""
        await self._gpg_exec("--delete-secret-key", self._gpg_fingerprint)
        await self._gpg_exec("--delete-key", self._gpg_fingerprint)

    def _invalidate_dirs(self) -> None:
        """Add modified files directories to cache invalidation."""
        add = self._to_invalidate_paths.add
        to_abs = self._storage.join
        for path in self._modified_paths | self._to_remove_paths:
            path = to_abs(path)
            while "/" in path:
                path = dirname(path)
                add(f"{path}/")

    @classmethod
    @abstractmethod
    async def find_repository(cls, filename: str, **variables: str) -> Dict[str, Any]:
        """Find the repository where to store a package.

        Args:
            filename: Package filename.
            variables: Extra variables to use to determinate repository URL.

        Returns:
            Repository configuration related to this package.
        """


async def get_repository(
    filename: str, variables: Optional[Dict[str, str]] = None, **kwargs: Any
) -> RepositoryBase:
    """Get repository object to use with a package.

    Args:
        filename: Package filename.
        variables: Extra variables to use to determinate repository URL.
        kwargs: Repository keyword arguments.

    Returns:
        Repository object
    """
    repo_class = import_component("repository", splitext(filename)[1].lstrip("."))
    config = await repo_class.find_repository(filename, **(variables or dict()))
    config.update(kwargs)
    return repo_class(**config)  # type: ignore
