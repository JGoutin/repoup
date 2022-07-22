"""Test repository base features."""
from os.path import isfile, join
from typing import Dict, List

import pytest

from repoup.repository import RepositoryBase
from tests.conftest import DIR_PATH


class MockRepository(RepositoryBase):
    """Test Repository."""

    async def add(
        self, path: str, remove_source: bool = True, sign: bool = True
    ) -> List[str]:
        """Add a package if not already present in the repository.

        Args:
            path: Absolute package path.
            remove_source: If True, remove the source file once moved in the repository.
            sign: If True, sign the package before adding it to the repository.

        Returns:
            Resulting package path once added to the repository.
        """
        return []

    async def remove(self, filename: str) -> None:
        """Add a package if present in the repository.

        Args:
            filename: Package filename.
        """

    async def _load(self) -> None:
        """Load current repository if exists."""

    async def _save(self) -> None:
        """Save updated repository."""

    @classmethod
    async def find_repository(cls, filename: str, **variables: str) -> Dict[str, str]:
        """Find the repository where to store a package.

        Args:
            filename: Package filename.
            variables: Extra variables to use to determinate repository URL.

        Returns:
            Path of the repository related to this package.
        """
        return dict(url="")


async def test_repository_no_gpg() -> None:
    """Test repository without GPG key."""
    url = "s3://bucket/key"
    rel_path = "file"

    async with MockRepository(url) as repo:
        assert repo.url == url
        assert repo.gpg_user_id is None

        path = repo._storage.tmp_join(rel_path)
        with open(path, "wb") as file:
            file.write(b"test")
        await repo._sign_asc(rel_path)
        assert not isfile(repo._storage.tmp_join(f"{rel_path}.asc"))


async def test_repository_gpg_no_password() -> None:
    """Test repository with GPG key but without password."""
    rel_path = "file"
    sig_path = f"{rel_path}.asc"
    key_path = join(DIR_PATH, "data/gpg_key.asc")

    async with MockRepository(
        "s3://bucket/key", gpg_private_key=key_path, gpg_verify=True, gpg_clear=True
    ) as repo:
        assert repo.gpg_user_id

        path = repo._storage.tmp_join(rel_path)
        with open(path, "wb") as file:
            file.write(b"test")
        await repo._sign_asc(rel_path)

        abs_sig_path = repo._storage.tmp_join(sig_path)
        with open(abs_sig_path, "rb") as file:
            assert await repo._storage.get_object(sig_path) == file.read()


async def test_repository_gpg_with_password() -> None:
    """Test repository with GPG key but without password."""
    rel_path = "file"
    key_path = join(DIR_PATH, "data/gpg_key_with_password.asc")

    try:
        async with MockRepository(
            "s3://bucket/key",
            gpg_private_key=key_path,
            gpg_password="password",
            gpg_verify=True,
            gpg_clear=True,
        ) as repo:
            path = repo._storage.tmp_join(rel_path)
            with open(path, "wb") as file:
                file.write(b"test")
                await repo._sign_asc(rel_path)

            assert isfile(repo._storage.tmp_join(f"{rel_path}.asc"))

    except RuntimeError as exception:
        if "caching passphrase failed: Not supported" in str(exception):
            pytest.skip(
                "Can only be tested on host with GPG agent not already "
                "started or started with the '--allow-preset-passphrase' option."
            )


async def test_repository_not_exists() -> None:
    """Test non-existing repository type."""
    from repoup.repository import get_repository

    with pytest.raises(NotImplementedError):
        await get_repository("notexists://path")
