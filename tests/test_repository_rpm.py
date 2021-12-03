"""Test RPM repository."""
from gzip import decompress
from os.path import join

import pytest

from repoup.repository import get_repository, rpm
from tests.conftest import DIR_PATH, StorageHelper

PKG = "centos-stream-release-8.6-1.el8.noarch.rpm"
PKG_PATH = f"tests/data/{PKG}"
PKG_NAME = "centos-stream-release"
rpm.BASEURL = "s3://bucket/$releasever/$basearch"
REPO_URL = "s3://bucket/8/noarch"
PKG_REPO_PATH = f"8/noarch/{PKG}"


async def test_initialize_empty_repository(storage_helper: StorageHelper) -> None:
    """Test empty repository initialization."""
    from createrepo_c import NO_COMPRESSION

    repomd = "8/noarch/repodata/repomd.xml"
    async with (await get_repository(PKG)) as repo:
        pass

    assert repomd in storage_helper.keys
    assert "repodata/repomd.xml" in repo.modified

    # Test without compression
    storage_helper.clear()
    async with (
        await get_repository(
            PKG, db_compression=NO_COMPRESSION, compression=NO_COMPRESSION
        )
    ):
        pass
    assert repomd in storage_helper.keys


async def test_add_remove_package(storage_helper: StorageHelper) -> None:
    """Test repository with add/remove packages."""
    from repoup.exceptions import InvalidPackage, PackageAlreadyExists

    # Add package
    storage_helper.put(PKG_PATH, PKG_PATH)
    async with (await get_repository(PKG_PATH)) as repo:
        await repo.add(PKG_PATH)

    content = storage_helper.keys
    assert PKG_REPO_PATH in content
    assert PKG not in content

    for key in content:
        for record_type in ("-primary.xml", "-other.xml", "-filelists.xml"):
            if record_type in key:
                assert PKG_NAME in decompress(storage_helper.get(key)).decode()

    # Add already existing package
    storage_helper.put(PKG_PATH, PKG_PATH)
    async with (await get_repository(PKG_PATH)) as repo:
        with pytest.raises(PackageAlreadyExists):
            await repo.add(PKG_PATH)
    assert PKG_REPO_PATH in storage_helper.keys

    # Add already existing package, inplace
    async with (await get_repository(PKG)) as repo:
        with pytest.raises(PackageAlreadyExists):
            await repo.add(PKG_REPO_PATH)
    assert PKG_REPO_PATH in storage_helper.keys

    # Remove package
    async with (await get_repository(PKG)) as repo:
        await repo.remove(PKG)
        assert PKG in repo.removed

    content = storage_helper.keys
    assert PKG_REPO_PATH not in content

    for key in content:
        for record_type in ("-primary.xml", "-other.xml", "-filelists.xml"):
            if record_type in key:
                assert PKG_NAME not in decompress(storage_helper.get(key)).decode()

    # Add package, inplace
    storage_helper.put(PKG_PATH, PKG_REPO_PATH)
    async with (await get_repository(PKG)) as repo:
        await repo.add(PKG_REPO_PATH)
    assert PKG_REPO_PATH in storage_helper.keys

    async with (await get_repository(PKG)) as repo:
        await repo.remove(PKG)

    # Add package, but keep source file
    storage_helper.put(PKG_PATH, PKG)
    async with (await get_repository(PKG)) as repo:
        await repo.add(PKG, remove_source=False)

    assert PKG in storage_helper.keys

    # Do nothing
    async with (await get_repository(PKG)):
        pass

    # Test package name not matching NVRA
    bad_pkg_path = f"tests/data/os{PKG}"
    storage_helper.put(PKG_PATH, bad_pkg_path)
    async with (await get_repository(PKG)) as repo:
        with pytest.raises(InvalidPackage):
            await repo.add(bad_pkg_path)
    assert PKG_REPO_PATH in storage_helper.keys


async def test_add_sign_package(storage_helper: StorageHelper) -> None:
    """Test Add and sign a package."""
    storage_helper.put(PKG_PATH, PKG)
    async with (
        await get_repository(
            PKG, gpg_private_key=join(DIR_PATH, "data/gpg_key.asc"), gpg_clear=True
        )
    ) as repo:
        await repo.add(PKG)

    content = storage_helper.keys
    assert PKG_REPO_PATH in content
    assert "8/noarch/repodata/repomd.xml.asc" in content


async def test_add_sign_verify_package(storage_helper: StorageHelper) -> None:
    """Test Add, sign and verify a package."""
    storage_helper.put(PKG_PATH, PKG)
    try:
        async with (
            await get_repository(
                PKG,
                gpg_private_key=join(DIR_PATH, "data/gpg_key.asc"),
                gpg_clear=True,
                gpg_verify=True,
            )
        ) as repo:
            await repo.add(PKG)
    except RuntimeError as exception:
        if "(Permission denied)" in str(exception):
            pytest.skip("Must be called as root to add import RPM signing key.")
        raise


async def test_find_repository() -> None:
    """Test find repository from package name."""
    from repoup.exceptions import InvalidPackage

    BASEURL = rpm.BASEURL
    find_repository = rpm.Repository.find_repository
    try:
        # Test valid package name
        assert (await find_repository(PKG))["url"] == REPO_URL

        # Test no BASEURL
        rpm.BASEURL = None
        with pytest.raises(ValueError):
            await find_repository(PKG)
        rpm.BASEURL = BASEURL

        # Test invalid package name
        with pytest.raises(InvalidPackage):
            await find_repository("centos-stream-release.rpm")

        # Test Package without "dist" but with "$releasever" in BASEURL
        with pytest.raises(InvalidPackage):
            await find_repository("centos-stream-release-8.6-1.noarch.rpm")

        # Test Package without "dist" but without "$releasever" in BASEURL
        rpm.BASEURL = "s3://bucket/$basearch"
        assert (await find_repository("centos-stream-release-8.6-1.noarch.rpm"))[
            "url"
        ] == "s3://bucket/noarch"

        # Test BASEURL with extra (Non RPM) variable
        rpm.BASEURL = "s3://bucket/$channel/$basearch"
        assert (await find_repository(PKG, channel="stable"))[
            "url"
        ] == "s3://bucket/stable/noarch"
        rpm.BASEURL = BASEURL

    finally:
        rpm.BASEURL = BASEURL
