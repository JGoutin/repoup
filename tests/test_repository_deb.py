"""Test DEB repository."""
from os.path import dirname, join
import pytest
from conftest import StorageHelper, DIR_PATH

REPO_URL = "s3://bucket/repo"
PKG = "base-files_11.1+deb11u3_amd64.deb"
PKG_NAME = "base-files"
PKG_PATH = f"tests/data/{PKG}"
PKG_REPO_PATH = f"repo/pool/main/{PKG_NAME[0]}/{PKG_NAME}/{PKG}"
PKG2 = "libpam-chroot_0.9-5_amd64.deb"
PKG_PATH2 = f"tests/data/{PKG2}"
PKG_REPO_PATH2 = f"repo/pool/main/{PKG2[:4]}/{PKG2.split('_', 1)[0]}/{PKG2}"


async def test_initialize_empty_repository(storage_helper: StorageHelper) -> None:
    """Test empty repository initialization."""
    from repoup.repository import deb, get_repository

    deb.CONFIG.clear()
    deb.CONFIG["component"] = "main"
    deb.CONFIG["suite"] = "stable"
    deb.CONFIG["codename"] = "bullseye"
    deb.CONFIG["url"] = REPO_URL

    async with (await get_repository(PKG)):
        pass
    content = storage_helper.keys

    assert "repo/dists/bullseye/InRelease" in content
    assert "repo/dists/bullseye/Release" in content
    assert "repo/dists/bullseye/main/binary-amd64/Release" in content
    inrelease = storage_helper.get("repo/dists/bullseye/InRelease")
    assert inrelease == storage_helper.get("repo/dists/bullseye/Release")
    release = inrelease.decode()
    assert "Suite: stable\n" in release
    assert "Codename: bullseye\n" in release
    assert "Components: main\n" in release
    assert "Architectures: amd64\n" in release
    assert "Acquire-By-Hash: yes\n" in release
    assert "Date: " in release

    # Add support for more architecture and component
    deb.CONFIG["component"] = "testing"
    async with (await get_repository(PKG.replace("amd64", "arm64"))):
        pass
    content = storage_helper.keys

    assert "repo/dists/bullseye/InRelease" in content
    assert "repo/dists/bullseye/Release" in content
    assert "repo/dists/bullseye/main/binary-amd64/Release" in content
    assert "repo/dists/bullseye/testing/binary-arm64/Release" in content
    inrelease = storage_helper.get("repo/dists/bullseye/InRelease")
    assert inrelease == storage_helper.get("repo/dists/bullseye/Release")
    release = inrelease.decode()
    assert "Suite: stable\n" in release
    assert "Codename: bullseye\n" in release
    assert "Components: main testing\n" in release
    assert "Architectures: amd64 arm64\n" in release


async def test_find_repository() -> None:
    """Test find repository from package name."""
    from repoup.exceptions import InvalidPackage
    from repoup.repository import deb

    find_repository = deb.Repository.find_repository
    deb_config = deb.CONFIG.copy()
    deb.CONFIG.clear()
    deb.CONFIG["component"] = "main"
    deb.CONFIG["suite"] = "stable"
    deb.CONFIG["codename"] = "stable"
    try:
        pkg_name = "my-package_1.0.0-1~bullseye_amd64.deb"

        # Test no "url" in CONFIG
        with pytest.raises(ValueError):
            await find_repository(pkg_name)
        deb.CONFIG["url"] = REPO_URL

        # Test no "codename" or "suite" in CONFIG
        with pytest.raises(ValueError):
            deb.Repository(url=REPO_URL, architecture="amd64,", component="core")

        # Test valid package name
        config = await find_repository(pkg_name)
        assert config["url"] == REPO_URL
        assert config["architecture"] == "amd64"
        assert config["component"] == "main"
        assert config["suite"] == "stable"

        # Test valid package name with "codename" auto-detection
        del deb.CONFIG["codename"]
        config = await find_repository(pkg_name)
        assert config["url"] == REPO_URL
        assert config["architecture"] == "amd64"
        assert config["codename"] == "bullseye"
        assert config["component"] == "main"

        config = await find_repository(pkg_name.replace("~", "+"))
        assert config["url"] == REPO_URL.replace("~", "+")
        assert config["architecture"] == "amd64"
        assert config["codename"] == "bullseye"
        assert config["component"] == "main"

        # Test invalid package name with "codename" auto-detection
        with pytest.raises(InvalidPackage):
            await find_repository("my-package_1.0.0-1~bullseye.deb")

        # Test missing revision in package name with "codename" auto-detection
        with pytest.raises(InvalidPackage):
            await find_repository("my-package_1.0.0_amd64.deb")

        # Test missing codename in package name with "codename" auto-detection
        with pytest.raises(InvalidPackage):
            await find_repository("my-package_1.0.0-1_amd64.deb")

        # Test template in config
        deb.CONFIG["suite"] = "$architecture"
        deb.CONFIG["codename"] = "$dist"
        config = await find_repository(pkg_name, dist="buster")
        assert config["url"] == REPO_URL
        assert config["architecture"] == "amd64"
        assert config["codename"] == "buster"
        assert config["component"] == "main"
        assert config["suite"] == config["architecture"]

    finally:
        deb.CONFIG = deb_config


async def test_add_remove_package(storage_helper: StorageHelper) -> None:
    """Test repository with add/remove packages."""
    from hashlib import new as new_hash
    from gzip import decompress as gz_decompress
    from lzma import decompress as xz_decompress
    from debian.deb822 import Packages, Release

    from repoup.exceptions import PackageAlreadyExists
    from repoup.repository import get_repository, deb

    deb_config = deb.CONFIG.copy()
    deb.CONFIG.clear()
    deb.CONFIG["component"] = "main"
    deb.CONFIG["suite"] = "stable"
    deb.CONFIG["codename"] = "stable"
    deb.CONFIG["url"] = REPO_URL
    try:
        storage_helper.put(PKG_PATH, PKG_PATH)
        storage_helper.put(PKG_PATH2, PKG_REPO_PATH2)
        async with (await get_repository(PKG_PATH)) as repo:
            await repo.add(PKG_PATH)
            await repo.add(PKG_REPO_PATH2)
        by_hash_paths = [f"repo/{path}" for path in repo.modified if "by-hash" in path]

        content = storage_helper.keys

        # Check package is moved inside pool
        assert PKG_REPO_PATH in content
        assert PKG not in content
        assert PKG_REPO_PATH2 in content
        for path in sorted(by_hash_paths):
            assert path in content

        # Check "Release" index
        inrelease = storage_helper.get("repo/dists/stable/InRelease")
        assert inrelease == storage_helper.get("repo/dists/stable/Release")
        release = Release(inrelease)
        for field in ("MD5Sum", "SHA256"):
            field_low = field.lower()
            algo = "md5" if field == "MD5Sum" else field_low
            for entry in release[field]:
                name = entry["name"]
                by_name = f"repo/dists/stable/{name}"
                assert by_name in content
                digest = entry[field_low]
                by_hash = f"repo/dists/stable/{dirname(name)}/by-hash/{field}/{digest}"
                assert by_hash in by_hash_paths
                assert by_hash in content
                index_content = storage_helper.get(by_hash)
                assert index_content == storage_helper.get(by_name)
                assert len(index_content) == int(entry["size"])
                assert new_hash(algo, index_content).hexdigest() == digest

        # Check "Packages" index
        pkg = storage_helper.get(PKG_REPO_PATH)
        pkgs = gz_decompress(
            storage_helper.get("repo/dists/stable/main/binary-amd64/Packages.gz")
        )
        assert pkgs == xz_decompress(
            storage_helper.get("repo/dists/stable/main/binary-amd64/Packages.xz")
        )
        assert pkgs == storage_helper.get(
            "repo/dists/stable/main/binary-amd64/Packages"
        )
        packages = pkgs.decode()
        assert packages.startswith("Package: ")
        assert "Package: base-files\n" in packages
        assert "Package: libpam-chroot\n" in packages
        assert "Version: 11.1+deb11u3\n" in packages
        assert "Architecture: amd64\n" in packages
        assert f"Filename: pool/main/{PKG_NAME[0]}/{PKG_NAME}/{PKG}\n" in packages
        assert f"MD5Sum: {new_hash('md5', pkg).hexdigest()}\n" in packages
        assert f"SHA256: {new_hash('sha256', pkg).hexdigest()}\n" in packages
        assert "Size: " in packages
        assert Packages(pkgs), "Check DEB822 parsable"

        # Check "Contents" index
        contents = gz_decompress(
            storage_helper.get("repo/dists/stable/main/Contents-amd64.gz")
        ).decode()
        assert "usr/lib/os-release admin/base-files\n" in contents
        assert "usr/share/base-files/motd admin/base-files\n" in contents
        assert "lib/x86_64-linux-gnu/pam_chroot.so devel/libpam-chroot\n" in contents

        # Add already existing package
        storage_helper.put(PKG_PATH, PKG_PATH)
        async with (await get_repository(PKG_PATH)) as repo:
            with pytest.raises(PackageAlreadyExists):
                await repo.add(PKG_PATH)
        content = storage_helper.keys
        assert PKG_REPO_PATH in content
        assert PKG_PATH not in content

        # Add already existing package, inplace
        async with (await get_repository(PKG_PATH)) as repo:
            with pytest.raises(PackageAlreadyExists):
                await repo.add(PKG_REPO_PATH)
        content = storage_helper.keys
        assert PKG_REPO_PATH in content

        # Remove package
        async with (await get_repository(PKG_PATH)) as repo:
            await repo.remove(PKG_PATH)

        content = storage_helper.keys

        assert PKG_REPO_PATH not in content
        assert PKG_REPO_PATH2 in content
        for path in by_hash_paths:
            # Check previous "by-hash" files are removed
            assert path not in content

        packages = gz_decompress(
            storage_helper.get("repo/dists/stable/main/binary-amd64/Packages.gz")
        ).decode()
        assert "Package: base-files\n" not in packages
        assert "Package: libpam-chroot\n" in packages

        contents = gz_decompress(
            storage_helper.get("repo/dists/stable/main/Contents-amd64.gz")
        ).decode()
        assert "etc/os-release admin/base-files\n" not in contents
        assert "usr/share/base-files/motd admin/base-files\n" not in contents
        assert "lib/x86_64-linux-gnu/pam_chroot.so devel/libpam-chroot\n" in contents

        # Add package with another new architecture
        path = PKG_PATH.replace("amd64", "arm64")
        storage_helper.put(path, path)
        async with (await get_repository(path)) as repo:
            await repo.add(path)

        content = storage_helper.keys
        assert "repo/dists/stable/main/binary-amd64/Packages.gz" in content
        assert "repo/dists/stable/main/Contents-amd64.gz" in content
        assert "repo/dists/stable/main/binary-arm64/Packages.gz" in content
        assert "repo/dists/stable/main/Contents-arm64.gz" in content

    finally:
        deb.CONFIG = deb_config


async def test_add_sign_package(storage_helper: StorageHelper) -> None:
    """Test Add and sign a package."""
    from repoup.repository import get_repository, deb

    deb_config = deb.CONFIG.copy()
    deb.CONFIG.clear()
    deb.CONFIG["component"] = "main"
    deb.CONFIG["suite"] = "stable"
    deb.CONFIG["codename"] = "stable"
    deb.CONFIG["url"] = REPO_URL
    try:
        storage_helper.put(PKG_PATH, PKG_PATH)
        async with (
            await get_repository(
                PKG_PATH,
                gpg_private_key=join(DIR_PATH, "data/gpg_key.asc"),
                gpg_clear=True,
                gpg_verify=True,
            )
        ) as repo:
            await repo.add(PKG_PATH)

        content = storage_helper.keys
        assert PKG_REPO_PATH in content
        assert "repo/dists/stable/Release.gpg" in content
        release = storage_helper.get("repo/dists/stable/InRelease").decode()
        assert "Signed-By:" in release
        assert "Suite: stable\n" in release
        assert "-----BEGIN PGP SIGNATURE-----" in release

    finally:
        deb.CONFIG = deb_config


def test_package_control_handling() -> None:
    """Test various package "control" handling."""
    from hashlib import md5
    from debian.debfile import DebFile
    from repoup.repository.deb import Repository
    from repoup.exceptions import InvalidPackage

    with DebFile(PKG_PATH) as file:
        control = file.debcontrol()
    parsed = Repository._parse_pkg_name(PKG_PATH)

    # _check_pkg: Check if control match with filename
    Repository._check_pkg(parsed, control)

    bad_parsed = parsed.copy()
    bad_parsed["version"] += "~test"
    with pytest.raises(InvalidPackage):
        Repository._check_pkg(bad_parsed, control)

    # _hash_description: Add description hash
    test_control = control.copy()
    assert "Description" in test_control
    assert "Description-md5" not in test_control
    Repository._hash_description(test_control)
    assert (
        test_control["Description-md5"]
        == md5(test_control["Description"].encode()).hexdigest()
    )

    test_control = control.copy()
    del test_control["Description"]
    assert "Description" not in test_control
    assert "Description-md5" not in test_control
    Repository._hash_description(test_control)
    assert "Description" not in test_control
    assert "Description-md5" not in test_control


def test_contents_provided_by_multiple_packages() -> None:
    """Tests case of files provided by multiple packages."""
    from copy import deepcopy
    from repoup.repository.deb import Repository

    contents = [
        ("usr/lib/os-release", ["admin/base-files"]),
        ("usr/share/base-files/dot.bashrc", ["admin/base-files"]),
        ("usr/share/base-files/dot.profile", ["admin/base-files"]),
        ("usr/share/base-files/motd", ["admin/base-files"]),
    ]
    initial_contents = deepcopy(contents)

    # Add new package content
    name = "test/pkg"
    files = ["usr/share/base-files/dot.profile", "usr/share/test"]
    assert Repository._update_contents_entries(name, files.copy(), contents)

    assert contents == [
        ("usr/lib/os-release", ["admin/base-files"]),
        ("usr/share/base-files/dot.bashrc", ["admin/base-files"]),
        ("usr/share/base-files/dot.profile", ["admin/base-files", name]),
        ("usr/share/base-files/motd", ["admin/base-files"]),
        ("usr/share/test", [name]),
    ]

    # Update existing package without change
    assert not Repository._update_contents_entries(name, files.copy(), contents)

    # Remove package
    assert Repository._update_contents_entries(name, [], contents)
    assert contents == initial_contents

    # Remove package that is already not in contents
    assert not Repository._update_contents_entries(name, [], contents)
