"""Update an DEB repository to add or remove packages."""

from asyncio import gather, to_thread
from bz2 import BZ2File
from contextlib import ExitStack
from datetime import datetime, timezone
from email.utils import format_datetime
from gzip import GzipFile
from hashlib import new as new_hash
from io import TextIOWrapper
from json import loads
from lzma import LZMAFile
from os import environ, getenv, makedirs
from os.path import basename, dirname, getsize, relpath, splitext
from re import compile
from shutil import copyfileobj
from string import Template
from typing import IO, Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from debian.deb822 import Deb822, Packages, Release
from debian.debfile import DebFile, DebError

try:
    import apt_pkg  # noqa

except (ImportError, AttributeError):  # pragma: no cover
    _USE_APT_PKG = False
else:  # pragma: no cover
    _USE_APT_PKG = True

from repoup.exceptions import InvalidPackage, PackageAlreadyExists, PackageNotFound
from repoup.lib import filter_none
from repoup.repository import RepositoryBase

_COMPRESSION = dict(xz=LZMAFile, gz=GzipFile, bz2=BZ2File, lzma=LZMAFile)
_HASH_ALGORITHMS = ("sha512", "sha256", "sha1", "md5")

#: The "Origin" field value of repository metadata
ORIGIN = getenv("DEB_ORIGIN", None)

#: The "Label" field value of repository metadata
LABEL = getenv("DEB_LABEL", None)

#: The "Description" field value of repository metadata
DESCRIPTION = getenv("DEB_DESCRIPTION", None)

#: Default "Component" to use when not specified in URL
DEFAULT_COMPONENT = getenv("DEB_DEFAULT_COMPONENT", None)

#: Comma separated default index file hash algorithms to use (md5, sha1, sha256, sha512)
DEFAULT_INDEX_HASH_ALGORITHMS = getenv(
    "DEB_DEFAULT_INDEX_HASH_ALGORITHMS", "md5,sha256"
).split(",")

#: Comma separated list of "Packages" index file compressions
DEFAULT_PACKAGES_COMPRESSION = getenv("DEFAULT_PACKAGES_COMPRESSION", "gz,xz").split(
    ","
)

#: Comma separated list of "Contents" index file compressions
DEFAULT_CONTENTS_COMPRESSION = getenv("DEFAULT_CONTENTS_COMPRESSION", "gz").split(",")

#: Repository config. The environment variable must be JSON formatted.
CONFIG = dict(
    component="main",
)
try:
    CONFIG.update(loads(environ["DEB_CONFIG"]))
except KeyError:
    pass

_NAME = compile(
    r"^(.*/)?(?P<package>[0-9a-z.+-]+)_"
    r"((?P<epoch>\d+):)?(?P<version>[\da-z.+~-]+?)(-(?P<revision>[0-9a-z.+~]+))?"
    r"_(?P<architecture>.+)\.deb$"
)
_REVISION = compile(r"^[0-9a-z.+~]+?([~+](?P<codename>[a-z]+))?$")


class Repository(RepositoryBase):
    """DEB repository to update.

    Args:
        url: Repository storage URL.
        gpg_private_key: Path to GPG private key.
        gpg_password: GPG private key password.
        gpg_verify: If True, verify signature after signing.
        gpg_clear: Clear the key from GPG after repository update.
        architecture: The Architecture of the repository to update.
        component: The Component of the repository to update.
        codename: The Codename of the repository to update.
        suite: The Suite of the repository to update.
        index_hash_algorithms: Algorithms used to hash index files.
    """

    __slots__ = [
        "_codename",
        "_architecture",
        "_component",
        "_suite",
        "_path_pool",
        "_path_dist",
        "_path_dist_component",
        "_path_dist_arch",
        "_path_inrelease",
        "_path_packages",
        "_path_contents",
        "_release",
        "_index_hash_algorithms",
        "_require_legacy_arch_release",
        "_path_legacy_arch_release",
        "_path_legacy_release",
        "_pkgs",
        "_contents",
        "_previous_indexes_hashes",
        "_updated_indexes",
    ]

    def __init__(
        self,
        url: str,
        *,
        gpg_private_key: Optional[str] = None,
        gpg_password: Optional[str] = None,
        gpg_verify: bool = False,
        gpg_clear: bool = False,
        architecture: str,
        component: str,
        codename: Optional[str] = None,
        suite: Optional[str] = None,
        index_hash_algorithms: Optional[Sequence[str]] = None,
    ) -> None:
        if (codename or suite) is None:
            raise ValueError('"codename" or "suite" required.')
        super().__init__(
            url,
            gpg_private_key=gpg_private_key,
            gpg_password=gpg_password,
            gpg_verify=gpg_verify,
            gpg_clear=gpg_clear,
        )
        self._codename = codename
        self._architecture = architecture
        self._component = component
        self._suite = suite
        self._path_pool = f"pool/{component}"
        self._path_dist = f"dists/{codename or suite}"
        self._path_inrelease = f"{self._path_dist}/InRelease"
        self._path_dist_component = f"{self._path_dist}/{component}"
        self._path_contents = f"{self._path_dist_component}/Contents-{architecture}"
        self._path_dist_arch = f"{self._path_dist_component}/binary-{architecture}"
        self._path_packages = f"{self._path_dist_arch}/Packages"
        self._index_hash_algorithms = (
            index_hash_algorithms or DEFAULT_INDEX_HASH_ALGORITHMS
        )
        self._path_legacy_release = f"{self._path_dist}/Release"
        self._path_legacy_arch_release = f"{self._path_dist_arch}/Release"
        self._require_legacy_arch_release = True
        self._pkgs: Dict[str, Deb822] = dict()
        self._contents: List[Tuple[str, List[str]]] = list()
        self._previous_indexes_hashes: Dict[str, Set[str]] = dict()
        self._updated_indexes: Set[str] = set()

    async def add(
        self, path: str, remove_source: bool = True, sign: bool = True
    ) -> str:
        """Add a package if not already present in the repository.

        Args:
            path: Absolute package path.
            remove_source: If True, remove the source file once moved in the repository.
            sign: If True, sign the package before adding it to the repository.

        Returns:
            Resulting package path once added to the repository.
        """
        filename = basename(path)
        dst_path = self._get_pool_path(filename)
        parsed_fields = self._parse_pkg_name(filename)

        await self._check_conflicts(path, dst_path, parsed_fields)

        await self._storage.get_file(path, dst=dst_path, absolute=True)
        tmp_path = self._storage.tmp_join(dst_path)

        try:
            with DebFile(tmp_path) as file:
                pkg_files = [pkg_file.decode() for pkg_file in file.md5sums()]
                pkg = file.debcontrol()
        except DebError as error:
            raise InvalidPackage(str(error))

        self._check_pkg(parsed_fields, pkg)
        signed = await self._sign_pkg(tmp_path, sign)

        name = pkg["Package"]
        pkg["Filename"] = dst_path
        pkg["Size"] = str(getsize(tmp_path))
        pkg.update(await self._hash_file(tmp_path))
        self._hash_description(pkg)

        self._update_packages(name, pkg)
        self._update_contents(name, pkg.get("Section"), pkg_files)

        transactions = []
        if signed or path != dst_path:
            transactions.append(self._storage.put_file(dst_path))
        if remove_source and path != self._storage.join(dst_path):
            transactions.append(self._storage.remove(path, absolute=True))
        await gather(*transactions)
        await self._storage.remove_tmp(filename)

        self._mark_as_modified(dst_path, invalidate=False)
        return dst_path

    async def remove(self, filename: str) -> None:
        """Remove a package if present in the repository.

        Args:
            filename: Package filename.
        """
        filename = basename(filename)
        fields = self._parse_pkg_name(filename)
        name = fields["package"]
        try:
            pkg = self._pkgs[name]
        except KeyError:  # pragma: no cover
            pass  # Already removed
        else:
            try:
                self._check_pkg(fields, pkg)
            except InvalidPackage:
                pass  # Not the version registered in indexes
            else:
                self._remove_from_packages(name)
                self._update_contents(name, pkg.get("Section"))
        self._mark_for_deletion(self._get_pool_path(filename))

    async def _load(self) -> None:
        """Load current repository data if exists."""
        await self._load_inrelease()
        await gather(self._load_packages(), self._load_contents())

    async def _save(self) -> None:
        """Save updated repository data."""
        self._clear_indexes()
        await gather(self._save_packages(), self._save_contents())
        await self._save_inrelease()

    async def _load_inrelease(self) -> None:
        """Load or initialize InRelease file."""
        makedirs(self._storage.tmp_join(self._path_dist), exist_ok=True)
        try:
            await self._storage.get_file(self._path_inrelease)
        except PackageNotFound:
            self._release = Release(
                filter_none(
                    {
                        "Description": DESCRIPTION,
                        "Origin": ORIGIN,
                        "Label": LABEL,
                        "Suite": self._suite,
                        "Codename": self._codename,
                        "Components": self._component,
                        "Architectures": self._architecture,
                        "Acquire-By-Hash": "yes",
                    }
                )
            )
        else:
            with open(self._storage.tmp_join(self._path_inrelease), "rt") as file:
                self._release = Release(file)
            self._ensure_field(self._release, "Components", self._component)
            self._ensure_field(self._release, "Architectures", self._architecture)
            self._release["Acquire-By-Hash"] = "yes"
            self._require_legacy_arch_release = not (
                await self._storage.exists(self._path_legacy_arch_release)
            )

    async def _save_inrelease(self) -> None:
        """Save the InRelease and Release indexes."""
        self._release["Date"] = format_datetime(datetime.now(timezone.utc))
        if self._gpg_key:
            self._release["Signed-By"] = self._gpg_fingerprint

        release_paths = [self._path_inrelease, self._path_legacy_release]
        release = self._release.dump()
        for path in release_paths:
            with open(self._storage.tmp_join(path), "wt") as file:
                file.write(release)

        if self._require_legacy_arch_release:
            release_paths.append(self._path_legacy_arch_release)
            makedirs(self._storage.tmp_join(self._path_dist_arch), exist_ok=True)
            with open(
                self._storage.tmp_join(self._path_legacy_arch_release), "wb"
            ) as file:
                Release(
                    filter_none(
                        {
                            "Archive": self._suite,
                            "Origin": ORIGIN,
                            "Label": LABEL,
                            "Acquire-By-Hash": "yes",
                            "Components": self._component,
                            "Architectures": self._architecture,
                        }
                    )
                ).dump(fd=file)
            self._mark_as_modified(self._path_legacy_arch_release)

        await self._sign(self._path_inrelease)
        await gather(
            self._sign_asc(self._path_legacy_release, extension="gpg"),
            *(self._storage.put_file(path) for path in release_paths),
        )

    async def _load_packages(self) -> None:
        """Load or initialize the Packages index."""
        try:
            with await self._open_index(self._path_packages, uncompress=True) as file:
                pkgs = self._pkgs
                for pkg in Packages.iter_paragraphs(file, use_apt_pkg=_USE_APT_PKG):
                    pkgs[pkg["Package"]] = pkg
        except PackageNotFound:
            return

    def _update_packages(self, name: str, pkg: Deb822) -> None:
        """Add or update a package in the Packages index.

        Args:
            name: Package name.
            pkg: Package control.
        """
        self._pkgs[name] = pkg
        self._updated_indexes.add(self._path_packages)

    def _remove_from_packages(self, name: str) -> None:
        """Remove a package from the Packages index.

        Args:
            name: Package name.
        """
        del self._pkgs[name]
        self._updated_indexes.add(self._path_packages)

    async def _save_packages(self) -> None:
        """Save the Package index."""
        if self._path_packages not in self._updated_indexes:
            # No changes
            return

        tmp_path = self._storage.tmp_join(self._path_packages)
        with ExitStack() as exit_stack:
            files = [exit_stack.enter_context(open(tmp_path, "wb"))]
            files.extend(
                (
                    exit_stack.enter_context(
                        _COMPRESSION[ext](f"{tmp_path}.{ext}", "wb")
                    )
                    for ext in DEFAULT_PACKAGES_COMPRESSION
                )
            )
            pkgs = self._pkgs
            for name in sorted(pkgs):
                pkgs[name].order_first("Package")
                paragraph = pkgs[name].dump().encode()
                for file in files:
                    file.write(paragraph)
                    file.write(b"\n")

        await gather(
            self._put_index(self._path_packages),
            self._put_compressed_index(
                self._path_packages, DEFAULT_PACKAGES_COMPRESSION
            ),
        )

    async def _load_contents(self) -> None:
        """Load the Contents index."""
        try:
            with TextIOWrapper(await self._open_index(self._path_contents)) as file:
                contents = self._contents
                for line in file:
                    try:
                        filename, pkgs_str = line.strip().split(" ", 1)
                        pkgs = pkgs_str.split(",")
                    except ValueError:  # pragma: no cover
                        # "Contents" specification define to skip unparseable lines
                        continue
                    contents.append((filename, pkgs))
        except PackageNotFound:
            return

    def _update_contents(
        self, name: str, section: Optional[str], files: Optional[List[str]] = None
    ) -> None:
        """Update the Contents index for a package.

        Args:
            name: Package name.
            section: Package Section field.
            files: Files to add.
        """
        if section:
            name = f"{section}/{name}"
        if files is None:
            files = list()
        if self._update_contents_entries(name, files, self._contents):
            self._updated_indexes.add(self._path_contents)

    @staticmethod
    def _update_contents_entries(
        name: str, files: List[str], contents: List[Tuple[str, List[str]]]
    ) -> bool:
        """Update the Contents index entries.

        Args:
            name: Package name.
            files: Files to add.
            contents: Contents index entries.

        Returns:
            True if "contents" argument changed.
        """
        changed = False
        for entry in contents.copy():
            filename, pkgs = entry
            if filename not in files and name in pkgs:
                # File no more in current package version
                if len(pkgs) == 1:
                    contents.remove(entry)
                else:
                    # Still in another package
                    pkgs.remove(name)
                changed = True
            elif filename in files:
                if name not in pkgs:
                    # File also in another package
                    pkgs.append(name)
                    changed = True
                files.remove(filename)
        if files:
            contents.extend((filename, [name]) for filename in files)
            changed = True
        return changed

    async def _save_contents(self) -> None:
        """Save the Contents index."""
        if self._path_contents not in self._updated_indexes:
            # No changes
            return

        tmp_path = self._storage.tmp_join(self._path_contents)
        with ExitStack() as exit_stack:
            files = tuple(
                exit_stack.enter_context(_COMPRESSION[ext](f"{tmp_path}.{ext}", "wb"))
                for ext in DEFAULT_CONTENTS_COMPRESSION
            )
            for filename, pkgs in sorted(self._contents):
                line = f"{filename} {','.join(pkgs)}\n".encode()
                for file in files:
                    file.write(line)

        await self._put_compressed_index(
            self._path_contents, DEFAULT_CONTENTS_COMPRESSION
        )

    async def _sign_pkg(self, path: str, sign: bool) -> bool:
        """Sign DEB package. Must be in temporary directory.

        Args:
            path: Package path.
            sign: If True, sign package.

        Returns:
            True if package was signed.
        """
        if self._gpg_key is None or not sign:
            return False

        await self._exec("debsigs", "--sign=origin", "-k", self._gpg_user_id, path)
        return True

    @classmethod
    async def find_repository(cls, filename: str, **variables: str) -> Dict[str, str]:
        """Find the repository where to store a package.

        Args:
            filename: Package filename.
            variables: Extra variables to substitute in CONFIG fields to determinate
                repository URL.

        Returns:
            Repository configuration related to this package.
        """
        if CONFIG.get("url") is None:
            raise ValueError(
                "DEB Repository root URL must be defined. "
                "It can be set using the 'url' field in the DEB_CONFIG environment "
                "variable."
            )
        autodetect_codename = "codename" not in CONFIG

        pkg_config = cls._parse_pkg_name(filename, autodetect_codename)

        variables.update(pkg_config)
        config = {
            key: Template(value).substitute(variables) for key, value in CONFIG.items()
        }
        for key in ("codename", "architecture"):
            try:
                config.setdefault(key, pkg_config[key])
            except KeyError:
                continue
        return config

    @staticmethod
    def _parse_pkg_name(
        filename: str, autodetect_codename: bool = False
    ) -> Dict[str, str]:
        """Parse DEB package name.

        Args:
            filename: Package filename.
            autodetect_codename: If True, require "Codename" in package name.

        Returns:
            Fields from package name.
        """
        match = _NAME.match(basename(filename))
        if match is None:
            raise InvalidPackage(
                f'Unable to parse the "{filename}" package name. '
                "The package name must be valid and follow the DEB naming "
                'convention "<Package>_<Version>_<architecture>.deb" with '
                '"Version" in the form '
                '"<upstream_version>-<debian_revision>+<codename>" (For instance: '
                '"my-package_1.0.0-1+bullseye_amd64.deb").'
                if autodetect_codename
                else (
                    f'Unable to parse the "{filename}" package name. '
                    "The package name must be valid and follow the DEB naming "
                    'convention "<Package>_<Version>_<Architecture>.deb" (For '
                    'instance: "my-package_1.0.0-1_amd64.deb").'
                )
            )
        fields = match.groupdict()

        if autodetect_codename:
            revision = fields["revision"]
            if not revision:
                raise InvalidPackage(
                    'Unable to get "debian_revision" from "Version" field '
                    f'"{revision}" for package "{filename}".'
                    'The package "Version" field must contain a Debian revision that '
                    "include the distribution codename and be in the form "
                    '"<upstream_version>-<debian_revision>+<codename>" '
                    '(For instance: "1.0.0-1+bullseye").'
                )
            match = _REVISION.match(revision)
            if match is None or not match.groupdict().get("codename"):
                raise InvalidPackage(
                    'Unable to get "codename" from "Version.debian_revision" field '
                    f'"{revision}" for package "{filename}".'
                    'The package "debian_revision" field must contain the codename '
                    'and be in the form "<debian_revision>+<codename>" '
                    '(For instance: "1+bullseye").'
                )
            fields.update(match.groupdict())

        return fields

    def _get_pool_path(self, filename: str) -> str:
        """Define package pool directory.

        Args:
            filename: Package name.

        Returns:
            Pool directory.
        """
        prefix = filename[: 4 if filename.startswith("lib") else 1]
        return f"{self._path_pool}/{prefix}/{filename.split('_', 1)[0]}/{filename}"

    @staticmethod
    def _ensure_field(file: Deb822, field: str, value: str) -> None:
        """Ensure a value is present in a field.

        Args:
            file: Deb822 file.
            field: Field Name.
            value: Value.
        """
        if value not in file[field].split():
            file[field] += f" {value}"

    async def _put_index(self, path: str) -> None:
        """Put a package index file and register it in the main Release file.

        Args:
            path: Relative path to the file to register.
        """
        name = relpath(path, self._path_dist)
        tmp_path = self._storage.tmp_join(path)
        dir_path = dirname(path)
        size = str(getsize(tmp_path))
        hash_by_paths = []
        for field, digest in (await self._hash_file(tmp_path)).items():
            self._release.setdefault(field, []).append(
                {"name": name, "size": size, field.lower(): digest}
            )
            hash_by_paths.append(f"{dir_path}/by-hash/{field}/{digest}")

        self._mark_as_modified(path, self._path_inrelease, self._path_legacy_release)
        self._mark_as_modified(*hash_by_paths, invalidate=False)
        await gather(
            self._storage.put_file(path),
            *(self._storage.put_file(path, dst=dst) for dst in hash_by_paths),
        )

    async def _put_compressed_index(
        self, path: str, compressions: Iterable[str]
    ) -> None:
        """Put one or more compressed variant of an index file.

        Args:
            path: Uncompressed file path.
            compressions: Compressions extensions.
        """
        await gather(*(self._put_index(f"{path}.{ext}") for ext in compressions))

    async def _open_index(self, path: str, uncompress: bool = False) -> IO[Any]:
        """Open index file for reading.

        Args:
            path: Uncompressed canonical relative index path.
            uncompress: If True, open an uncompressed copy of the file, like
                required when using "python-apt". Else directly stream content.

        Returns:
            Opened file.
        """
        indexes = dict()
        for algorithm in _HASH_ALGORITHMS:
            field = self._hash_field_name(algorithm)
            try:
                indexes[field] = self._release[field]
            except KeyError:
                continue
        if indexes:
            found_path, ext = self._find_index_from_release(path, indexes)
        else:
            found_path, ext = await self._find_index_from_storage(path)

        makedirs(self._storage.tmp_join(dirname(found_path)), exist_ok=True)
        await self._storage.get_file(found_path)
        if uncompress and ext:
            tmp_path = self._storage.tmp_join(path)
            with _COMPRESSION[ext](
                self._storage.tmp_join(found_path), "rb"
            ) as compressed:
                with open(tmp_path, "wb") as file:
                    copyfileobj(compressed, file)
            return open(tmp_path, "rb")
        return _COMPRESSION.get(ext, open)(self._storage.tmp_join(found_path), "rb")

    async def _find_index_from_storage(self, path: str) -> Tuple[str, str]:
        """Find best index path in storage.

        Args:
            path: Uncompressed canonical relative index path.

        Returns:
            Canonical relative path, File extension.
        """
        paths_exts = [(f"{path}.{ext}", ext) for ext in _COMPRESSION]
        paths_exts.append((path, ""))
        available_paths = [
            paths_exts[i]
            for i, path_exists in enumerate(
                await gather(*(self._storage.exists(path) for path, _ in paths_exts))
            )
            if path_exists
        ]
        self._previous_indexes_hashes.setdefault(path, set()).update(
            relpath(path, self._path_dist) for path, _ in available_paths
        )
        try:
            return available_paths[0]
        except IndexError:
            raise PackageNotFound("Unable to find index path.")

    def _find_index_from_release(
        self, path: str, indexes: Dict[str, List[Dict[str, str]]]
    ) -> Tuple[str, str]:
        """Find best "by-hash" index path if exists in InRelease.

        Args:
            path: Uncompressed canonical relative index path.
            indexes: Indexes list from InRelease.

        Returns:
            "by-hash" relative path, File extension.
        """
        previous = self._previous_indexes_hashes.setdefault(path, set())
        path = relpath(path, self._path_dist)
        paths = [f"{path}.{ext}" for ext in _COMPRESSION]
        previous.add(path)
        previous.update(paths)
        paths.append(path)
        candidates: Dict[str, str] = dict()
        for field, field_indexes in indexes.items():
            for index in field_indexes:
                if index["name"] in paths:
                    by_hash_path = (
                        f"{dirname(index['name'])}/by-hash/"
                        f"{field}/{index[field.lower()]}"
                    )
                    candidates.setdefault(index["name"], by_hash_path)
                    previous.add(by_hash_path)

        for path in paths:
            try:
                by_hash_path = candidates[path]
            except KeyError:
                continue
            return f"{self._path_dist}/{by_hash_path}", splitext(path)[1].lstrip(".")
        raise PackageNotFound("Unable to find index path.")

    def _clear_indexes(self) -> None:
        """Clear indexes in InReleases."""
        all_indexes = []
        for algorithm in _HASH_ALGORITHMS:
            try:
                all_indexes.append(self._release[self._hash_field_name(algorithm)])
            except KeyError:
                continue

        to_remove = set()
        previous = self._previous_indexes_hashes
        for changed_path in self._updated_indexes:
            to_remove.update(previous[changed_path])

        for indexes in all_indexes:
            for index in indexes.copy():
                if index["name"] in to_remove:
                    indexes.remove(index)

        self._mark_for_deletion(*(f"{self._path_dist}/{path}" for path in to_remove))

    async def _hash_file(self, path: str) -> Dict[str, str]:
        """Hash a file with multiple hashing algorithms.

        Args:
            path: Local path to file to hash.

        Returns:
            Fields with hash digests.
        """
        hash_objs = {
            self._hash_field_name(algo): new_hash(algo)
            for algo in self._index_hash_algorithms
        }
        with open(path, "rb") as file:
            while True:
                data = await to_thread(file.read, 1048576)
                if not data:
                    break
                await gather(
                    *(
                        to_thread(hash_obj.update, data)
                        for hash_obj in hash_objs.values()
                    )
                )
        return {field: hash_obj.hexdigest() for field, hash_obj in hash_objs.items()}

    @staticmethod
    def _hash_field_name(algorithm: str) -> str:
        """Return hash field name for the given algorithm.

        Args:
            algorithm: Hash algorithm.

        Returns:
            Field name.
        """
        return "MD5Sum" if algorithm == "md5" else algorithm.upper()

    @staticmethod
    def _hash_description(pkg: Deb822) -> None:
        """Hash package description and add it to package control.

        Args:
            pkg: Package control.
        """
        try:
            desc = pkg["Description"]
        except KeyError:
            return
        else:
            pkg["Description-md5"] = new_hash("md5", desc.encode()).hexdigest()  # nosec

    @staticmethod
    def _check_pkg(fields: Dict[str, str], pkg: Deb822) -> None:
        """Check package consistency.

        Args:
            fields: Fields from package name.
            pkg: Package control.
        """
        if fields.get("revision"):
            fields["version"] = f'{fields["version"]}-{fields["revision"]}'

        for key in ("Package", "Version", "Architecture"):
            field = fields[key.lower()]
            control = pkg[key]
            if field != control:
                raise InvalidPackage(
                    f'DEB package "{key}" metadata field must match with value in '
                    f'package filename: "{field}" != "{control}"'
                )

    async def _check_conflicts(
        self, path: str, pool_path: str, fields: Dict[str, str]
    ) -> None:
        """Check if the package already exists.

        Args:
            path: Absolute package path.
            pool_path: Pool package path.
            fields: Fields from package name.
        """
        if await self._storage.exists(pool_path):
            try:
                current_pkg_path = self._pkgs[fields["package"]]["Filename"]
            except KeyError:
                pass
            else:
                abs_pool_path = self._storage.join(pool_path)
                if (current_pkg_path == pool_path) or (abs_pool_path != path):
                    if path != abs_pool_path:
                        await self._storage.remove(path, absolute=True)
                    raise PackageAlreadyExists(basename(path))
