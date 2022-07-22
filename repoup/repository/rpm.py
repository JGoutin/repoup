"""Update an RPM repository to add or remove packages."""
from asyncio import gather, to_thread
from contextlib import contextmanager
from os import getenv, makedirs
from os.path import basename, splitext
from re import IGNORECASE, compile
from string import Template, ascii_letters
from typing import Dict, Generator, List, Optional, Set, Tuple, Union, Iterable

import createrepo_c as cr

from repoup.exceptions import InvalidPackage, PackageAlreadyExists, PackageNotFound
from repoup.repository import RepositoryBase

_FILES = {
    "primary": cr.PrimaryXmlFile,
    "primary_db": cr.PrimarySqlite,
    "filelists": cr.FilelistsXmlFile,
    "filelists_db": cr.FilelistsSqlite,
    "other": cr.OtherXmlFile,
    "other_db": cr.OtherSqlite,
    "updateinfo": cr.UpdateInfoXmlFile,
    "prestodelta": None,  # Not supported yet by "createrepo_c"
}
_PKG_METADATA = ("primary", "other", "filelists")
_REPODATA = "repodata"
_REPOMD = f"{_REPODATA}/repomd.xml"
_NEVRA = compile(
    r"^(.*/)?(?P<name>.*)-((?P<epoch>\d+):)?(?P<version>.*)-(?P<release>.*)"
    r"\.(?P<arch>.*)\.rpm$",
    flags=IGNORECASE,
)
_DIST_TAG = "%{?dist}"

#: RPM repository base URL to use. Support RPM variables like $releasever, $basearch.
BASEURL = getenv("RPM_BASEURL", None)

#: Comma separated list of default supported "$basearch" values
BASEARCHS = getenv("RPM_BASEARCHS", "x86_64,aarch64").split(",")

#: Checksum type to use in metadata.
#: See "createrepo_c" documentation for possible values.
CHECKSUM_TYPE = int(getenv("RPM_CHECKSUM_TYPE", cr.SHA256))

#: Metadata XML files compression to use.
#: See "createrepo_c" documentation for possible values.
COMPRESSION = int(getenv("RPM_COMPRESSION", cr.GZ_COMPRESSION))

#: Database compression to use. See "createrepo_c" documentation for possible values.
DB_COMPRESSION = int(getenv("RPM_DB_COMPRESSION", cr.BZ2_COMPRESSION))

# If True, use "sudo" with the "rpm" command to import and remove the GPG key
_RPM = ("sudo", "rpm") if bool(getenv("RPM_GPG_REQUIRE_SUDO", False)) else ("rpm",)


class Repository(RepositoryBase):
    """RPM repository to update.

    Args:
        url: Repository storage URL.
        gpg_private_key: Path to GPG private key.
        gpg_password: GPG private key password.
        gpg_verify: If True, verify signature after signing.
        gpg_clear: Clear the key from GPG after repository update.
        checksum_type: Checksum type to use.
        compression: Compression to use for metadata.
        db_compression: Compression to use for database.
        basearchs: Supported repository architectures. "noarch" packages are
            added to all supported architecture, other packages are added to their
            architecture only.
    """

    __slots__ = [
        "_checksum_type",
        "_pkgs",
        "_outdated_files",
        "_compression",
        "_db_compression",
        "_basearchs",
        "_basearch_path",
    ]

    def __init__(
        self,
        url: str,
        *,
        gpg_private_key: Optional[str] = None,
        gpg_password: Optional[str] = None,
        gpg_verify: bool = False,
        gpg_clear: bool = False,
        checksum_type: int = CHECKSUM_TYPE,
        compression: int = COMPRESSION,
        db_compression: int = DB_COMPRESSION,
        basearchs: Optional[List[str]] = None,
    ) -> None:
        url, basearch_path = url.split("$basearch", 1)
        super().__init__(
            url,
            gpg_private_key=gpg_private_key,
            gpg_password=gpg_password,
            gpg_verify=gpg_verify,
            gpg_clear=gpg_clear,
        )
        self._checksum_type = checksum_type
        self._pkgs: Dict[str, Dict[str, Dict[str, cr.Package]]] = dict()
        self._outdated_files: Set[str] = set()
        self._compression = compression
        self._db_compression = db_compression
        self._basearchs = BASEARCHS if basearchs is None else basearchs
        self._basearch_path = basearch_path

    async def __aenter__(self) -> "Repository":
        await super().__aenter__()
        if self._gpg_key is not None and self._gpg_verify:
            await self._exec(*_RPM, "--import", self._gpg_public_key)
        return self

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
        filename = basename(path)
        pkg_name = splitext(filename)[0]

        await self._storage.get_file(path, dst=filename, absolute=True)
        signed = await self._sign_pkg(filename, sign)

        pkg = cr.package_from_rpm(self._storage.tmp_join(filename), self._checksum_type)
        nvra = pkg.nvra()
        if not (pkg_name == nvra or pkg_name == pkg.nevra()):
            raise InvalidPackage(
                "RPM package filename must match NVRA or NEVRA from its metadata: "
                + nvra
            )
        archs = self._get_archs(pkg.arch)
        await self._load_archs(archs)

        dst_paths = [
            self._storage.join(self._arch_path(filename, arch)) for arch in archs
        ]
        if all(pkg_name in self._pkgs[arch]["primary"] for arch in archs):
            if all(path != dst_path for dst_path in dst_paths):
                await self._storage.remove(path, absolute=True)
            raise PackageAlreadyExists(filename)

        pkg.location_href = filename
        transactions = []
        for arch, dst_path in zip(archs, dst_paths):
            pkg_path = self._arch_path(filename, arch)

            for pkgs in self._pkgs[arch].values():
                pkgs.setdefault(nvra, pkg)

            if signed or pkg_path != dst_path:
                transactions.append(self._storage.put_file(filename, dst=pkg_path))
                self._mark_as_modified(pkg_path, invalidate=False)
            else:
                remove_source = False

        if remove_source:
            transactions.append(self._storage.remove(path, absolute=True))

        await gather(*transactions)
        await self._storage.remove_tmp(filename)
        return dst_paths

    async def remove(self, filename: str) -> None:
        """Remove a package if present in the repository.

        Args:
            filename: Package filename.
        """
        filename = basename(filename)
        nvra = splitext(filename)[0]
        archs = self._get_archs(self._parse_nevra(filename)["arch"])
        await self._load_archs(archs)
        for arch in archs:
            try:
                pkgs = self._pkgs[arch]
            except KeyError:
                continue
            found = False
            for record_pkgs in pkgs.values():
                try:
                    del record_pkgs[nvra]
                    found = True
                except KeyError:
                    continue
            if found:
                self._mark_for_deletion(self._arch_path(filename, arch))

    async def _load(self) -> None:
        """Load current repository data if exists."""

    async def _load_arch(self, arch: str) -> None:
        """Load architecture repository data if exists.

        Args:
            arch: Architecture.
        """
        try:
            self._pkgs[arch]
        except KeyError:
            self._init_arch(arch)
            makedirs(
                self._storage.tmp_join(self._arch_path(_REPODATA, arch)), exist_ok=True
            )
            repomd_path = self._arch_path(_REPOMD, arch)
            try:
                await self._storage.get_file(repomd_path)
            except PackageNotFound:
                return

            repomd = cr.Repomd()
            cr.xml_parse_repomd(self._storage.tmp_join(repomd_path), repomd)

            records = dict()
            for record in repomd.records:
                self._outdated_files.add(record.location_href)
                if record.type in _PKG_METADATA:
                    records[record.type] = record.location_href

            await gather(
                *(
                    self._storage.get_file(self._arch_path(path, arch))
                    for path in records.values()
                )
            )
            for record_type, path in records.items():
                self._load_record(record_type, path, arch)

    async def _load_archs(self, archs: Iterable[str]) -> None:
        """Load architecture repository data for a list of architectures.

        Args:
            archs: Architectures.
        """
        await gather(*(self._load_arch(arch) for arch in archs))

    def _get_archs(self, arch: str) -> Iterable[str]:
        """Get full list of packages architectures from NEVRA architecture.

        Args:
            arch: Architecture value from NEVRA.

        Returns:
            Architectures.
        """
        return self._basearchs if arch == "noarch" else (arch,)

    def _init_arch(self, arch: str) -> None:
        """Initialize architecture repository data.

        Args:
            arch: Architecture.
        """
        self._pkgs[arch] = dict(
            primary=dict(), filelists=dict(), other=dict(), updateinfo=dict()
        )

    def _load_record(self, record_type: str, path: str, arch: str) -> None:
        """Load record from XML file.

        Args:
            record_type: Record type.
            path: Record file path.
            arch: Architecture.
        """
        packages = self._pkgs[arch][record_type]

        def add_pkg(pkg: cr.Package) -> None:
            """Add Package to repository packages.

            Args:
                pkg: Package
            """
            packages[pkg.nvra()] = pkg

        getattr(cr, f"xml_parse_{record_type}")(
            self._storage.tmp_join(self._arch_path(path, arch)), pkgcb=add_pkg
        )

    async def _save(self) -> None:
        """Save updated repository data."""
        if not self._pkgs:
            for arch in BASEARCHS:
                self._init_arch(arch)
        await gather(*(self._save_arch(arch) for arch in self._pkgs))

    async def _save_arch(self, arch: str) -> None:
        """Save updated architecture repository data.

        Args:
            arch: Architecture.
        """
        makedirs(
            self._storage.tmp_join(self._arch_path(_REPODATA, arch)), exist_ok=True
        )
        repomd = cr.Repomd()
        metadata_files: List[str] = list()
        for metadata_type in _PKG_METADATA:
            metadata_files.extend(self._save_record(metadata_type, repomd, arch))
        repomd.sort_records()

        for path in tuple(metadata_files):
            # If metadata files have the same name, there is no changes (Contains hash)
            if path in self._outdated_files:
                metadata_files.remove(path)
                self._outdated_files.remove(path)

        if not metadata_files:
            return

        repomd_path = self._arch_path(_REPOMD, arch)
        self._mark_as_modified(repomd_path)
        self._mark_as_modified(*metadata_files, invalidate=False)
        self._mark_for_deletion(*self._outdated_files)

        with open(self._storage.tmp_join(repomd_path), "wt") as repomd_file:
            await to_thread(repomd_file.write, repomd.xml_dump())

        metadata_files.append(repomd_path)
        await gather(
            self._sign_asc(repomd_path),
            *(self._storage.put_file(path) for path in metadata_files),
        )

    def _save_record(
        self, record_type: str, repomd: cr.Repomd, arch: str
    ) -> Tuple[str, str]:
        """Save record as XML and SQLite files.

        Args:
            record_type: Record type.
            repomd: Repomd
            arch: Architecture.

        Returns:
            Record files paths.
        """
        content_stat = cr.ContentStat(self._checksum_type)
        db_record_type = f"{record_type}_db"
        with self._create_db(db_record_type, content_stat, arch) as db:
            db_file, db_path = db
            with self._create_xml(record_type, content_stat, arch) as xml:
                xml_file, xml_path = xml
                for pkg in self._pkgs[arch][record_type].values():
                    xml_file.add_pkg(pkg)
                    db_file.add_pkg(pkg)
        return (
            self._set_record(db_path, db_record_type, repomd, content_stat, arch),
            self._set_record(xml_path, record_type, repomd, content_stat, arch),
        )

    @contextmanager
    def _create_xml(
        self, record_type: str, content_stat: cr.ContentStat, arch: str
    ) -> Generator[Tuple[cr.XmlFile, str], None, None]:
        """Create XML record.

        Args:
            record_type: Record type.
            content_stat: Empty content stat.
            arch: Architecture.

        Yields:
            XML file.
        """
        path = self._storage.tmp_join(
            self._arch_path(_REPODATA, arch),
            f"{record_type}.xml{cr.compression_suffix(self._compression) or ''}",
        )
        file = _FILES[record_type](path, self._compression, content_stat)
        file.set_num_of_pkgs(len(self._pkgs[arch][record_type]))
        yield file, path
        file.close()

    @contextmanager
    def _create_db(
        self, record_type: str, content_stat: cr.ContentStat, arch: str
    ) -> Generator[Tuple[cr.Sqlite, str], None, None]:
        """Create SQLite record.

        Args:
            record_type: Record type.
            content_stat: XML content stat.
            arch: Architecture.

        Yields:
            SQLite file.
        """
        compression = self._db_compression != cr.NO_COMPRESSION

        path = self._storage.tmp_join(
            self._arch_path(_REPODATA, arch), f"{record_type[:-3]}.sqlite"
        )
        file = _FILES[record_type](path)
        yield file, path if not compression else path + cr.compression_suffix(
            self._db_compression
        )
        file.dbinfo_update(content_stat.checksum)
        file.close()

        if compression:
            record = cr.RepomdRecord(record_type, path)
            record.load_contentstat(content_stat)
            record.compress_and_fill(self._checksum_type, self._db_compression)

    def _set_record(
        self,
        path: str,
        record_type: str,
        repomd: cr.Repomd,
        content_stat: cr.ContentStat,
        arch: str,
    ) -> str:
        """Set repomd record.

        Args:
            path: record file path.
            record_type: Record type.
            repomd: Repomd.
            content_stat: XML content stat.
            arch: Architecture

        Returns:
            Record file path.
        """
        record = cr.RepomdRecord(record_type, path)
        record.load_contentstat(content_stat)
        record.fill(self._checksum_type)
        record.rename_file()
        path = record.location_href
        repomd.set_record(record)
        return self._arch_path(path, arch)

    async def _sign_pkg(self, filename: str, sign: bool) -> bool:
        """Sign RPM package. Must be in temporary directory.

        Args:
            filename: package
            sign: If True, sign package.

        Returns:
            True if the package was signed.
        """
        if self._gpg_key is None or not sign:
            return False

        await self._exec(
            "rpm", "--addsign", "--define", f"%_gpg_name {self._gpg_user_id}", filename
        )
        if self._gpg_verify:
            await self._exec(*_RPM, "--checksig", filename)
        return True

    async def _gpg_clear_key(self) -> None:
        """Clear the key from GPG."""
        await RepositoryBase._gpg_clear_key(self)
        if self._gpg_verify:
            key_name = f"{self._gpg_user_id} ".encode()
            for line in (
                await self._exec(
                    *_RPM,
                    "-q",
                    "gpg-pubkey",
                    "--qf",
                    "%{NAME}-%{VERSION}-%{RELEASE}\t%{SUMMARY}\n",
                )
            ).splitlines():
                key_id, summary = line.split(b"\t", 1)
                if summary.startswith(key_name):
                    break
            else:  # pragma: no cover
                # Already uninstalled
                return
            await self._exec(*_RPM, "--erase", "--allmatches", key_id.decode())

    def _arch_path(self, path: str, arch: str) -> str:
        """Return architecture specific relative path.

        Args:
            arch: Architecture.

        Returns:
            Relative path.
        """
        return f"{arch}{self._basearch_path}/{path}"

    @classmethod
    async def find_repository(
        cls, filename: str, **variables: str
    ) -> Dict[str, Union[str, List[str]]]:
        """Find the repository where to store a package.

        Based on the "baseurl" field of the repository configuration.
        Variables like $releasever & $basearch are replaced with values detected in
        package name.

        To support $releasever, the dist tag must be present in the "release" field
        of the RPM package (See Fedora/RHEL naming convention for more information).

        Args:
            filename: Package filename.
            variables: Extra variables to substitute in BASEURL to determinate
                repository URL.

        Returns:
            Repository configuration related to this package.
        """
        if BASEURL is None:
            raise ValueError(
                "BASEURL must be defined. "
                "It can be set using RPM_BASEURL environment variable."
            )

        nevra = cls._parse_nevra(basename(filename))
        arch = nevra["arch"]

        if "$releasever" in BASEURL:
            try:
                dist = nevra["release"].split(".", 1)[1]
            except IndexError:
                raise InvalidPackage(
                    'Unable to get "releasever" from "release" value '
                    f'"{nevra["release"]}" for package "{filename}".'
                    'The package "release" field must contain the dist tag and be in '
                    'the form "<number>.<dist>" (For instance: "1.el8"). '
                    "This is generally done using the dist macro in RPM spec: "
                    f'"Release: 1{_DIST_TAG}".'
                )

            variables["releasever"] = dist.lstrip(ascii_letters)

        if arch != "noarch" and arch not in BASEARCHS:
            basearchs = BASEARCHS.copy()
            basearchs.append(arch)
        else:
            basearchs = BASEARCHS

        variables["basearch"] = "$basearch"
        return dict(url=Template(BASEURL).substitute(variables), basearchs=basearchs)

    @staticmethod
    def _parse_nevra(filename: str) -> Dict[str, str]:
        """Parse filename and return NEVRA.

        Args:
            filename: Filename.

        Returns:
            NEVRA.
        """
        match = _NEVRA.match(filename)
        if match is None:
            raise InvalidPackage(
                f'Unable to parse the "{filename}" package name. '
                "The package name must be valid and follow the RPM naming convention "
                '"<name>-<version>-<release>-<arch>.rpm" with "release" in the form '
                '"<number>.<dist>" (For instance: '
                '"my_package-1.0.0-1.el8.noarch.rpm").'
            )
        return match.groupdict()
