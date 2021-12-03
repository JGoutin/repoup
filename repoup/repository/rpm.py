"""Update an RPM repository to add or remove packages.

TODO: Improvements
    - Add support for "prestodelta" records
    - Add support for "updateinfo" records
    - Run "rpmlint" before adding package and return result
"""
from asyncio import gather, to_thread
from contextlib import contextmanager
from os import getenv, makedirs
from os.path import basename, splitext
from re import IGNORECASE, compile
from string import Template, ascii_letters
from typing import Dict, Generator, Optional, Set, Tuple

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
    """

    __slots__ = [
        "_checksum_type",
        "_pkgs",
        "_outdated_files",
        "_compression",
        "_db_compression",
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
    ) -> None:
        super().__init__(
            url,
            gpg_private_key=gpg_private_key,
            gpg_password=gpg_password,
            gpg_verify=gpg_verify,
            gpg_clear=gpg_clear,
        )
        self._checksum_type = checksum_type
        self._pkgs: Dict[str, Dict[str, cr.Package]] = dict(
            primary=dict(), filelists=dict(), other=dict(), updateinfo=dict()
        )
        self._outdated_files: Set[str] = set()
        self._compression = compression
        self._db_compression = db_compression

    async def __aenter__(self) -> "Repository":
        await super().__aenter__()
        self._changed_paths += [
            self._storage.join(_REPOMD),
            f"{self._storage.join(_REPODATA)}/",
            f"{self._storage.join()}/",
        ]
        if self._gpg_key is not None and self._gpg_verify:
            await self._exec(*_RPM, "--import", self._gpg_public_key)
        return self

    async def add(self, path: str, remove_source: bool = True) -> None:
        """Add a package if not already present in the repository.

        Args:
            path: Absolute package path.
            remove_source: If True, remove the source file once moved in the repository.
        """
        filename = basename(path)
        if splitext(filename)[0] in self._pkgs["primary"]:
            if path != self._storage.join(filename):
                await self._storage.remove(path, absolute=True)
            raise PackageAlreadyExists(filename)

        await self._storage.get_file(path, absolute=True)
        await self._sign_pkg(filename)

        pkg = cr.package_from_rpm(self._storage.tmp_join(filename), self._checksum_type)
        nvra = pkg.nvra()
        for pkgs in self._pkgs.values():
            pkgs.setdefault(nvra, pkg)

        transactions = [self._storage.put_file(filename)]
        if remove_source and path != self._storage.join(filename):
            transactions.append(self._storage.remove(path, absolute=True))

        await gather(*transactions)
        await self._storage.remove_tmp(filename)

    async def remove(self, filename: str) -> None:
        """Remove a package if present in the repository.

        Args:
            filename: Package filename.
        """
        nvra = splitext(filename)[0]
        for record_pkgs in self._pkgs.values():
            try:
                del record_pkgs[nvra]
            except KeyError:
                continue
        await self._storage.remove(filename)

    async def _load(self) -> None:
        """Load current repository data if exists."""
        makedirs(self._storage.tmp_join(_REPODATA), exist_ok=True)
        try:
            await self._storage.get_file(_REPOMD)
        except PackageNotFound:
            return

        repomd = cr.Repomd()
        cr.xml_parse_repomd(self._storage.tmp_join(_REPOMD), repomd)

        records = dict()
        for record in repomd.records:
            self._outdated_files.add(record.location_href)
            if record.type in _PKG_METADATA:
                records[record.type] = record.location_href

        await gather(*(self._storage.get_file(path) for path in records.values()))
        for record_type, path in records.items():
            self._load_record(record_type, path)

    def _load_record(self, record_type: str, path: str) -> None:
        """Load record from XML file.

        Args:
            record_type: Record type.
            path: Record file path.
        """
        packages = self._pkgs[record_type]

        def add_pkg(pkg: cr.Package) -> None:
            """Add Package to repository packages.

            Args:
                pkg: Package
            """
            packages[pkg.nvra()] = pkg

        getattr(cr, f"xml_parse_{record_type}")(
            self._storage.tmp_join(path), pkgcb=add_pkg
        )

    async def _save(self) -> None:
        """Save updated repository data."""
        makedirs(self._storage.tmp_join(_REPODATA), exist_ok=True)
        repomd = cr.Repomd()
        metadata_files = [_REPOMD]
        for metadata_type in _PKG_METADATA:
            metadata_files.extend(self._save_record(metadata_type, repomd))
        repomd.sort_records()

        with open(self._storage.tmp_join(_REPOMD), "wt") as repomd_file:
            await to_thread(repomd_file.write, repomd.xml_dump())

        await gather(
            self._sign_asc(_REPOMD),
            *(self._storage.put_file(path) for path in metadata_files),
            *(self._storage.remove(path) for path in self._outdated_files),
        )

    def _save_record(self, record_type: str, repomd: cr.Repomd) -> Tuple[str, str]:
        """Save record as XML and SQLite files.

        Args:
            record_type: Record type.
            repomd: Repomd

        Returns:
            Record files paths.
        """
        content_stat = cr.ContentStat(self._checksum_type)
        db_record_type = f"{record_type}_db"
        with self._create_db(db_record_type, content_stat) as db:
            db_file, db_path = db
            with self._create_xml(record_type, content_stat) as xml:
                xml_file, xml_path = xml
                for pkg in self._pkgs[record_type].values():
                    xml_file.add_pkg(pkg)
                    db_file.add_pkg(pkg)
        return (
            self._set_record(db_path, db_record_type, repomd, content_stat),
            self._set_record(xml_path, record_type, repomd, content_stat),
        )

    @contextmanager
    def _create_xml(
        self, record_type: str, content_stat: cr.ContentStat
    ) -> Generator[Tuple[cr.XmlFile, str], None, None]:
        """Create XML record.

        Args:
            record_type: Record type.
            content_stat: Empty content stat.

        Yields:
            XML file.
        """
        path = self._storage.tmp_join(
            _REPODATA,
            f"{record_type}.xml{cr.compression_suffix(self._compression) or ''}",
        )
        file = _FILES[record_type](path, self._compression, content_stat)
        file.set_num_of_pkgs(len(self._pkgs[record_type]))
        yield file, path
        file.close()

    @contextmanager
    def _create_db(
        self, record_type: str, content_stat: cr.ContentStat
    ) -> Generator[Tuple[cr.Sqlite, str], None, None]:
        """Create SQLite record.

        Args:
            record_type: Record type.
            content_stat: XML content stat.

        Yields:
            SQLite file.
        """
        compression = self._db_compression != cr.NO_COMPRESSION

        path = self._storage.tmp_join(_REPODATA, f"{record_type[:-3]}.sqlite")
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
    ) -> str:
        """Set repomd record.

        Args:
            path: record file path.
            record_type: Record type.
            repomd: Repomd.
            content_stat: XML content stat.

        Returns:
            Record file path.
        """
        record = cr.RepomdRecord(record_type, path)
        record.load_contentstat(content_stat)
        record.fill(self._checksum_type)
        record.rename_file()
        path = record.location_href
        self._outdated_files.discard(path)
        repomd.set_record(record)
        return path

    async def _sign_pkg(self, filename: str) -> None:
        """Sign RPM package. Must be in temporary directory.

        Args:
            filename: package
        """
        if self._gpg_key is None:
            return

        await self._exec(
            "rpm", "--addsign", "--define", f"%_gpg_name {self._gpg_user_id}", filename
        )
        if self._gpg_verify:
            await self._exec(*_RPM, "--checksig", filename)

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

    @classmethod
    async def find_repository(cls, filename: str, **variables: str) -> str:
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
            Path of the repository related to this package.
        """
        if BASEURL is None:
            raise ValueError(
                "BASEURL must be defined. "
                "It can be set using RPM_BASEURL environment variable."
            )

        match = _NEVRA.match(basename(filename))
        if match is None:
            raise InvalidPackage(
                f'Unable to parse the "{filename}" package name. '
                f"The package name must be valid and follow the RPM naming convention "
                f'"<name>-<version>-<release>-<arch>.rpm" with "release" in the form '
                f'"<number>.<dist>" (For instance: '
                f'"my_package-1.0.0-1.el8.noarch.rpm").'
            )

        nevra = match.groupdict()
        variables["arch"] = nevra["arch"]
        variables["basearch"] = nevra["arch"]

        if "$releasever" in BASEURL:
            try:
                dist = nevra["release"].split(".", 1)[1]
            except IndexError:
                raise InvalidPackage(
                    f'Unable to get "releasever" from "release" value '
                    f'"{nevra["release"]}" for package "{filename}".'
                    f'The package "release" field must contain the dist tag and be in '
                    f'the form "<number>.<dist>" (For instance: "1.el8"). '
                    f"This is generally done using the dist macro in RPM spec: "
                    f'"Release: 1{_DIST_TAG}".'
                )

            variables["releasever"] = dist.lstrip(ascii_letters)

        return Template(BASEURL).substitute(variables)