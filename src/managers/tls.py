#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling tls."""

import ipaddress
import logging
import re
import subprocess
from dataclasses import dataclass
from datetime import timedelta

from charms.tls_certificates_interface.v4.tls_certificates import (
    Certificate,
    PrivateKey,
    ProviderCertificate,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

from common.exceptions import ExecError
from core.state import ResolvedTLSContext, TLSScope
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)

USER_NAME = "_daemon_"
GROUP = "root"


@dataclass
class Sans:
    """Sans IP and DNS names."""

    sans_ip: list[str]
    sans_dns: list[str]


@dataclass
class StoreEntry:
    """Single entry of a keystore or a truststore."""

    alias: str
    cert: Certificate


class TLSManager:
    """Manage all TLS related events."""

    DEFAULT_HASH_ALGORITHM: hashes.HashAlgorithm = hashes.SHA256()
    SCOPES = (TLSScope.PEER, TLSScope.CLIENT)
    keytool = "charmed-cassandra.keytool"
    nodetool = "charmed-cassandra.nodetool"

    def __init__(self, workload: WorkloadBase):
        self.workload = workload

    def get_truststore_path(self, scope: TLSScope) -> str:
        """Return the truststore path for the given scope."""
        return self.workload.cassandra_paths.get_truststore(scope).as_posix()

    def get_keystore_path(self, scope: TLSScope) -> str:
        """Return the keystore path for the given scope."""
        return self.workload.cassandra_paths.get_keystore(scope).as_posix()

    @property
    def client_tls_ready(self) -> bool:
        """Return the readiness of client TLS configuration files."""
        return all(
            self.workload.path_exists(f.as_posix())
            for f in [
                self.workload.cassandra_paths.get_truststore(TLSScope.CLIENT),
                self.workload.cassandra_paths.get_keystore(TLSScope.CLIENT),
            ]
        )

    def build_sans(self, sans_dns: list[str], sans_ip: list[str]) -> Sans:
        """Build a SANs dictionary from lists of DNS names and IP addresses.

        Args:
            sans_dns: List of DNS names.
            sans_ip: List of IP addresses.

        Returns:
            Dict with keys 'sans_ip' and 'sans_dns' containing the processed values.
        """
        dns_names: list[str] = []
        ip_addresses: list[str] = []

        for dns_name in sans_dns:
            dns_names.append(str(x509.DNSName(dns_name).value))

        for ip_str in sans_ip:
            try:
                ip_addresses.append(str(x509.IPAddress(ipaddress.ip_address(ip_str)).value))
            except ValueError as e:
                logger.error(f"Invalid IP address: {ip_str}, error: {e}")
                continue

        return Sans(
            sans_ip=ip_addresses,
            sans_dns=dns_names,
        )

    def generate_internal_ca(
        self,
        common_name: str,
    ) -> tuple[Certificate, PrivateKey]:
        """Set up internal CA to issue self-signed certificates for internal communications.

        Should only run on leader unit.
        """
        ca_key = generate_private_key()
        ca = generate_ca(
            private_key=ca_key,
            validity=timedelta(days=3650),
            common_name=common_name,
            organization=TLSScope.PEER.value,
        )
        return (ca, ca_key)

    def generate_internal_credentials(
        self,
        ca: Certificate,
        ca_key: PrivateKey,
        unit_key: PrivateKey | None,
        common_name: str,
        sans_ip: frozenset[str] | None,
        sans_dns: frozenset[str] | None,
    ) -> tuple[ProviderCertificate, PrivateKey]:
        """Generate a ProviderCertificate and private key for internal use.

        Args:
            ca: The CA certificate to sign with.
            ca_key: The CA private key.
            unit_key: The unit's private key (if None, a new one is generated).
            common_name: The common name for the certificate.
            sans_ip: Optional set of SAN IP addresses.
            sans_dns: Optional set of SAN DNS names.

        Returns:
            Tuple of (ProviderCertificate, PrivateKey).
        """
        unit_key = unit_key if unit_key else generate_private_key()

        csr = generate_csr(
            private_key=unit_key,
            common_name=common_name,
            sans_ip=sans_ip,
            sans_dns=sans_dns,
        )
        certificate = generate_certificate(
            csr=csr, ca=ca, ca_private_key=ca_key, validity=timedelta(days=3650)
        )

        provider_cert = ProviderCertificate(
            relation_id=0,
            certificate=certificate,
            certificate_signing_request=csr,
            ca=ca,
            chain=[certificate, ca],
            revoked=None,
        )

        logger.debug(
            f"Certificate sans ip: {certificate.sans_ip}, \
                       sans dns: {certificate.sans_dns}"
        )

        return provider_cert, unit_key

    def set_ca(self, ca: Certificate, scope: TLSScope) -> None:
        """Write the CA certificate to the appropriate file for each scope."""
        (self.workload.cassandra_paths.tls_dir / f"{scope.value}-ca.pem").write_text(str(ca))

    def set_certificate(self, crt: Certificate, scope: TLSScope) -> None:
        """Write the unit certificate to the appropriate file for each scope."""
        (self.workload.cassandra_paths.tls_dir / f"{scope.value}-unit.pem").write_text(str(crt))

    def set_bundle(self, ca_list: list[Certificate], scope: TLSScope) -> None:
        """Write the bundle of certificates to a PEM file for each scope."""
        raw_list = [c.raw for c in ca_list]
        (self.workload.cassandra_paths.tls_dir / f"{scope.value}-bundle.pem").write_text(
            str("\n".join(raw_list))
        )

    def set_private_key(self, pk: PrivateKey, scope: TLSScope) -> None:
        """Write the private key to the appropriate file for each scope."""
        (self.workload.cassandra_paths.tls_dir / f"{scope.value}-private.key").write_text(str(pk))

    def set_chain(self, ca_list: list[Certificate], scope: TLSScope) -> None:
        """Write each certificate in the chain to a separate PEM file for each scope."""
        for i, chain_cert in enumerate(ca_list):
            (self.workload.cassandra_paths.tls_dir / f"{scope.value}-bundle{i}.pem").write_text(
                str(chain_cert)
            )

    def get_ca(self, scope: TLSScope) -> Certificate | None:
        """Read and return the CA certificate for the given scope, or None if not found."""
        try:
            return Certificate.from_string(
                (self.workload.cassandra_paths.tls_dir / f"{scope.value}-ca.pem").read_text()
            )
        except Exception as e:
            logger.error(f"can not read CA certificate: {e}")
            return None

    def get_certificate(self, scope: TLSScope) -> Certificate | None:
        """Read and return the unit certificate for the given scope, or None if not found."""
        try:
            return Certificate.from_string(
                (self.workload.cassandra_paths.tls_dir / f"{scope.value}-unit.pem").read_text()
            )
        except Exception as e:
            logger.error(f"can not read certificate {e}")
            return None

    def get_private_key(self, scope: TLSScope) -> PrivateKey | None:
        """Read and return the private key for the given scope.

        Or None if not found.
        """
        try:
            return PrivateKey.from_string(
                (self.workload.cassandra_paths.tls_dir / f"{scope.value}-private.key").read_text()
            )
        except Exception as e:
            logger.error(f"can not read private key: {e}")
            return None

    def get_bundle(self, scope: TLSScope) -> list[Certificate]:
        """Read and return a list of Certificate objects for the given scope.

        Args:
            scope: The TLS scope (e.g., PEER, CLIENT).

        Returns:
            List of Certificate objects parsed from the bundle file.
        """
        bundle = []
        path = self.workload.cassandra_paths.tls_dir / f"{scope.value}-bundle.pem"
        try:
            text = path.read_text()
            cert_blocks = text.split("-----BEGIN CERTIFICATE-----")[1:]

            for cert_block in cert_blocks:
                if "-----END CERTIFICATE-----" in cert_block:
                    pem = f"-----BEGIN CERTIFICATE-----{cert_block}"
                    end_pos = pem.find("-----END CERTIFICATE-----") + len(
                        "-----END CERTIFICATE-----"
                    )
                    pem = pem[:end_pos] + "\n"
                    bundle.append(Certificate.from_string(pem))
        except Exception as e:
            logger.error(f"cannot read bundle: {e}")
        return bundle

    def set_truststore(self, trust_password: str, scope: TLSScope) -> None:
        """Create and set the truststore for the given scope.

        Imports all certificates from the bundle into the truststore file.

        Args:
            trust_password: Password for the truststore.
            scope: The TLS scope (e.g., PEER, CLIENT).

        Raises:
            subprocess.CalledProcessError or ExecError
            if a command fails (unless cert already exists).
        """
        pk, crt, ca, bundle = (
            self.get_private_key(scope),
            self.get_certificate(scope),
            self.get_ca(scope),
            self.get_bundle(scope),
        )

        if not all([pk, crt, ca]):
            logger.debug("Can't set truststore, missing TLS artifacts.")
            return

        trust_aliases = [f"bundle{i}" for i in range(len(bundle))]
        for alias in trust_aliases:
            try:
                self.workload.exec(
                    command=[
                        f"{self.keytool}",
                        "-import",
                        "-alias",
                        alias,
                        "-file",
                        f"{scope.value}-{alias}.pem",
                        "-keystore",
                        f"{scope.value}-truststore.jks",
                        "-storepass",
                        trust_password,
                        "-noprompt",
                    ],
                    cwd=self.workload.cassandra_paths.tls_dir.as_posix(),
                )
                self.workload.exec(
                    f"chown {USER_NAME}:{GROUP} {self.get_truststore_path(scope)}".split()
                )
                self.workload.exec(f"chmod 770 {self.get_truststore_path(scope)}".split())

            except (subprocess.CalledProcessError, ExecError) as e:
                if e.stdout and "already exists" in e.stdout:
                    continue
                logger.error(e.stdout)
                raise e

    def set_keystore(self, keystore_password: str, scope: TLSScope) -> None:
        """Create and set the Java keystore for the given scope using the provided password.

        Exports the private key and certificates into a PKCS#12 keystore file.

        Args:
            keystore_password: Password for the keystore.
            scope: The TLS scope (e.g., PEER, CLIENT).

        Raises:
            subprocess.CalledProcessError or ExecError if a command fails.
        """
        pk, crt, ca = self.get_private_key(scope), self.get_certificate(scope), self.get_ca(scope)
        if not (all([ca, crt, pk])):
            logger.error("Can't set keystore, missing TLS artifacts.")
            return

        try:
            self.workload.exec(
                command=[
                    "openssl",
                    "pkcs12",
                    "-export",
                    "-in",
                    f"{scope.value}-bundle.pem",
                    "-inkey",
                    f"{scope.value}-private.key",
                    "-passin",
                    "pass:",
                    "-certfile",
                    f"{scope.value}-unit.pem",
                    "-out",
                    f"{scope.value}-keystore.p12",
                    "-password",
                    f"pass:{keystore_password}",
                ],
                cwd=self.workload.cassandra_paths.tls_dir.as_posix(),
            )

            self.workload.exec(
                f"chown {USER_NAME}:{GROUP} {self.get_keystore_path(scope)}".split()
            )
            self.workload.exec(f"chmod 770 {self.get_keystore_path(scope)}".split())
        except (subprocess.CalledProcessError, ExecError) as e:
            logger.error(e.stdout)
            raise e

    def configure(
        self, tls_state: ResolvedTLSContext, keystore_password: str, trust_password: str
    ) -> None:
        """Configure all TLS artifacts for the given state and passwords.

        Sets private key, CA, bundle, chain, certificate,
        keystore, and truststore for the given scope.

        Args:
            tls_state: The resolved TLS context containing all required artifacts.
            keystore_password: Password for the keystore.
            trust_password: Password for the truststore.
        """
        scope = tls_state.scope
        self.set_private_key(tls_state.private_key, scope)
        self.set_ca(tls_state.ca, scope)
        self.set_bundle(tls_state.bundle, scope)
        self.set_chain(tls_state.chain, scope)
        self.set_certificate(tls_state.certificate, scope)
        self.set_keystore(keystore_password, scope)
        self.set_truststore(trust_password, scope)

    def import_cert(
        self, alias: str, filename: str, trust_password: str, scope: TLSScope = TLSScope.CLIENT
    ) -> None:
        """Import a certificate into the truststore for the given alias and scope.

        Args:
            alias: Alias for the certificate in the truststore.
            filename: Path to the certificate file to import.
            trust_password: Password for the truststore.
            scope: The TLS scope (default: CLIENT).

        Raises:
            subprocess.CalledProcessError or ExecError
            if a command fails (unless cert already exists).
        """
        try:
            self.workload.exec(
                command=[
                    f"{self.keytool}",
                    "-import",
                    "-alias",
                    alias,
                    "-file",
                    filename,
                    "-keystore",
                    f"{scope.value}-truststore.jks",
                    "-storepass",
                    trust_password,
                    "-noprompt",
                ],
                cwd=self.workload.cassandra_paths.tls_dir.as_posix(),
            )
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                logger.debug(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def remove_cert(
        self, alias: str, trust_password: str, scope: TLSScope = TLSScope.CLIENT
    ) -> None:
        """Remove a cert from the truststore."""
        try:
            self.workload.exec(
                command=[
                    f"{self.keytool}",
                    "-delete",
                    "-v",
                    "-alias",
                    alias,
                    "-keystore",
                    f"{scope.value}-truststore.jks",
                    "-storepass",
                    trust_password,
                    "-noprompt",
                ],
                cwd=self.workload.cassandra_paths.tls_dir.as_posix(),
            )
        except (subprocess.CalledProcessError, ExecError) as e:
            if e.stdout and "does not exist" in e.stdout:
                logger.warning(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def update_cert(
        self,
        alias: str,
        cert: str,
        trust_password: str,
        scope: TLSScope = TLSScope.CLIENT,
    ) -> None:
        """Update a certificate in the truststore."""
        # we should remove the previous cert first.
        # If it doesn't exist, it will not raise an error.
        self.remove_cert(alias=alias, trust_password=trust_password, scope=scope)
        file = self.workload.cassandra_paths.tls_dir / f"{scope.value}-{alias}.pem"
        file.write_text(cert)
        self.import_cert(
            alias=f"{alias}",
            filename=file.as_posix(),
            trust_password=trust_password,
            scope=scope,
        )

    def alias_needs_update(self, alias: str, cert: str) -> bool:
        """Check whether an alias in the truststore requires update."""
        if not cert:
            return False

        return self.certificate_fingerprint(cert) != self.get_trusted_certificates.get(alias, b"")

    def remove_stores(self, scope: TLSScope = TLSScope.CLIENT) -> None:
        """Clean up all keys/certs/stores on a unit."""
        for pattern in ["*.pem", "*.key", "*.p12", "*.jks"]:
            for path in (self.workload.cassandra_paths.tls_dir).glob(f"{scope.value}-{pattern}"):
                logger.debug(f"Removing {path}")
                path.unlink()

    def reload_truststore(self, scope: TLSScope) -> None:
        """Reload the Cassandra truststore for the given TLS scope."""
        if not self.workload.cassandra_paths.get_truststore(scope).exists():
            return

        self.workload.exec([self.nodetool, "reloadssl"])

    def update_truststore(
        self, entities: set[StoreEntry], trust_password: str, scope: TLSScope
    ) -> None:
        """Update the truststore with the given certificates.

        Each entity's certificate is added or updated if needed.
        Reloads the truststore if any changes were made.
        """
        should_reload = False
        live_aliases = set()

        for entity in entities:
            if not self.alias_needs_update(entity.alias, entity.cert.raw):
                continue

            self.update_cert(entity.alias, entity.cert.raw, trust_password, scope)
            live_aliases.add(entity.alias)
            should_reload = True

        if should_reload:
            logger.debug(f"Following aliases should be in the {scope} truststore: {live_aliases}")
            self.reload_truststore(scope)

    @staticmethod
    def certificate_fingerprint(cert: str):
        """Return the certificate fingerprint using SHA-256 algorithm."""
        cert_obj = x509.load_pem_x509_certificate(cert.encode("utf-8"), default_backend())
        hash_algorithm = cert_obj.signature_hash_algorithm or TLSManager.DEFAULT_HASH_ALGORITHM
        return cert_obj.fingerprint(hash_algorithm)

    @staticmethod
    def keytool_hash_to_bytes(hash: str) -> bytes:
        """Convert a hash in the keytool format (AB:CD:0F:...) to a bytes object."""
        return bytes([int(s, 16) for s in hash.split(":")])

    @staticmethod
    def certificate_common_name(cert: str) -> str:
        """Return the certificate Common Name (CN)."""
        cert_obj = x509.load_pem_x509_certificate(cert.encode("utf-8"), default_backend())
        return cert_obj.subject.rfc4514_string()

    @property
    def get_trusted_certificates(self, scope: TLSScope = TLSScope.CLIENT) -> dict[str, bytes]:
        """Returns a mapping of alias to certificate fingerprint (hash).

        For all certificates in the truststore.
        """
        if not (self.workload.cassandra_paths.tls_dir / "truststore.jks").exists():
            logger.warning("Truststore does not exist")
            return {}

        command = [
            f"{self.keytool}",
            "-list",
            "-keystore",
            f"{scope.value}-truststore.jks",
            "-storepass",
            "mytrustpass",
            "-noprompt",
        ]

        stdout, _ = self.workload.exec(
            command=command, cwd=self.workload.cassandra_paths.tls_dir.as_posix()
        )

        # Extract alias and SHA-256 fingerprint from keytool output
        matches = re.findall(
            r"(?m)^(.+?),.*?trustedCertEntry.*?^Certificate fingerprint \(SHA-256\): ([0-9A-F:]{95})",  # noqa: E501
            stdout,
        )

        return {
            alias.strip(): self.keytool_hash_to_bytes(fingerprint)
            for alias, fingerprint in matches
        }
