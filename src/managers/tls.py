# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling tls."""

import logging
import re
import subprocess
from datetime import timedelta
from typing import FrozenSet, List, Optional, Self, Tuple, TypedDict

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

from core.workload import WorkloadBase
from core.state import ApplicationState, TLSContext, TLSScope
from common.cassandra_client import CassandraClient

logger = logging.getLogger(__name__)

Sans = TypedDict("Sans", {"sans_ip": list[str], "sans_dns": list[str]})

USER_NAME = "_daemon_"
GROUP = "root"


class TLSManager:
    """Manage all TLS related events."""

    DEFAULT_HASH_ALGORITHM: hashes.HashAlgorithm = hashes.SHA256()
    SCOPES = (TLSScope.PEER, TLSScope.CLIENT)

    def __init__(self, workload: WorkloadBase):
        self.workload = workload

    def with_client(self, client: CassandraClient) -> Self:
        self.cassandra_client = client
        return self

    def get_truststore_path(self, scope: TLSScope) -> str:
        """Returns the truststore path for the given scope."""
        return (self.workload.cassandra_paths.tls_directory / f"{scope.value}-truststore.jks").as_posix()

    def get_keystore_path(self, scope: TLSScope) -> str:
        """Returns the keystore path for the given scope."""
        return (self.workload.cassandra_paths.tls_directory / f"{scope.value}-keystore.p12").as_posix()

    def generate_internal_ca(self,
        common_name: str,
        ) -> Tuple[Certificate, PrivateKey]:
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

    def generate_internal_credentials(self,
        ca: Certificate,
        ca_key: PrivateKey,
        common_name: str,
        sans_ip: Optional[FrozenSet[str]],
        sans_dns: Optional[FrozenSet[str]],
        ) -> Tuple[List[ProviderCertificate], Optional[PrivateKey]]:

        logger.debug(f"sans_ip contents: {[] if not sans_ip else [repr(i) for i in sans_ip]}")
        logger.debug(f"sans_dns contents: {[] if not sans_dns else [repr(i) for i in sans_dns]}")        

        csr = generate_csr(
            private_key=ca_key,
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
        
        return [provider_cert], ca_key


    def set_ca(self, ca: Certificate) -> None:
        for scope in self.SCOPES:
            (self.workload.cassandra_paths.tls_directory / f"{scope.value}-ca.pem").write_text(
                str(ca)
            )

    def set_certificate(self, crt: Certificate) -> None:
        for scope in self.SCOPES:
            (self.workload.cassandra_paths.tls_directory / f"{scope.value}-unit.pem").write_text(
                str(crt)
            )

    def set_bundle(self, ca_list: List[Certificate]) -> None:
        raw_list = [c.raw for c in ca_list]
        
        for scope in self.SCOPES:
            (self.workload.cassandra_paths.tls_directory / f"{scope.value}-bundle.pem").write_text(
                str("\n".join(raw_list))
            )        

    def set_private_key(self, pk: PrivateKey) -> None:
        for scope in self.SCOPES:
            (self.workload.cassandra_paths.tls_directory / f"{scope.value}-private.key").write_text(str(pk))

    def set_chain(self, ca_list: List[Certificate]) -> None:
        """Sets the unit chain."""
        for scope in self.SCOPES:
            for i, chain_cert in enumerate(ca_list):
                (self.workload.cassandra_paths.tls_directory / f"{scope.value}-bundle{i}.pem").write_text(str(chain_cert))

    def get_ca(self, scope: TLSScope) -> Optional[Certificate]:
        try:
            return Certificate.from_string((self.workload.cassandra_paths.tls_directory / f"{scope.value}-ca.pem").read_text())
        except Exception as e:
            logger.error(f"can not read CA certificate: {e}")
            return None

    def get_certificate(self, scope: TLSScope) -> Optional[Certificate]:
        try:
            return Certificate.from_string((self.workload.cassandra_paths.tls_directory / f"{scope.value}-unit.pem").read_text())
        except Exception as e:
            logger.error(f"can not read certificate {e}")
            return None

    def get_private_key(self, scope: TLSScope) -> Optional[PrivateKey]:
        try:        
            return PrivateKey.from_string((self.workload.cassandra_paths.tls_directory / f"{scope.value}-private.key").read_text())
        except Exception as e:
            logger.error(f"can not read private key: {e}")
            return None

    def get_bundle(self, scope: TLSScope) -> List[Certificate]:
        bundle = []
        path = self.workload.cassandra_paths.tls_directory / f"{scope.value}-bundle.pem"
        try:
            text = path.read_text()
            cert_blocks = text.split('-----BEGIN CERTIFICATE-----')[1:]
            
            for cert_block in cert_blocks:
                if '-----END CERTIFICATE-----' in cert_block:
                    pem = f"-----BEGIN CERTIFICATE-----{cert_block}"
                    end_pos = pem.find('-----END CERTIFICATE-----') + len('-----END CERTIFICATE-----')
                    pem = pem[:end_pos] + '\n'
                    bundle.append(Certificate.from_string(pem))
        except Exception as e:
            logger.error(f"cannot read bundle: {e}")
        return bundle

    def set_truststore(self, trust_password: str) -> None:
        for scope in self.SCOPES:
            pk, crt, ca, bundle = self.get_private_key(scope), self.get_certificate(scope), self.get_ca(scope), self.get_bundle(scope)
            
            if not all([pk, crt, ca]):
                logger.debug("Can't set truststore, missing TLS artifacts.")
                return
            
            trust_aliases = [f"bundle{i}" for i in range(len(bundle))]
            for alias in trust_aliases:
              try:
                self.workload.exec(
                    command=[
                        "charmed-cassandra.keytool",
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
                    cwd=self.workload.cassandra_paths.tls_directory.as_posix(),
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

    def set_keystore(self, pk_password: str, keystore_password: str) -> None:
        for scope in self.SCOPES:        
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
                        f"pass:{pk_password}",
                        "-certfile",
                        f"{scope.value}-unit.pem",
                        "-out",
                        f"{scope.value}-keystore.p12",
                        "-password",
                        f"pass:{keystore_password}",
                    ],
                cwd=self.workload.cassandra_paths.tls_directory.as_posix(),
                )

                self.workload.exec(
                  f"chown {USER_NAME}:{GROUP} {self.get_keystore_path(scope)}".split()
                  )
                self.workload.exec(f"chmod 770 {self.get_keystore_path(scope)}".split())
            except (subprocess.CalledProcessError, ExecError) as e:
                logger.error(e.stdout)
                raise e                

    def configure(self,
                  pk: PrivateKey,
                  ca: Certificate,
                  chain: List[Certificate],
                  certificate: Certificate,
                  bundle: List[Certificate],
                  pk_password: str,
                  keystore_password: str,
                  trust_password: str,
                  ) -> None:
        self.set_private_key(pk)
        self.set_ca(ca)
        self.set_bundle(chain) #TODO: explore the way in done in kafka with [certificate, ca] + chain
        self.set_chain(chain)
        self.set_certificate(certificate)
        self.set_keystore(pk_password, keystore_password)
        self.set_truststore(trust_password)

    def import_cert(self, alias: str, filename: str, trust_password: str, scope: TLSScope = TLSScope.CLIENT) -> None:
        try:
            self.workload.exec(
                command=[
                    "charmed-cassandra.keytool",
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
                cwd=self.workload.cassandra_paths.tls_directory.as_posix(),
            )
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                logger.debug(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def reload_truststore(self, scope: TLSScope = TLSScope.CLIENT) -> None:
        """Reloads the truststore using `mgmt-api`."""
        if not (self.workload.cassandra_paths.tls_directory / f"{scope.value}-truststore.jks").exists():
            logger.warning("Truststore does not exist")
            return

        # TODO: nodetool reloadssl

        logger.debug("Truststore reloaded")
        return

    def remove_cert(self, alias: str, trust_password: str, scope: TLSScope = TLSScope.CLIENT) -> None:
        """Remove a cert from the truststore."""
        try:
            self.workload.exec(
                command=[
                    "charmed-cassandra.keytool",
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
                cwd=self.workload.cassandra_paths.tls_directory.as_posix(),
            )
        except (subprocess.CalledProcessError, ExecError) as e:
            if e.stdout and "does not exist" in e.stdout:
                logger.warning(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def update_cert(self, alias: str, cert: str, trust_password: str, scope: TLSScope = TLSScope.CLIENT) -> None:
        """Update a certificate in the truststore."""
        # we should remove the previous cert first. If it doesn't exist, it will not raise an error.
        self.remove_cert(alias=alias, trust_password=trust_password, scope=scope)
        file = (self.workload.cassandra_paths.tls_directory / f"{scope.value}-{alias}.pem")
        file.write_text(cert)
        self.import_cert(alias=f"{alias}", filename=file.as_posix(), trust_password=trust_password, scope=scope)

    def alias_needs_update(self, alias: str, cert: str) -> bool:
        """Checks whether an alias in the truststore requires update based on the provided certificate."""
        if not cert:
            return False

        return self.certificate_fingerprint(cert) != self.get_trusted_certificates.get(alias, b"")

    def remove_stores(self, scope: TLSScope = TLSScope.CLIENT) -> None:
        """Cleans up all keys/certs/stores on a unit."""
        for pattern in ["*.pem", "*.key", "*.p12", "*.jks"]:
            for path in (self.workload.cassandra_paths.tls_directory).glob(
                f"{scope.value}-{pattern}"
            ):
                logger.debug(f"Removing {path}")
                path.unlink()

    @staticmethod
    def certificate_fingerprint(cert: str):
        """Returns the certificate fingerprint using SHA-256 algorithm."""
        cert_obj = x509.load_pem_x509_certificate(cert.encode("utf-8"), default_backend())
        hash_algorithm = cert_obj.signature_hash_algorithm or TLSManager.DEFAULT_HASH_ALGORITHM
        return cert_obj.fingerprint(hash_algorithm)

    @staticmethod
    def keytool_hash_to_bytes(hash: str) -> bytes:
        """Converts a hash in the keytool format (AB:CD:0F:...) to a bytes object."""
        return bytes([int(s, 16) for s in hash.split(":")])

    @staticmethod
    def certificate_common_name(cert: str) -> str:
        """Returns the certificate Common Name (CN)."""
        cert_obj = x509.load_pem_x509_certificate(cert.encode("utf-8"), default_backend())
        return cert_obj.subject.rfc4514_string()

    @property
    def get_trusted_certificates(self, scope: TLSScope = TLSScope.CLIENT) -> dict[str, bytes]:
        """Returns a mapping of alias to certificate fingerprint (hash) for all certificates in the truststore."""
        if not (self.workload.cassandra_paths.tls_directory / "truststore.jks").exists():
            logger.warning("Truststore does not exist")
            return {}

        command = [
            "charmed-cassandra.keytool",
            "-list",
            "-keystore",
            f"{scope.value}-truststore.jks",
            "-storepass",
            "mytrustpass",
            "-noprompt",
        ]

        stdout, _ = self.workload.exec(
            command=command, cwd=self.workload.cassandra_paths.tls_directory.as_posix()
        )

        # Extract alias and SHA-256 fingerprint from keytool output
        matches = re.findall(
            r"(?m)^(.+?),.*?trustedCertEntry.*?^Certificate fingerprint \(SHA-256\): ([0-9A-F:]{95})",
            stdout,
        )

        return {
            alias.strip(): self.keytool_hash_to_bytes(fingerprint)
            for alias, fingerprint in matches
        }


def setup_internal_ca(tls_manager: TLSManager, state: ApplicationState) -> None:
        if not state.unit.unit.is_leader():
            return

        ca, pk = tls_manager.generate_internal_ca(common_name=state.unit.unit.app.name)

        state.cluster.internal_ca = ca
        state.cluster.internal_ca_key = pk
    
def setup_internal_credentials(
        tls_manager: TLSManager,
        state: ApplicationState,
        sans_ip: Optional[FrozenSet[str]],
        sans_dns: Optional[FrozenSet[str]],
        is_leader: bool,
) -> None:
        ca = state.cluster.internal_ca
        ca_key = state.cluster.internal_ca_key
        
        if ca is None or ca_key is None:
            logger.error("Internal CA is not set up yet.")
            return

        if state.unit.peer_tls.ready:
            logger.debug("No need to set up internal credentials...")
            _configure_internal_tls(tls_manager, state.unit.peer_tls)
            return

        provider_crt, pk = tls_manager.generate_internal_credentials(
            ca=ca,
            ca_key=ca_key,
            common_name=state.unit.unit.app.name,
            sans_ip=sans_ip,
            sans_dns=sans_dns,
        )

        if not pk:
            logger.error("private key for internal tls is empty")
            return

        state.unit.peer_tls.certificate = provider_crt[0].certificate
        state.unit.peer_tls.csr = provider_crt[0].certificate_signing_request
        state.unit.peer_tls.private_key = pk
        state.unit.peer_tls.ca = ca
        state.unit.peer_tls.chain = provider_crt[0].chain

        _configure_internal_tls(tls_manager, state.unit.peer_tls)

        if is_leader:
            state.cluster.peer_cluster_ca = state.unit.peer_tls.bundle
    

def _configure_internal_tls(tls_manager: TLSManager, state: TLSContext) -> None:
    if not state.ready:
        return

    resolved = state.resolved()
    
    tls_manager.configure(
        pk=resolved.private_key,
        ca=resolved.ca,
        chain=resolved.chain,
        certificate=resolved.certificate,
        bundle=resolved.bundle,
        pk_password="",
        keystore_password="myStorePass",
        trust_password="myStorePass",
    )            
