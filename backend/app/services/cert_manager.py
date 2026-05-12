"""Per-cluster TLS certificate authority + broker keystore + client cert issuance.

Uses the `cryptography` library (already in requirements for Fernet) — no
separate openssl binary, no Java keytool.

Layout on disk:
  /var/lib/tantor/certs/{cluster_id}/
    ca.crt                 PEM CA cert
    ca.key                 PEM CA private key (mode 0600)
    serial.txt             monotonic serial counter
    brokers/{ip}_{node_id}/
        broker.crt         PEM broker cert
        broker.key         PEM broker private key
        broker.p12         PKCS12 keystore (cert + key, password = cluster.tls_password)
        truststore.p12     PKCS12 truststore (CA only, same password)
    clients/{name}/
        client.crt         PEM
        client.key         PEM
        client.p12         PKCS12 (cert + key)

Tantor regenerates broker keystores from the live CA + per-broker key on
every deploy so you can rotate the CA by deleting `ca.crt` / `ca.key` —
the next deploy will re-mint everything.
"""
from __future__ import annotations

import datetime as dt
import ipaddress
import logging
import secrets as pysecrets
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from app.config import settings
from app.models.cluster import Cluster
from app.services.crypto import decrypt as fernet_decrypt, encrypt as fernet_encrypt

logger = logging.getLogger("tantor.certs")


CERTS_BASE = Path("/var/lib/tantor/certs")


# ── Helpers ────────────────────────────────────────────────────────────────


def _cluster_dir(cluster_id: str) -> Path:
    p = CERTS_BASE / cluster_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _next_serial(cluster_id: str) -> int:
    serial_file = _cluster_dir(cluster_id) / "serial.txt"
    if serial_file.exists():
        n = int(serial_file.read_text().strip()) + 1
    else:
        n = 1
    serial_file.write_text(str(n))
    return n


def _write_secure(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


# ── CA ─────────────────────────────────────────────────────────────────────


# ── User-uploaded CA / certs (v1.4.0 #8) ───────────────────────────────


def list_cluster_certificates(cluster: Cluster) -> dict:
    """Inventory of cert files stored for this cluster.

    Returns {ca: {present, fingerprint, not_after}, broker_certs: [...]}.
    Used by the cluster Security tab so operators can see what Tantor
    is currently using to mint broker keystores.
    """
    cdir = _cluster_dir(cluster.id)
    out: dict = {"cluster_id": cluster.id, "ca": {"present": False}, "broker_certs": []}

    ca_path = cdir / "ca.crt"
    if ca_path.exists():
        try:
            cert_data = ca_path.read_bytes()
            cert = x509.load_pem_x509_certificate(cert_data)
            out["ca"] = {
                "present": True,
                "subject": cert.subject.rfc4514_string(),
                "fingerprint_sha256": cert.fingerprint(hashes.SHA256()).hex(),
                "not_before": cert.not_valid_before_utc.isoformat(),
                "not_after": cert.not_valid_after_utc.isoformat(),
                "uploaded": (cdir / "ca.upload.marker").exists(),
            }
        except Exception as e:
            out["ca"] = {"present": True, "error": str(e)}

    # broker certs are minted on-the-fly into pkcs12 files
    for f in cdir.glob("broker-*.p12"):
        out["broker_certs"].append({
            "filename": f.name,
            "size_bytes": f.stat().st_size,
        })

    return out


def upload_cluster_ca(cluster: Cluster, ca_cert_pem: bytes, ca_key_pem: bytes | None) -> dict:
    """Replace the cluster's CA with operator-supplied PEM material.

    v1.4.0 #8 — instead of using Tantor's auto-generated CA the
    operator can upload their own CA cert + key, and Tantor will sign
    broker certs with it. If only the cert is supplied (no key) we
    keep it as a truststore-only CA; broker keystores will still be
    issued by the auto-generated internal CA.
    """
    # Validate the cert parses
    try:
        cert = x509.load_pem_x509_certificate(ca_cert_pem)
    except Exception as e:
        raise ValueError(f"Invalid CA certificate PEM: {e}")
    if ca_key_pem:
        try:
            serialization.load_pem_private_key(ca_key_pem, password=None)
        except Exception as e:
            raise ValueError(f"Invalid CA private key PEM: {e}")

    cdir = _cluster_dir(cluster.id)
    (cdir / "ca.crt").write_bytes(ca_cert_pem)
    if ca_key_pem:
        _write_secure(cdir / "ca.key", ca_key_pem)
    # Drop a marker so list_cluster_certificates can label this as
    # operator-supplied vs Tantor-issued.
    (cdir / "ca.upload.marker").write_text(
        f"uploaded by user; subject={cert.subject.rfc4514_string()}\n"
    )
    return {
        "uploaded": True,
        "subject": cert.subject.rfc4514_string(),
        "has_key": bool(ca_key_pem),
    }


def ensure_cluster_ca(cluster: Cluster) -> tuple[bytes, bytes]:
    """Return (ca_cert_pem, ca_key_pem). Generate + persist on first call."""
    cdir = _cluster_dir(cluster.id)
    cert_path = cdir / "ca.crt"
    key_path = cdir / "ca.key"
    if cert_path.exists() and key_path.exists():
        return cert_path.read_bytes(), key_path.read_bytes()

    logger.info("Generating cluster CA for %s", cluster.id)
    key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    name = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, f"Tantor CA · {cluster.name}"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Tantor"),
    ])
    now = dt.datetime.now(dt.timezone.utc)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=365 * 10))
        .add_extension(x509.BasicConstraints(ca=True, path_length=1), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=True,
                crl_sign=True, encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    cert_path.write_bytes(cert_pem)
    _write_secure(key_path, key_pem)
    return cert_pem, key_pem


def load_ca(cluster: Cluster) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    cert_pem, key_pem = ensure_cluster_ca(cluster)
    cert = x509.load_pem_x509_certificate(cert_pem)
    key = serialization.load_pem_private_key(key_pem, password=None)
    return cert, key


# ── Cluster TLS password (Fernet-encrypted on the cluster row) ────────────


def _ensure_tls_password(cluster: Cluster, db) -> str:
    """All keystores in a cluster share one password — simplifies Ansible."""
    if cluster.encrypted_tls_password:
        return fernet_decrypt(cluster.encrypted_tls_password)
    pw = pysecrets.token_urlsafe(32)
    cluster.encrypted_tls_password = fernet_encrypt(pw)
    db.commit()
    return pw


def get_tls_password(cluster: Cluster) -> str | None:
    if not cluster.encrypted_tls_password:
        return None
    return fernet_decrypt(cluster.encrypted_tls_password)


# ── Issuing certs ─────────────────────────────────────────────────────────


def _issue_signed_cert(
    ca_cert: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    common_name: str,
    san_dns: list[str],
    san_ips: list[str],
    is_server: bool,
    days_valid: int = 365,
) -> tuple[bytes, bytes]:
    """Issue an end-entity cert + private key signed by the cluster CA."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Tantor"),
    ])
    san_entries: list[x509.GeneralName] = [x509.DNSName(d) for d in san_dns]
    for ip in san_ips:
        try:
            san_entries.append(x509.IPAddress(ipaddress.ip_address(ip)))
        except ValueError:
            san_entries.append(x509.DNSName(ip))
    now = dt.datetime.now(dt.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=days_valid))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName(san_entries) if san_entries else x509.SubjectAlternativeName([]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_cert.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=True,
                data_encipherment=False, key_agreement=False, key_cert_sign=False,
                crl_sign=False, encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [x509.oid.ExtendedKeyUsageOID.SERVER_AUTH, x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
                if is_server else [x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
    )
    cert = builder.sign(ca_key, hashes.SHA256())
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def _make_pkcs12(name: str, key_pem: bytes, cert_pem: bytes, ca_pem: bytes, password: str) -> bytes:
    key = serialization.load_pem_private_key(key_pem, password=None)
    cert = x509.load_pem_x509_certificate(cert_pem)
    ca = x509.load_pem_x509_certificate(ca_pem)
    return pkcs12.serialize_key_and_certificates(
        name=name.encode(),
        key=key,
        cert=cert,
        cas=[ca],
        encryption_algorithm=serialization.BestAvailableEncryption(password.encode()),
    )


def _make_truststore_pem(ca_pem: bytes) -> bytes:
    """Truststore as a plain PEM file — Kafka 4.x supports `ssl.truststore.type=PEM`.

    History: we tried PKCS12 first but `cryptography.pkcs12.serialize_key_and_certificates`
    can't produce a Java-compatible truststore in one shot. With cas=[ca]
    Java sees no TrustedCertEntries and rejects with `trustAnchors must
    be non-empty`. With cert=ca + key=None Java needs NoEncryption (the
    library refuses encryption without a key), and Kafka then fails
    integrity check on the unencrypted MAC-less keystore. PEM avoids both
    issues — single-file, no password, native Kafka support.
    """
    return ca_pem


# ── Public API used by deployer ───────────────────────────────────────────


def materialize_broker_keystores(cluster: Cluster, db, broker_infos: list[dict]) -> dict[str, dict]:
    """Generate (or reuse) keystore + truststore for every broker.

    Returns a map { "<ip>_<node_id>": {"keystore": Path, "truststore": Path,
    "password": str} } so the deployer can ship them via Ansible.
    """
    if not cluster.ssl_enabled:
        return {}
    cdir = _cluster_dir(cluster.id)
    brokers_dir = cdir / "brokers"
    brokers_dir.mkdir(exist_ok=True)
    ca_cert, ca_key = load_ca(cluster)
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    password = _ensure_tls_password(cluster, db)

    out: dict[str, dict] = {}
    for info in broker_infos:
        if info["role"] not in ("broker", "broker_controller", "controller"):
            continue
        bkey = f"{info['ip_address']}_{info['node_id']}"
        bdir = brokers_dir / bkey
        bdir.mkdir(parents=True, exist_ok=True)
        cert_path = bdir / "broker.crt"
        key_path = bdir / "broker.key"
        ks_path = bdir / "broker.p12"
        ts_path = bdir / "truststore.p12"

        # Reissue if missing (or if we want rotation later, just delete the dir).
        if not (cert_path.exists() and key_path.exists()):
            cert_pem, key_pem = _issue_signed_cert(
                ca_cert, ca_key,
                common_name=f"kafka-broker-{info['node_id']}",
                san_dns=["localhost"],
                san_ips=[info["ip_address"]],
                is_server=True,
                days_valid=365 * 2,
            )
            cert_path.write_bytes(cert_pem)
            _write_secure(key_path, key_pem)
        else:
            cert_pem = cert_path.read_bytes()
            key_pem = key_path.read_bytes()

        # Always rebuild — they're fast and the password may have rotated.
        _write_secure(ks_path, _make_pkcs12(f"broker-{info['node_id']}", key_pem, cert_pem, ca_pem, password))
        # Switched truststore to PEM (single CA cert) for Java compat — see _make_truststore_pem.
        ts_pem_path = bdir / "truststore.pem"
        _write_secure(ts_pem_path, _make_truststore_pem(ca_pem))

        out[bkey] = {
            "keystore": str(ks_path),
            "truststore": str(ts_pem_path),
            "password": password,
        }
    return out


def issue_client_cert(cluster: Cluster, db, common_name: str, ttl_days: int = 365) -> dict:
    """Mint + persist a client cert. Returns the bundle for download."""
    if not cluster.ssl_enabled:
        raise ValueError("SSL is not enabled on this cluster")
    ca_cert, ca_key = load_ca(cluster)
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    cert_pem, key_pem = _issue_signed_cert(
        ca_cert, ca_key,
        common_name=common_name,
        san_dns=[],
        san_ips=[],
        is_server=False,
        days_valid=ttl_days,
    )
    password = _ensure_tls_password(cluster, db)
    cdir = _cluster_dir(cluster.id) / "clients" / common_name
    cdir.mkdir(parents=True, exist_ok=True)
    (cdir / "client.crt").write_bytes(cert_pem)
    _write_secure(cdir / "client.key", key_pem)
    p12 = _make_pkcs12(common_name, key_pem, cert_pem, ca_pem, password)
    _write_secure(cdir / "client.p12", p12)
    return {
        "common_name": common_name,
        "ca_pem": ca_pem.decode(),
        "cert_pem": cert_pem.decode(),
        "key_pem": key_pem.decode(),
        "p12_password": password,
        "issued_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "expires_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=ttl_days)).isoformat(),
    }


def list_client_certs(cluster: Cluster) -> list[dict]:
    cdir = _cluster_dir(cluster.id) / "clients"
    if not cdir.exists():
        return []
    out = []
    for entry in sorted(cdir.iterdir()):
        cert_file = entry / "client.crt"
        if not cert_file.exists():
            continue
        cert = x509.load_pem_x509_certificate(cert_file.read_bytes())
        out.append({
            "common_name": entry.name,
            "issued_at": cert.not_valid_before_utc.isoformat(),
            "expires_at": cert.not_valid_after_utc.isoformat(),
            "serial_number": str(cert.serial_number),
        })
    return out


def revoke_client_cert(cluster: Cluster, common_name: str) -> bool:
    cdir = _cluster_dir(cluster.id) / "clients" / common_name
    if not cdir.exists():
        return False
    # No CRL — just delete the local copy. Real revocation would need a CRL or OCSP.
    import shutil
    shutil.rmtree(cdir)
    return True


def get_ca_pem(cluster: Cluster) -> bytes:
    ca_pem, _ = ensure_cluster_ca(cluster)
    return ca_pem
