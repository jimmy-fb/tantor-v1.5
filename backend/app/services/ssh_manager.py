import io
import time
import threading
from contextlib import contextmanager

import paramiko

from app.services.crypto import decrypt

# Connection pool: reuse SSH connections within a time window
_pool: dict[str, tuple[paramiko.SSHClient, float]] = {}
_pool_lock = threading.Lock()
_POOL_TTL = 120  # seconds to keep idle connections


def _pool_key(ip: str, port: int, username: str) -> str:
    return f"{username}@{ip}:{port}"


def _cleanup_pool():
    """Remove stale connections from the pool."""
    now = time.time()
    stale = [k for k, (_, ts) in _pool.items() if now - ts > _POOL_TTL]
    for k in stale:
        try:
            _pool[k][0].close()
        except Exception:
            pass
        del _pool[k]


class SSHManager:
    @staticmethod
    @contextmanager
    def connect(ip_address: str, port: int, username: str, auth_type: str, encrypted_credential: str):
        """Context manager that yields a connected Paramiko SSHClient.

        Uses a connection pool to reuse existing connections for up to 120s.
        """
        key = _pool_key(ip_address, port, username)

        # Step 1: try to grab a live pooled connection. If the keepalive
        # ping fails the connection is dead — drop it and fall through to
        # the "create new" path. CRITICAL: any exception raised here must
        # come from the keepalive itself, NOT from the caller's `with`
        # block. Previously we wrapped `yield client` in the same try and
        # any caller-side exception got swallowed, causing the generator
        # to yield twice → "generator didn't stop after throw()" surfaced
        # to the user every time a config change failed.
        client: paramiko.SSHClient | None = None
        with _pool_lock:
            _cleanup_pool()
            if key in _pool:
                pooled, _ = _pool[key]
                try:
                    pooled.get_transport().send_ignore()
                    _pool[key] = (pooled, time.time())
                    client = pooled
                except Exception:
                    try:
                        pooled.close()
                    except Exception:
                        pass
                    del _pool[key]
                    client = None

        # Step 2: open a fresh connection if we don't have a live pooled one
        if client is None:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            credential = decrypt(encrypted_credential)
            try:
                if auth_type == "password":
                    client.connect(
                        hostname=ip_address, port=port, username=username,
                        password=credential, timeout=15,
                        look_for_keys=False, allow_agent=False,
                    )
                else:
                    pkey = paramiko.RSAKey.from_private_key(io.StringIO(credential))
                    client.connect(
                        hostname=ip_address, port=port, username=username,
                        pkey=pkey, timeout=15,
                        look_for_keys=False, allow_agent=False,
                    )
            except Exception:
                client.close()
                raise
            with _pool_lock:
                _pool[key] = (client, time.time())

        # Step 3: hand the live client to the caller. Any exception they
        # raise propagates out untouched — we never yield twice.
        yield client

    @staticmethod
    def exec_command(client: paramiko.SSHClient, command: str, timeout: int = 30) -> tuple[int, str, str]:
        """Execute a command and return (exit_code, stdout, stderr)."""
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        return exit_code, stdout.read().decode().strip(), stderr.read().decode().strip()

    @staticmethod
    def upload_content(client: paramiko.SSHClient, content: str, remote_path: str):
        """Upload string content to a remote file via SFTP."""
        sftp = client.open_sftp()
        try:
            with sftp.file(remote_path, "w") as f:
                f.write(content)
        finally:
            sftp.close()

    @staticmethod
    def test_connection(ip_address: str, port: int, username: str, auth_type: str, encrypted_credential: str) -> tuple[bool, str, str | None]:
        """Test SSH connectivity. Returns (success, message, os_info)."""
        try:
            with SSHManager.connect(ip_address, port, username, auth_type, encrypted_credential) as client:
                _, os_info, _ = SSHManager.exec_command(client, "cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2 | tr -d '\"'")
                return True, "Connection successful", os_info or None
        except paramiko.AuthenticationException:
            return False, "Authentication failed. Check credentials.", None
        except paramiko.SSHException as e:
            return False, f"SSH error: {e}", None
        except TimeoutError:
            return False, "Connection timed out", None
        except Exception as e:
            return False, f"Connection failed: {e}", None


ssh_manager = SSHManager()
