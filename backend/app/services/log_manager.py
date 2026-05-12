"""Service log fetching via SSH — journalctl with fallback to log files."""

import logging

from app.models.host import Host
from app.services.ssh_manager import SSHManager

logger = logging.getLogger("tantor.logs")

# Map service roles to systemd unit names
SERVICE_UNIT_MAP = {
    "broker": "kafka",
    "broker_controller": "kafka",
    "controller": "kafka-kraft-controller",
    "zookeeper": "kafka",
    "ksqldb": "ksqldb",
    "kafka_connect": "kafka-connect",
}

# Fallback: map service roles to log file paths (used when journalctl is unavailable)
SERVICE_LOG_FILE_MAP = {
    "broker": "/opt/kafka/logs/server.log",
    "broker_controller": "/opt/kafka/logs/server.log",
    "controller": "/opt/kafka/logs/controller.log",
    "zookeeper": "/opt/kafka/logs/server.log",
    "ksqldb": "/opt/ksqldb/logs/ksql.log",
    "kafka_connect": "/opt/kafka/logs/connect.log",
}


class LogManager:
    """Fetches and streams service logs from cluster hosts.

    Tries journalctl first; falls back to reading log files directly
    (necessary in Docker containers that use a systemctl shim).
    """

    @staticmethod
    def _build_journalctl_cmd(unit_name: str, lines: int, since: str | None,
                               priority: str | None, grep_filter: str | None) -> str:
        # Use sudo -n: ec2-user / tantor can SSH but generally can't read
        # other users' systemd journals without elevation. -n means
        # non-interactive — fail rather than prompt for a password.
        cmd = f"sudo -n journalctl -u {unit_name}.service --no-pager -n {lines} --output=short-iso"
        if since:
            cmd += f' --since="{since}"'
        if priority:
            cmd += f" -p {priority}"
        if grep_filter:
            safe_filter = grep_filter.replace("'", "'\\''")
            cmd += f" | grep -i '{safe_filter}'"
        return cmd

    @staticmethod
    def _build_logfile_cmd(log_path: str, lines: int, grep_filter: str | None) -> str:
        # sudo for the same reason — kafka.service's log files are owned
        # kafka:kafka mode 640 by default.
        cmd = f"sudo -n tail -n {lines} {log_path} 2>/dev/null"
        if grep_filter:
            safe_filter = grep_filter.replace("'", "'\\''")
            cmd += f" | grep -i '{safe_filter}'"
        return cmd

    @staticmethod
    def get_logs(
        host: Host,
        service_role: str,
        lines: int = 200,
        since: str | None = None,
        priority: str | None = None,
        grep_filter: str | None = None,
        unit_override: str | None = None,
        kafka_install_dir: str | None = None,
    ) -> dict:
        """Fetch historical logs via SSH.

        Strategy: try journalctl first; if it fails (not found), fall back to
        reading the service's log file via tail.

        unit_override + kafka_install_dir let the caller supply the
        per-cluster systemd unit (e.g. kafka-prod-1ac9bbbe.service) and
        Kafka install dir so multi-cluster deployments don't all read from
        the legacy /opt/kafka path. Falls back to the role-derived defaults.
        """
        if unit_override:
            # The override is the FULL unit name (e.g. "kafka-prod-XYZ.service");
            # _build_journalctl_cmd appends ".service" so strip it here.
            unit_name = unit_override.removesuffix(".service")
        else:
            unit_name = SERVICE_UNIT_MAP.get(service_role, "kafka")
        if kafka_install_dir and service_role in ("broker", "broker_controller", "controller", "zookeeper"):
            log_path = f"{kafka_install_dir}/logs/server.log"
            if service_role == "controller":
                log_path = f"{kafka_install_dir}/logs/controller.log"
        else:
            log_path = SERVICE_LOG_FILE_MAP.get(service_role, "/opt/kafka/logs/server.log")

        try:
            with SSHManager.connect(
                host.ip_address, host.ssh_port, host.username,
                host.auth_type, host.encrypted_credential,
            ) as client:
                # Try journalctl first
                jctl_cmd = LogManager._build_journalctl_cmd(unit_name, lines, since, priority, grep_filter)
                exit_code, stdout, stderr = SSHManager.exec_command(client, jctl_cmd, timeout=30)

                # If journalctl not found or failed, fall back to log files
                if exit_code != 0 and ("not found" in stderr.lower() or "no such file" in stderr.lower()
                                       or "command not found" in stderr.lower()):
                    logger.info(f"journalctl unavailable on {host.ip_address}, falling back to log file")
                    file_cmd = LogManager._build_logfile_cmd(log_path, lines, grep_filter)
                    exit_code, stdout, stderr = SSHManager.exec_command(client, file_cmd, timeout=30)

                log_lines = stdout.splitlines() if stdout else []
                if exit_code != 0 and not log_lines:
                    log_lines = [f"Error fetching logs: {stderr}"]
                return {
                    "host_ip": host.ip_address,
                    "hostname": host.hostname,
                    "role": service_role,
                    "lines": log_lines,
                    "line_count": len(log_lines),
                }
        except Exception as e:
            logger.error(f"Failed to fetch logs from {host.ip_address}: {e}")
            return {
                "host_ip": host.ip_address,
                "hostname": host.hostname,
                "role": service_role,
                "lines": [f"SSH connection failed: {e}"],
                "line_count": 1,
            }

    @staticmethod
    def tail_logs(host: Host, service_role: str, unit_override: str | None = None,
                  kafka_install_dir: str | None = None):
        """Generator that yields log lines in real-time.

        Tries journalctl -f first; falls back to tail -f on the log file.
        Used by the WebSocket endpoint for live tailing. Per-cluster
        unit_override / kafka_install_dir keep this working when the
        cluster's systemd unit isn't named "kafka.service" (multi-cluster
        deployments — v1.2.0 #5).
        """
        if unit_override:
            unit_name = unit_override.removesuffix(".service")
        else:
            unit_name = SERVICE_UNIT_MAP.get(service_role, "kafka")
        if kafka_install_dir and service_role in ("broker", "broker_controller", "controller", "zookeeper"):
            log_path = f"{kafka_install_dir}/logs/server.log"
            if service_role == "controller":
                log_path = f"{kafka_install_dir}/logs/controller.log"
        else:
            log_path = SERVICE_LOG_FILE_MAP.get(service_role, "/opt/kafka/logs/server.log")

        # Build command that tries journalctl, falls back to tail -f.
        # sudo -n needed for both since journal + log files are owned by
        # the kafka user, not the SSH user.
        cmd = (
            f"if command -v journalctl >/dev/null 2>&1; then "
            f"sudo -n journalctl -u {unit_name}.service -f --no-pager --output=short-iso; "
            f"else sudo -n tail -f {log_path} 2>/dev/null; fi"
        )

        client = None
        try:
            import io
            import paramiko
            from app.services.crypto import decrypt

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            credential = decrypt(host.encrypted_credential)

            if host.auth_type == "password":
                client.connect(
                    hostname=host.ip_address, port=host.ssh_port,
                    username=host.username, password=credential,
                    timeout=15, look_for_keys=False, allow_agent=False,
                )
            else:
                pkey = paramiko.RSAKey.from_private_key(io.StringIO(credential))
                client.connect(
                    hostname=host.ip_address, port=host.ssh_port,
                    username=host.username, pkey=pkey,
                    timeout=15, look_for_keys=False, allow_agent=False,
                )

            _, stdout, _ = client.exec_command(cmd, get_pty=True)
            for line in stdout:
                yield line.strip()
        except Exception as e:
            logger.error(f"Log tail failed for {host.ip_address}: {e}")
            yield f"Error: {e}"
        finally:
            if client:
                client.close()


log_manager = LogManager()
