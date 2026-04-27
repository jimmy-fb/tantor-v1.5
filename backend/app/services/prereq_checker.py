import re

import paramiko

from app.services.ssh_manager import SSHManager


class PrereqChecker:
    """Runs prerequisite checks on a remote host via SSH."""

    @staticmethod
    def check_os(client: paramiko.SSHClient) -> dict:
        _, stdout, _ = SSHManager.exec_command(client, "cat /etc/os-release")
        os_id = ""
        version_id = ""
        pretty_name = ""
        for line in stdout.splitlines():
            if line.startswith("ID="):
                os_id = line.split("=", 1)[1].strip('"')
            elif line.startswith("VERSION_ID="):
                version_id = line.split("=", 1)[1].strip('"')
            elif line.startswith("PRETTY_NAME="):
                pretty_name = line.split("=", 1)[1].strip('"')

        supported = {
            "ubuntu": 20, "debian": 11, "rhel": 8, "centos": 8,
            "rocky": 8, "almalinux": 8, "fedora": 37, "amzn": 2,
        }

        try:
            major = int(version_id.split(".")[0])
        except (ValueError, IndexError):
            return {"name": "Operating System", "status": "warn", "message": f"Could not parse version: {pretty_name}", "details": stdout}

        min_version = supported.get(os_id)
        if min_version is None:
            return {"name": "Operating System", "status": "warn", "message": f"Untested OS: {pretty_name}", "details": stdout}

        if major >= min_version:
            return {"name": "Operating System", "status": "pass", "message": pretty_name, "details": None}
        return {"name": "Operating System", "status": "fail", "message": f"{pretty_name} — minimum {os_id} {min_version}+", "details": stdout}

    @staticmethod
    def check_java(client: paramiko.SSHClient) -> dict:
        exit_code, stdout, stderr = SSHManager.exec_command(client, "java -version 2>&1")
        output = stdout or stderr
        if exit_code != 0:
            return {"name": "Java", "status": "fail", "message": "Java not installed", "details": output}

        match = re.search(r'"(\d+)[\._]', output)
        if match:
            version = int(match.group(1))
            if version in (11, 17, 21):
                return {"name": "Java", "status": "pass", "message": f"Java {version} detected", "details": output}
            return {"name": "Java", "status": "warn", "message": f"Java {version} — recommend 11, 17, or 21", "details": output}
        return {"name": "Java", "status": "warn", "message": "Could not determine Java version", "details": output}

    @staticmethod
    def check_ram(client: paramiko.SSHClient) -> dict:
        # Cloud VMs marketed as 4 GB typically report ~3.6 GB usable after kernel
        # and cloud-init overhead, so the fail threshold sits below that floor.
        _, stdout, _ = SSHManager.exec_command(client, "free -m | awk '/Mem:/{print $2}'")
        try:
            ram_mb = int(stdout)
            ram_gb = ram_mb / 1024
            if ram_gb >= 8:
                return {"name": "RAM", "status": "pass", "message": f"{ram_gb:.1f} GB available", "details": None}
            elif ram_gb >= 3.5:
                return {"name": "RAM", "status": "warn", "message": f"{ram_gb:.1f} GB — 8GB+ recommended for production", "details": None}
            return {"name": "RAM", "status": "fail", "message": f"{ram_gb:.1f} GB — minimum 3.5GB required", "details": None}
        except ValueError:
            return {"name": "RAM", "status": "fail", "message": "Could not determine RAM", "details": stdout}

    @staticmethod
    def check_disk(client: paramiko.SSHClient) -> dict:
        # 20 GB cloud root volumes typically show ~18-19 GB free after OS install,
        # so the fail threshold sits below that floor.
        _, stdout, _ = SSHManager.exec_command(client, "df -BG /opt | tail -1 | awk '{print $4}' | tr -d 'G'")
        try:
            free_gb = int(stdout)
            if free_gb >= 50:
                return {"name": "Disk Space", "status": "pass", "message": f"{free_gb} GB free on /opt", "details": None}
            elif free_gb >= 15:
                return {"name": "Disk Space", "status": "warn", "message": f"{free_gb} GB free — 50GB+ recommended for production", "details": None}
            return {"name": "Disk Space", "status": "fail", "message": f"{free_gb} GB free — minimum 15GB", "details": None}
        except ValueError:
            return {"name": "Disk Space", "status": "fail", "message": "Could not determine disk space", "details": stdout}

    @staticmethod
    def check_cpu(client: paramiko.SSHClient) -> dict:
        _, stdout, _ = SSHManager.exec_command(client, "nproc")
        try:
            cores = int(stdout)
            if cores >= 4:
                return {"name": "CPU Cores", "status": "pass", "message": f"{cores} cores", "details": None}
            elif cores >= 2:
                return {"name": "CPU Cores", "status": "warn", "message": f"{cores} cores — 4+ recommended", "details": None}
            return {"name": "CPU Cores", "status": "fail", "message": f"{cores} core(s) — minimum 2", "details": None}
        except ValueError:
            return {"name": "CPU Cores", "status": "fail", "message": "Could not determine CPU count", "details": stdout}

    @staticmethod
    def check_ports(client: paramiko.SSHClient) -> dict:
        _, stdout, _ = SSHManager.exec_command(client, "ss -tlnp 2>/dev/null | grep -E ':(9092|9093|2181|8088|8083)' || true")
        if stdout.strip():
            ports_in_use = []
            for line in stdout.strip().splitlines():
                match = re.search(r':(\d+)\s', line)
                if match:
                    ports_in_use.append(match.group(1))
            return {"name": "Port Availability", "status": "warn", "message": f"Ports in use: {', '.join(ports_in_use)}", "details": stdout}
        return {"name": "Port Availability", "status": "pass", "message": "All Kafka ports available (9092, 9093, 2181, 8088, 8083)", "details": None}

    @staticmethod
    def check_time_sync(client: paramiko.SSHClient) -> dict:
        _, stdout, _ = SSHManager.exec_command(client, "timedatectl 2>/dev/null | grep -i 'synchronized\\|NTP' || echo 'unavailable'")
        if "yes" in stdout.lower():
            return {"name": "Time Sync", "status": "pass", "message": "NTP synchronized", "details": stdout}
        if "unavailable" in stdout:
            return {"name": "Time Sync", "status": "warn", "message": "timedatectl not available", "details": None}
        return {"name": "Time Sync", "status": "warn", "message": "NTP may not be synchronized", "details": stdout}

    @staticmethod
    def check_sudo(client: paramiko.SSHClient) -> dict:
        exit_code, _, stderr = SSHManager.exec_command(client, "sudo -n true 2>&1")
        if exit_code == 0:
            return {"name": "Sudo Access", "status": "pass", "message": "Passwordless sudo available", "details": None}
        return {"name": "Sudo Access", "status": "warn", "message": "No passwordless sudo — deployment may prompt for password", "details": stderr}

    @staticmethod
    def check_firewall(client: paramiko.SSHClient) -> dict:
        _, stdout, _ = SSHManager.exec_command(client, "systemctl is-active firewalld 2>/dev/null; systemctl is-active ufw 2>/dev/null")
        lines = [l.strip() for l in stdout.splitlines()]
        active_firewalls = []
        services = ["firewalld", "ufw"]
        for i, line in enumerate(lines):
            if i < len(services) and line == "active":
                active_firewalls.append(services[i])

        if active_firewalls:
            return {"name": "Firewall", "status": "warn", "message": f"Active firewall(s): {', '.join(active_firewalls)} — ensure Kafka ports are open", "details": stdout}
        return {"name": "Firewall", "status": "pass", "message": "No active firewall detected", "details": None}

    @classmethod
    def run_all(cls, client: paramiko.SSHClient) -> list[dict]:
        checks = [
            cls.check_os,
            cls.check_java,
            cls.check_ram,
            cls.check_disk,
            cls.check_cpu,
            cls.check_ports,
            cls.check_time_sync,
            cls.check_sudo,
            cls.check_firewall,
        ]
        results = []
        for check_fn in checks:
            try:
                results.append(check_fn(client))
            except Exception as e:
                results.append({
                    "name": check_fn.__name__.replace("check_", "").replace("_", " ").title(),
                    "status": "fail",
                    "message": f"Check failed: {e}",
                    "details": None,
                })
        return results


prereq_checker = PrereqChecker()
