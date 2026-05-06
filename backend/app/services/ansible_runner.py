import json
import os
import shutil
import subprocess
from pathlib import Path
from collections.abc import Callable

from jinja2 import Environment, FileSystemLoader

from app.config import settings
from app.services.crypto import decrypt

ANSIBLE_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "ansible"
KAFKA_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "kafka"
ansible_env = Environment(loader=FileSystemLoader(str(ANSIBLE_TEMPLATE_DIR)), keep_trailing_newline=True)
kafka_env = Environment(loader=FileSystemLoader(str(KAFKA_TEMPLATE_DIR)), keep_trailing_newline=True)


class AnsibleRunner:
    """Generates Ansible artifacts and runs playbooks with real-time streaming."""

    @staticmethod
    def prepare_workspace(task_id: str) -> Path:
        """Create a clean workspace directory for this deployment task."""
        work_dir = Path(settings.ANSIBLE_WORKING_DIR) / task_id
        work_dir.mkdir(parents=True, exist_ok=True)
        return work_dir

    @staticmethod
    def generate_inventory(work_dir: Path, services: list[dict], tls_keystores: dict | None = None) -> Path:
        """
        Generate inventory.yml grouping hosts by role.

        Each service dict must have:
          ip_address, port, username, auth_type, credential (decrypted),
          role, node_id

        When `tls_keystores` is provided (keyed by `<ip>_<node_id>`), each
        broker host gets `broker_keystore_src` / `broker_truststore_src`
        ansible vars pointing at the keystore PKCS12 paths on the Tantor
        controller — the playbook uses them in copy: src.
        """
        template = ansible_env.get_template("inventory.yml.j2")

        # Group services by role
        groups: dict[str, list[dict]] = {}
        for svc in services:
            group_name = svc["role"] + "s"
            groups.setdefault(group_name, []).append(svc)

        # For key-based auth, write key files
        keys_dir = work_dir / "keys"
        keys_dir.mkdir(exist_ok=True)
        for svc in services:
            if svc["auth_type"] == "key":
                key_path = keys_dir / f"id_{svc['ip_address']}_{svc['node_id']}"
                key_path.write_text(svc["credential"])
                key_path.chmod(0o600)
                svc["key_file"] = str(key_path)
            if tls_keystores:
                ks = tls_keystores.get(f"{svc['ip_address']}_{svc['node_id']}")
                if ks:
                    svc["broker_keystore_src"] = ks["keystore"]
                    svc["broker_truststore_src"] = ks["truststore"]

        content = template.render(groups=groups)
        inv_path = work_dir / "inventory.yml"
        inv_path.write_text(content)
        return inv_path

    @staticmethod
    def generate_ansible_cfg(work_dir: Path) -> Path:
        """Write ansible.cfg with sane defaults for automated deployment."""
        cfg = work_dir / "ansible.cfg"
        cfg.write_text(
            "[defaults]\n"
            "host_key_checking = False\n"
            "timeout = 60\n"
            "stdout_callback = default\n"
            "deprecation_warnings = False\n"
            "interpreter_python = auto_silent\n"
            "\n[ssh_connection]\n"
            "ssh_args = -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\n"
            "pipelining = True\n"
        )
        return cfg

    @staticmethod
    def write_config_files(work_dir: Path, configs: dict[str, str]) -> Path:
        """
        Write pre-rendered config files to the workspace.
        configs: {filename: content} where filename is like "192.168.1.1_broker.properties"
        Returns the configs directory path.
        """
        configs_dir = work_dir / "configs"
        configs_dir.mkdir(exist_ok=True)
        for filename, content in configs.items():
            (configs_dir / filename).write_text(content)
        return configs_dir

    @staticmethod
    def write_systemd_units(work_dir: Path, units: dict[str, str]) -> Path:
        """Write pre-rendered systemd units to workspace."""
        units_dir = work_dir / "systemd"
        units_dir.mkdir(exist_ok=True)
        for filename, content in units.items():
            (units_dir / filename).write_text(content)
        return units_dir

    @staticmethod
    def write_kafka_log4j2(work_dir: Path, kafka_log_dir: str) -> Path:
        """Render Tantor's log4j2.yaml into the workspace.

        Kafka 4.x ships its own log4j2.yaml in the tarball, but customers
        sometimes hit ownership / permission issues on it after extract.
        Owning the copy ourselves lets us guarantee broker logs land in
        {kafka_log_dir}/server.log with predictable rotation.
        """
        out = work_dir / "kafka" / "log4j2.yaml"
        out.parent.mkdir(exist_ok=True, parents=True)
        template = kafka_env.get_template("log4j2.yaml.j2")
        out.write_text(template.render(kafka_log_dir=kafka_log_dir))
        return out

    @staticmethod
    def generate_playbook(work_dir: Path, template_name: str, context: dict) -> Path:
        """Render a playbook template and write it to the workspace."""
        template = ansible_env.get_template(template_name)
        content = template.render(**context)
        pb_name = template_name.replace(".j2", "")
        pb_path = work_dir / pb_name
        pb_path.write_text(content)
        return pb_path

    @staticmethod
    def run_playbook(
        work_dir: Path,
        playbook_path: Path,
        inventory_path: Path,
        log_callback: Callable[[str], None],
        extra_vars: dict | None = None,
    ) -> int:
        """
        Run ansible-playbook as subprocess, streaming output line by line.
        Returns exit code (0 = success).
        """
        cmd = [
            "ansible-playbook",
            str(playbook_path),
            "-i", str(inventory_path),
            "--force-handlers",
            "-v",
        ]
        if extra_vars:
            cmd.extend(["--extra-vars", json.dumps(extra_vars)])

        env = os.environ.copy()
        env["ANSIBLE_CONFIG"] = str(work_dir / "ansible.cfg")
        env["ANSIBLE_FORCE_COLOR"] = "0"
        env["ANSIBLE_NOCOLOR"] = "1"

        log_callback(f"Running: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(work_dir),
            env=env,
            text=True,
            bufsize=1,
        )

        for line in iter(process.stdout.readline, ""):
            stripped = line.rstrip()
            if stripped:
                log_callback(stripped)

        process.wait()
        return process.returncode

    @staticmethod
    def cleanup_workspace(work_dir: Path):
        """Remove workspace after deployment."""
        shutil.rmtree(work_dir, ignore_errors=True)


ansible_runner = AnsibleRunner()
