import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), keep_trailing_newline=True)


class ConfigGenerator:
    """Generates Kafka configuration files from Jinja2 templates."""

    @staticmethod
    def _build_quorum_voters(services: list[dict]) -> str:
        """Build controller.quorum.voters string from controller/broker_controller services."""
        voters = []
        for svc in services:
            if svc["role"] in ("controller", "broker_controller"):
                voters.append(f"{svc['node_id']}@{svc['ip_address']}:{svc.get('controller_port', 9093)}")
        return ",".join(voters)

    @staticmethod
    def generate_kraft_broker_config(service: dict, all_services: list[dict], cluster_config: dict) -> str:
        """Generate server.properties for a KRaft broker."""
        template = env.get_template("kraft_server.properties.j2")
        quorum_voters = ConfigGenerator._build_quorum_voters(all_services)

        ssl_enabled = bool(cluster_config.get("ssl_enabled"))
        mtls_required = bool(cluster_config.get("mtls_required"))
        listener_port = cluster_config.get("listener_port", 9092)
        controller_port = cluster_config.get("controller_port", 9093)
        ssl_port = cluster_config.get("ssl_listener_port", 9096)

        if service["role"] == "broker_controller":
            process_roles = "broker,controller"
            listeners_list = [
                f"PLAINTEXT://:{listener_port}",
                f"CONTROLLER://:{controller_port}",
            ]
            adv_list = [f"PLAINTEXT://{service['ip_address']}:{listener_port}"]
        else:
            process_roles = "broker"
            listeners_list = [f"PLAINTEXT://:{listener_port}"]
            adv_list = [f"PLAINTEXT://{service['ip_address']}:{listener_port}"]

        protocol_map = ["CONTROLLER:PLAINTEXT", "PLAINTEXT:PLAINTEXT"]
        if ssl_enabled:
            listeners_list.append(f"SSL://:{ssl_port}")
            adv_list.append(f"SSL://{service['ip_address']}:{ssl_port}")
            protocol_map.append("SSL:SSL")

        return template.render(
            node_id=service["node_id"],
            process_roles=process_roles,
            quorum_voters=quorum_voters,
            listeners=",".join(listeners_list),
            advertised_listeners=",".join(adv_list),
            listener_security_protocol_map=",".join(protocol_map),
            log_dirs=cluster_config.get("log_dirs", "/var/lib/kafka/data"),
            num_partitions=cluster_config.get("num_partitions", 3),
            replication_factor=cluster_config.get("replication_factor", 3),
            ssl_enabled=ssl_enabled,
            mtls_required=mtls_required,
            ssl_keystore_location="/etc/kafka/ssl/broker.p12",
            # PEM truststore — see cert_manager._make_truststore_pem for why.
            ssl_truststore_location="/etc/kafka/ssl/truststore.pem",
            # The placeholder is replaced at deploy time with the cluster's
            # Fernet-decrypted password so the property file on disk has the
            # real value (Kafka can't read env vars from server.properties).
            ssl_keystore_password=cluster_config.get("_ssl_keystore_password", ""),
        )

    @staticmethod
    def generate_kraft_controller_config(service: dict, all_services: list[dict], cluster_config: dict) -> str:
        """Generate server.properties for a dedicated KRaft controller."""
        template = env.get_template("kraft_controller.properties.j2")
        quorum_voters = ConfigGenerator._build_quorum_voters(all_services)

        return template.render(
            node_id=service["node_id"],
            quorum_voters=quorum_voters,
            controller_port=cluster_config.get("controller_port", 9093),
            ip_address=service["ip_address"],
            log_dirs=cluster_config.get("log_dirs", "/var/lib/kafka/data"),
        )

    @staticmethod
    def generate_zookeeper_config(service: dict, all_zk_services: list[dict]) -> str:
        """Generate zookeeper.properties."""
        template = env.get_template("zookeeper.properties.j2")
        servers = []
        for zk in all_zk_services:
            servers.append({"id": zk["node_id"], "ip": zk["ip_address"]})
        return template.render(
            my_id=service["node_id"],
            servers=servers,
            data_dir="/var/lib/zookeeper",
        )

    @staticmethod
    def generate_ksqldb_config(service: dict, broker_services: list[dict], cluster_config: dict) -> str:
        """Generate ksql-server.properties."""
        template = env.get_template("ksqldb.properties.j2")
        bootstrap_servers = ",".join(
            f"{s['ip_address']}:{cluster_config.get('listener_port', 9092)}"
            for s in broker_services
            if s["role"] in ("broker", "broker_controller")
        )
        return template.render(
            bootstrap_servers=bootstrap_servers,
            ksqldb_port=cluster_config.get("ksqldb_port", 8088),
            service_id=f"ksqldb_{service['node_id']}",
        )

    @staticmethod
    def generate_connect_config(service: dict, broker_services: list[dict], cluster_config: dict) -> str:
        """Generate connect-distributed.properties."""
        template = env.get_template("connect_distributed.properties.j2")
        bootstrap_servers = ",".join(
            f"{s['ip_address']}:{cluster_config.get('listener_port', 9092)}"
            for s in broker_services
            if s["role"] in ("broker", "broker_controller")
        )
        return template.render(
            bootstrap_servers=bootstrap_servers,
            connect_port=cluster_config.get("connect_rest_port", 8083),
            group_id="tantor-connect-cluster",
        )

    @staticmethod
    def generate_schema_registry_config(service: dict, broker_services: list[dict], cluster_config: dict) -> str:
        """Generate Apicurio Registry application.properties.

        Storage backend is `kafkasql` — Apicurio persists schemas in a Kafka
        topic on the same cluster, so the registry has no separate database.
        ccompat-v7 is enabled so kafka-avro-serializer / Confluent SerDes
        clients can talk to it like Confluent Schema Registry.
        """
        template = env.get_template("schema_registry.properties.j2")
        bootstrap_servers = ",".join(
            f"{s['ip_address']}:{cluster_config.get('listener_port', 9092)}"
            for s in broker_services
        )
        return template.render(
            bootstrap_servers=bootstrap_servers,
            schema_registry_port=cluster_config.get("schema_registry_port", 8085),
        )

    @staticmethod
    def generate_systemd_unit(
        service_type: str,
        config_path: str,
        kafka_home: str = "/opt/kafka",
        ksqldb_home: str = "/opt/ksqldb",
        heap_opts: str = "",
        java_home: str = "",
    ) -> str:
        """Generate a systemd unit file for a Kafka service.

        java_home is discovered at deploy time by the Ansible playbook.
        We use a placeholder that gets replaced during deployment, or
        a safe fallback that works on most systems.
        """
        if not java_home:
            # Use a safe default that works on both Debian and RHEL
            # The playbook will override this with the actual discovered path
            java_home = "/usr"  # /usr/bin/java exists on all systems with java installed
        template = env.get_template(f"systemd/{service_type}.service.j2")
        return template.render(
            kafka_home=kafka_home,
            ksqldb_home=ksqldb_home,
            config_path=config_path,
            java_home=java_home,
            heap_opts=heap_opts,
        )

    @staticmethod
    def generate_config_for_service(service: dict, all_services: list[dict], cluster_config: dict) -> str:
        """Route to the correct config generator based on service role."""
        role = service["role"]
        broker_services = [s for s in all_services if s["role"] in ("broker", "broker_controller")]

        if role == "broker" or role == "broker_controller":
            return ConfigGenerator.generate_kraft_broker_config(service, all_services, cluster_config)
        elif role == "controller":
            return ConfigGenerator.generate_kraft_controller_config(service, all_services, cluster_config)
        elif role == "zookeeper":
            zk_services = [s for s in all_services if s["role"] == "zookeeper"]
            return ConfigGenerator.generate_zookeeper_config(service, zk_services)
        elif role == "ksqldb":
            return ConfigGenerator.generate_ksqldb_config(service, broker_services, cluster_config)
        elif role == "kafka_connect":
            return ConfigGenerator.generate_connect_config(service, broker_services, cluster_config)
        elif role == "schema_registry":
            return ConfigGenerator.generate_schema_registry_config(service, broker_services, cluster_config)
        else:
            raise ValueError(f"Unknown service role: {role}")


config_generator = ConfigGenerator()
