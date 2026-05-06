"""Deploy Prometheus, Grafana, Alertmanager, and JMX exporter for Kafka monitoring."""

import json
import logging
import time
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.models.alert_rule import AlertRule
from app.models.cluster import Cluster
from app.models.host import Host
from app.models.notification_channel import NotificationChannel
from app.models.service import Service
from app.models.monitoring import MonitoringConfig
from app.services import alert_manager
from app.services.ssh_manager import SSHManager
from app.services.crypto import decrypt

logger = logging.getLogger("tantor.monitoring_deployer")

PROMETHEUS_VERSION = "2.51.0"
GRAFANA_VERSION = getattr(settings, "GRAFANA_VERSION", "10.4.1")
ALERTMANAGER_VERSION = getattr(settings, "ALERTMANAGER_VERSION", "0.27.0")
JMX_EXPORTER_VERSION = "0.20.0"
JMX_EXPORTER_PORT = 7071


class MonitoringDeployer:

    @staticmethod
    def deploy_monitoring_stack(cluster_id: str, monitoring_host_id: str,
                                grafana_port: int, prometheus_port: int, db: Session,
                                external_jmx_endpoints: list[str] | None = None) -> dict:
        """Deploy Prometheus + Grafana on a host, JMX exporter on all brokers.

        For managed clusters: Tantor SSHes to each broker host and installs JMX
        exporter, then points Prometheus at the broker IPs.

        For external clusters (cluster.kind == "external") Tantor doesn't own
        the brokers, so the JMX-exporter-per-broker step is skipped. Instead
        the caller passes `external_jmx_endpoints` (list of "host:port" the
        customer's brokers expose) and Prometheus scrapes those directly.
        """
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        mon_host = db.query(Host).filter(Host.id == monitoring_host_id).first()
        if not mon_host:
            raise ValueError("Monitoring host not found")

        is_external = (cluster.kind or "managed") == "external"
        services = db.query(Service).filter(Service.cluster_id == cluster_id).all()
        broker_hosts = []
        for svc in services:
            if "broker" in svc.role:
                host = db.query(Host).filter(Host.id == svc.host_id).first()
                if host:
                    broker_hosts.append(host)

        steps = []

        # Step 1: Deploy JMX exporter on each broker (managed clusters only)
        if not is_external:
            from app.services import cluster_paths
            kafka_unit = cluster_paths.unit_name(cluster)
            # Per-cluster JMX exporter port. Derived from listener_port so two
            # clusters on the same host don't both try to bind 7071. Default
            # cluster (9092) → 7071; second (9192) → 7171; etc.
            import json as _json
            try:
                cfg = _json.loads(cluster.config_json or "{}")
            except Exception:
                cfg = {}
            listener = int(cfg.get("listener_port", 9092))
            jmx_port = JMX_EXPORTER_PORT + (listener - 9092)
            for host in broker_hosts:
                try:
                    MonitoringDeployer._deploy_jmx_exporter(host, kafka_unit=kafka_unit, jmx_port=jmx_port)
                    steps.append({"step": f"JMX exporter on {host.hostname} (port {jmx_port})", "status": "success"})
                except Exception as e:
                    steps.append({"step": f"JMX exporter on {host.hostname}", "status": "failed", "error": str(e)})
        else:
            steps.append({
                "step": "JMX exporter (external — must be exposed by customer)",
                "status": "skipped",
            })

        # Step 2: Deploy Prometheus — for external clusters use the operator-supplied
        # JMX endpoints as scrape targets instead of broker hosts we own.
        try:
            if is_external:
                MonitoringDeployer._deploy_prometheus_external(
                    mon_host, external_jmx_endpoints or [], prometheus_port,
                )
            else:
                MonitoringDeployer._deploy_prometheus(mon_host, broker_hosts, prometheus_port, jmx_port=jmx_port)
            steps.append({"step": "Prometheus", "status": "success"})
        except Exception as e:
            steps.append({"step": "Prometheus", "status": "failed", "error": str(e)})

        # Step 3: Deploy Grafana
        try:
            MonitoringDeployer._deploy_grafana(mon_host, grafana_port, prometheus_port)
            steps.append({"step": "Grafana", "status": "success"})
        except Exception as e:
            steps.append({"step": "Grafana", "status": "failed", "error": str(e)})

        # Step 4: Deploy Alertmanager (alongside Prometheus on the same host)
        try:
            MonitoringDeployer._deploy_alertmanager(mon_host, settings.ALERTMANAGER_PORT)
            steps.append({"step": "Alertmanager", "status": "success"})
        except Exception as e:
            steps.append({"step": "Alertmanager", "status": "failed", "error": str(e)})

        # Step 5: Render initial Tantor rules + alertmanager.yml from DB
        try:
            MonitoringDeployer._render_alerting_files(cluster_id, mon_host, db)
            steps.append({"step": "Alert rules + receivers", "status": "success"})
        except Exception as e:
            steps.append({"step": "Alert rules + receivers", "status": "failed", "error": str(e)})

        # Step 6: Provision dashboards
        try:
            time.sleep(5)  # Wait for Grafana to start
            MonitoringDeployer._provision_dashboards(mon_host, grafana_port, prometheus_port)
            steps.append({"step": "Dashboards", "status": "success"})
        except Exception as e:
            steps.append({"step": "Dashboards", "status": "failed", "error": str(e)})

        # Save config
        config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
        if not config:
            from uuid import uuid4
            config = MonitoringConfig(id=str(uuid4()), cluster_id=cluster_id)
            db.add(config)

        config.monitoring_host_id = monitoring_host_id
        config.prometheus_port = prometheus_port
        config.grafana_port = grafana_port
        config.prometheus_url = f"http://{mon_host.ip_address}:{prometheus_port}"
        config.grafana_url = f"http://{mon_host.ip_address}:{grafana_port}"
        config.deployed = True
        db.commit()

        return {
            "status": "completed",
            "steps": steps,
            "grafana_url": f"http://{mon_host.ip_address}:{grafana_port}",
            "prometheus_url": f"http://{mon_host.ip_address}:{prometheus_port}",
        }

    @staticmethod
    def _ssh_exec(host: Host, command: str, timeout: int = 60) -> tuple:
        with SSHManager.connect(
            host.ip_address, host.ssh_port, host.username,
            host.auth_type, host.encrypted_credential,
        ) as client:
            return SSHManager.exec_command(client, command, timeout=timeout)

    @staticmethod
    def _deploy_jmx_exporter(host: Host, kafka_unit: str = "kafka.service", jmx_port: int = JMX_EXPORTER_PORT):
        """Deploy JMX Prometheus exporter on a Kafka broker."""
        logger.info(f"Deploying JMX exporter on {host.hostname}")

        jmx_config = """---
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  # Per-topic broker metrics. Kafka publishes the same metric NAMES at two
  # MBean granularities — cluster-wide (no `topic=` in the MBean) and per-
  # topic (with `topic=`). This more-specific rule MUST come first so the
  # per-topic samples carry a `topic` label; otherwise everything collapses
  # to the cluster-wide version and the per-topic dashboard panels stay
  # empty.
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+), topic=(.+)><>(\w+)
    name: kafka_server_brokertopicmetrics_$1_$3
    labels:
      topic: "$2"
    type: GAUGE
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+)><>(\w+)
    name: kafka_server_brokertopicmetrics_$1_$2
    type: GAUGE
  - pattern: kafka.server<type=ReplicaManager, name=(.+)><>(\w+)
    name: kafka_server_replicamanager_$1_$2
    type: GAUGE
  - pattern: kafka.controller<type=KafkaController, name=(.+)><>(\w+)
    name: kafka_controller_kafkacontroller_$1_$2
    type: GAUGE
  - pattern: kafka.network<type=RequestMetrics, name=(.+), request=(.+)><>(\w+)
    name: kafka_network_requestmetrics_$1_$2_$3
    type: GAUGE
  - pattern: kafka.log<type=Log, name=Size, topic=(.+), partition=(.+)><>Value
    name: kafka_log_size
    labels:
      topic: $1
      partition: $2
    type: GAUGE
  - pattern: java.lang<type=Memory><HeapMemoryUsage>(\w+)
    name: jvm_memory_heap_$1
    type: GAUGE
  - pattern: java.lang<type=GarbageCollector, name=(.+)><>CollectionCount
    name: jvm_gc_collection_count
    labels:
      gc: $1
    type: COUNTER
"""
        jmx_jar_url = f"https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/{JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-{JMX_EXPORTER_VERSION}.jar"

        commands = f"""
sudo mkdir -p /opt/jmx_exporter
sudo bash -c 'cat > /opt/jmx_exporter/kafka.yml << "JMXEOF"
{jmx_config}
JMXEOF'

# Download JMX exporter if not present
if [ ! -f /opt/jmx_exporter/jmx_prometheus_javaagent.jar ]; then
    sudo curl -sL "{jmx_jar_url}" -o /opt/jmx_exporter/jmx_prometheus_javaagent.jar
fi

# Add JMX exporter to this cluster's Kafka systemd unit (per-cluster, APB v1.2.0 #5).
# JMX port is also per-cluster so two clusters on the same host don't both
# try to bind 7071 — the JVM panics if you do.
if ! grep -q "jmx_prometheus_javaagent" /etc/systemd/system/{kafka_unit} 2>/dev/null; then
    sudo sed -i '/\\[Service\\]/a Environment="KAFKA_OPTS=-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar={jmx_port}:/opt/jmx_exporter/kafka.yml"' /etc/systemd/system/{kafka_unit}
    sudo systemctl daemon-reload
    sudo systemctl restart {kafka_unit}
fi

# APB v1.2.0 #9: Capacity forecast was always empty because node_exporter
# (the source of node_filesystem_*_bytes metrics) was never deployed —
# Prometheus's scrape config referenced :9100 but nothing was listening.
# Install node_exporter alongside JMX so disk metrics flow through.
if [ ! -f /opt/node_exporter/node_exporter ]; then
    cd /tmp
    NEV="{settings.NODE_EXPORTER_VERSION}"
    sudo curl -sL "https://github.com/prometheus/node_exporter/releases/download/v$NEV/node_exporter-$NEV.linux-amd64.tar.gz" -o node_exporter.tar.gz
    sudo tar xzf node_exporter.tar.gz
    sudo mkdir -p /opt/node_exporter
    sudo cp node_exporter-$NEV.linux-amd64/node_exporter /opt/node_exporter/
    sudo rm -rf node_exporter.tar.gz node_exporter-$NEV.linux-amd64
fi
if [ ! -f /etc/systemd/system/node_exporter.service ]; then
    sudo bash -c 'cat > /etc/systemd/system/node_exporter.service << "NEEOF"
[Unit]
Description=Prometheus node_exporter
After=network.target

[Service]
Type=simple
ExecStart=/opt/node_exporter/node_exporter --web.listen-address=:9100
Restart=always

[Install]
WantedBy=multi-user.target
NEEOF'
    sudo systemctl daemon-reload
    sudo systemctl enable --now node_exporter
fi

echo "JMX_DONE"
"""
        exit_code, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=180)
        if "JMX_DONE" not in stdout:
            raise RuntimeError(f"JMX/node exporter deploy failed: {stderr}")

    @staticmethod
    def _deploy_prometheus(host: Host, broker_hosts: list, port: int, jmx_port: int = JMX_EXPORTER_PORT):
        """Deploy Prometheus on the monitoring host."""
        logger.info(f"Deploying Prometheus on {host.hostname}")

        targets = ", ".join([f'"{h.ip_address}:{jmx_port}"' for h in broker_hosts])
        node_targets = ", ".join([f'"{h.ip_address}:9100"' for h in broker_hosts])

        prometheus_yml = f"""global:
  scrape_interval: 15s
  evaluation_interval: 15s

# Tantor-managed: Alertmanager runs on the same host as Prometheus.
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:{settings.ALERTMANAGER_PORT}']

# Tantor-managed: alert rules are written by alert_manager.render_prometheus_rules.
rule_files:
  - /etc/prometheus/rules/*.yml

scrape_configs:
  - job_name: 'kafka-jmx'
    static_configs:
      - targets: [{targets}]

  - job_name: 'node'
    static_configs:
      - targets: [{node_targets}]
"""

        commands = f"""
# Install Prometheus
if ! command -v prometheus &>/dev/null && [ ! -f /opt/prometheus/prometheus ]; then
    cd /tmp
    curl -sL "https://github.com/prometheus/prometheus/releases/download/v{PROMETHEUS_VERSION}/prometheus-{PROMETHEUS_VERSION}.linux-amd64.tar.gz" -o prometheus.tar.gz
    tar xzf prometheus.tar.gz
    sudo mkdir -p /opt/prometheus /var/lib/prometheus
    sudo cp prometheus-{PROMETHEUS_VERSION}.linux-amd64/prometheus /opt/prometheus/
    sudo cp prometheus-{PROMETHEUS_VERSION}.linux-amd64/promtool /opt/prometheus/
    rm -rf prometheus.tar.gz prometheus-{PROMETHEUS_VERSION}.linux-amd64
fi

# Write config
sudo mkdir -p /etc/prometheus /etc/prometheus/rules
sudo bash -c 'cat > /etc/prometheus/prometheus.yml << "PROMEOF"
{prometheus_yml}
PROMEOF'

# Create systemd service
sudo bash -c 'cat > /etc/systemd/system/prometheus.service << "SVCEOF"
[Unit]
Description=Prometheus
After=network.target

[Service]
Type=simple
ExecStart=/opt/prometheus/prometheus --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/var/lib/prometheus --web.listen-address=:{port} --storage.tsdb.retention.time=180d --web.enable-lifecycle
Restart=always

[Install]
WantedBy=multi-user.target
SVCEOF'

sudo systemctl daemon-reload
sudo systemctl enable prometheus
sudo systemctl restart prometheus
sleep 2
curl -sf http://localhost:{port}/-/healthy && echo "PROM_OK" || echo "PROM_FAIL"
"""
        exit_code, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=180)
        if "PROM_OK" not in stdout:
            raise RuntimeError(f"Prometheus deploy failed: {stderr}")

    @staticmethod
    def _deploy_prometheus_external(host: Host, jmx_endpoints: list[str], port: int):
        """Variant of _deploy_prometheus for external clusters.

        Tantor doesn't own the brokers, so it can't push JMX exporter to them.
        The customer must expose JMX (or JMX exporter) themselves; the
        endpoints they give us go straight into the kafka-jmx scrape job.
        """
        logger.info(f"Deploying Prometheus on {host.hostname} for external cluster")

        # Tolerate missing endpoints — the operator can start with just rules
        # and add scrape targets later by re-running the monitoring deploy.
        targets = ", ".join([f'"{ep.strip()}"' for ep in jmx_endpoints if ep.strip()]) or '""'
        prometheus_yml = f"""global:
  scrape_interval: 15s
  evaluation_interval: 15s

# Tantor-managed: Alertmanager runs on the same host as Prometheus.
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:{settings.ALERTMANAGER_PORT}']

rule_files:
  - /etc/prometheus/rules/*.yml

scrape_configs:
  - job_name: 'kafka-jmx'
    static_configs:
      - targets: [{targets}]
"""

        commands = f"""
if ! command -v prometheus &>/dev/null && [ ! -f /opt/prometheus/prometheus ]; then
    cd /tmp
    curl -sL "https://github.com/prometheus/prometheus/releases/download/v{PROMETHEUS_VERSION}/prometheus-{PROMETHEUS_VERSION}.linux-amd64.tar.gz" -o prometheus.tar.gz
    tar xzf prometheus.tar.gz
    sudo mkdir -p /opt/prometheus /var/lib/prometheus
    sudo cp prometheus-{PROMETHEUS_VERSION}.linux-amd64/prometheus /opt/prometheus/
    sudo cp prometheus-{PROMETHEUS_VERSION}.linux-amd64/promtool /opt/prometheus/
    rm -rf prometheus.tar.gz prometheus-{PROMETHEUS_VERSION}.linux-amd64
fi

sudo mkdir -p /etc/prometheus /etc/prometheus/rules
sudo bash -c 'cat > /etc/prometheus/prometheus.yml << "PROMEOF"
{prometheus_yml}
PROMEOF'

sudo bash -c 'cat > /etc/systemd/system/prometheus.service << "SVCEOF"
[Unit]
Description=Prometheus
After=network.target

[Service]
Type=simple
ExecStart=/opt/prometheus/prometheus --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/var/lib/prometheus --web.listen-address=:{port} --storage.tsdb.retention.time=180d --web.enable-lifecycle
Restart=always

[Install]
WantedBy=multi-user.target
SVCEOF'

sudo systemctl daemon-reload
sudo systemctl enable prometheus
sudo systemctl restart prometheus
sleep 2
curl -sf http://localhost:{port}/-/healthy && echo "PROM_OK" || echo "PROM_FAIL"
"""
        exit_code, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=180)
        if "PROM_OK" not in stdout:
            raise RuntimeError(f"Prometheus deploy failed: {stderr}")

    @staticmethod
    def _deploy_grafana(host: Host, grafana_port: int, prometheus_port: int):
        """Deploy Grafana on the monitoring host."""
        logger.info(f"Deploying Grafana on {host.hostname}")

        commands = f"""
# Install Grafana
if ! command -v grafana-server &>/dev/null; then
    # Detect OS
    if command -v apt-get &>/dev/null; then
        sudo apt-get install -y adduser libfontconfig1 musl >/dev/null 2>&1
        curl -sL "https://dl.grafana.com/oss/release/grafana_{GRAFANA_VERSION}_amd64.deb" -o /tmp/grafana.deb
        sudo dpkg -i /tmp/grafana.deb >/dev/null 2>&1
        rm -f /tmp/grafana.deb
    else
        curl -sL "https://dl.grafana.com/oss/release/grafana-{GRAFANA_VERSION}-1.x86_64.rpm" -o /tmp/grafana.rpm
        sudo dnf install -y /tmp/grafana.rpm >/dev/null 2>&1 || sudo yum install -y /tmp/grafana.rpm >/dev/null 2>&1
        rm -f /tmp/grafana.rpm
    fi
fi

# Configure Grafana
sudo bash -c 'cat > /etc/grafana/grafana.ini << "GRAFEOF"
[server]
http_port = {grafana_port}
root_url = %(protocol)s://%(domain)s:{grafana_port}/
serve_from_sub_path = false

[security]
allow_embedding = true
admin_user = admin
admin_password = admin

[auth.anonymous]
enabled = true
org_role = Viewer

[dashboards]
default_home_dashboard_path = /var/lib/grafana/dashboards/kafka-overview.json

[users]
allow_sign_up = false
GRAFEOF'

# SELinux
if command -v setsebool &>/dev/null; then
    sudo setsebool -P httpd_can_network_connect 1 2>/dev/null || true
fi

# Ensure data dirs exist with grafana ownership — `--purge` may have wiped them
# and the deb's postinst doesn't always recreate on a re-deploy.
sudo mkdir -p /var/lib/grafana /var/lib/grafana/plugins /var/lib/grafana/dashboards /var/log/grafana
sudo chown -R grafana:grafana /var/lib/grafana /var/log/grafana 2>/dev/null || true

sudo systemctl daemon-reload
sudo systemctl enable grafana-server
sudo systemctl restart grafana-server
# Grafana boot can take 5-10s, especially on cold installs; poll up to 30s.
GRAFANA_OK=0
for i in 1 2 3 4 5 6; do
    if curl -sf http://localhost:{grafana_port}/api/health >/dev/null 2>&1; then
        GRAFANA_OK=1
        break
    fi
    sleep 5
done
[ "$GRAFANA_OK" = "1" ] && echo "GRAFANA_OK" || echo "GRAFANA_FAIL"
"""
        exit_code, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=180)
        if "GRAFANA_OK" not in stdout:
            raise RuntimeError(f"Grafana deploy failed: {stderr}")

    @staticmethod
    def _provision_dashboards(host: Host, grafana_port: int, prometheus_port: int):
        """Add Prometheus data source and Kafka dashboards to Grafana."""
        logger.info(f"Provisioning Grafana dashboards on {host.hostname}")

        # Add Prometheus data source
        ds_json = json.dumps({
            "name": "Prometheus",
            "type": "prometheus",
            "url": f"http://localhost:{prometheus_port}",
            "access": "proxy",
            "isDefault": True,
        })

        # Kafka overview dashboard
        dashboard_json = json.dumps({
            "dashboard": {
                "title": "Kafka Overview",
                "tags": ["kafka"],
                "timezone": "browser",
                "panels": [
                    {
                        "title": "Messages In/sec",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                        "targets": [{"expr": "rate(kafka_server_brokertopicmetrics_messagesinpersec_count[5m])", "legendFormat": "{{instance}}"}],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Bytes In/sec",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                        "targets": [{"expr": "rate(kafka_server_brokertopicmetrics_bytesinpersec_count[5m])", "legendFormat": "{{instance}}"}],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Under-Replicated Partitions",
                        "type": "stat",
                        "gridPos": {"h": 4, "w": 6, "x": 0, "y": 8},
                        "targets": [{"expr": "kafka_server_replicamanager_underreplicatedpartitions_value", "legendFormat": "{{instance}}"}],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Active Controller",
                        "type": "stat",
                        "gridPos": {"h": 4, "w": 6, "x": 6, "y": 8},
                        "targets": [{"expr": "kafka_controller_kafkacontroller_activecontrollercount_value", "legendFormat": "{{instance}}"}],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "JVM Heap Used",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 12},
                        "targets": [{"expr": "jvm_memory_heap_used / 1024 / 1024", "legendFormat": "{{instance}} MB"}],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "GC Count",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 12},
                        "targets": [{"expr": "rate(jvm_gc_collection_count[5m])", "legendFormat": "{{instance}} {{gc}}"}],
                        "datasource": "Prometheus",
                    },
                ],
                "time": {"from": "now-1h", "to": "now"},
                "refresh": "30s",
            },
            "overwrite": True,
        })

        # APB-requested per-topic dashboard. Uses a Grafana variable `topic`
        # populated from `label_values(...)` on the JMX-exporter rewrite rule
        # that exposes a `topic` label on broker-topic metrics. Operators get
        # throughput, lag and partition count in one place per topic.
        topic_dashboard_json = json.dumps({
            "dashboard": {
                "title": "Kafka — Topic Performance",
                "tags": ["kafka", "topic"],
                "timezone": "browser",
                "templating": {
                    "list": [
                        {
                            "name": "topic",
                            "type": "query",
                            "datasource": "Prometheus",
                            "query": "label_values(kafka_server_brokertopicmetrics_messagesinpersec_count{topic!=\"\"}, topic)",
                            "refresh": 2,
                            "includeAll": False,
                            "multi": False,
                        }
                    ]
                },
                "panels": [
                    {
                        "title": "Messages In/sec — $topic",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                        "targets": [{
                            "expr": "sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic=~\"$topic\"}[5m]))",
                            "legendFormat": "{{topic}}"
                        }],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Bytes In/sec — $topic",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                        "targets": [{
                            "expr": "sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesinpersec_count{topic=~\"$topic\"}[5m]))",
                            "legendFormat": "{{topic}}"
                        }],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Bytes Out/sec — $topic",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                        "targets": [{
                            "expr": "sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesoutpersec_count{topic=~\"$topic\"}[5m]))",
                            "legendFormat": "{{topic}}"
                        }],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Consumer Lag — $topic (per group)",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                        "targets": [{
                            "expr": "sum by (consumergroup, topic) (kafka_consumergroup_lag{topic=~\"$topic\"})",
                            "legendFormat": "{{consumergroup}}"
                        }],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Log Size (bytes) — $topic",
                        "type": "timeseries",
                        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
                        "targets": [{
                            "expr": "sum by (topic) (kafka_log_size{topic=~\"$topic\"})",
                            "legendFormat": "{{topic}}"
                        }],
                        "datasource": "Prometheus",
                    },
                    {
                        "title": "Partition count — $topic",
                        "type": "stat",
                        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16},
                        "targets": [{
                            "expr": "count by (topic) (kafka_log_size{topic=~\"$topic\"})",
                            "legendFormat": "{{topic}}"
                        }],
                        "datasource": "Prometheus",
                    },
                ],
                "time": {"from": "now-1h", "to": "now"},
                "refresh": "30s",
            },
            "overwrite": True,
        })

        commands = f"""
# Add Prometheus data source
curl -sf -X POST http://admin:admin@localhost:{grafana_port}/api/datasources \
  -H "Content-Type: application/json" \
  -d '{ds_json}' 2>/dev/null || true

# Create Kafka Overview dashboard
curl -sf -X POST http://admin:admin@localhost:{grafana_port}/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d '{dashboard_json}' 2>/dev/null

# Create per-topic performance dashboard
curl -sf -X POST http://admin:admin@localhost:{grafana_port}/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d '{topic_dashboard_json}' 2>/dev/null

echo "DASHBOARDS_OK"
"""
        exit_code, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=30)
        if "DASHBOARDS_OK" not in stdout:
            raise RuntimeError(f"Dashboard provisioning failed: {stderr}")

    @staticmethod
    def get_grafana_info(cluster_id: str, db: Session) -> dict:
        """Get Grafana connection info for a cluster."""
        config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
        if not config or not config.deployed:
            return {"deployed": False}

        return {
            "deployed": True,
            "grafana_url": config.grafana_url,
            "prometheus_url": config.prometheus_url,
            "grafana_port": config.grafana_port,
            "prometheus_port": config.prometheus_port,
        }

    # ── Alertmanager ───────────────────────────────────────────────────────

    @staticmethod
    def _deploy_alertmanager(host: Host, port: int) -> None:
        """Install Alertmanager on the monitoring host."""
        logger.info("Deploying Alertmanager %s on %s:%d", ALERTMANAGER_VERSION, host.hostname, port)
        commands = f"""
if [ ! -f /opt/alertmanager/alertmanager ]; then
    cd /tmp
    curl -sL "https://github.com/prometheus/alertmanager/releases/download/v{ALERTMANAGER_VERSION}/alertmanager-{ALERTMANAGER_VERSION}.linux-amd64.tar.gz" -o alertmanager.tar.gz
    tar xzf alertmanager.tar.gz
    sudo mkdir -p /opt/alertmanager /var/lib/alertmanager /etc/alertmanager
    sudo cp alertmanager-{ALERTMANAGER_VERSION}.linux-amd64/alertmanager /opt/alertmanager/
    sudo cp alertmanager-{ALERTMANAGER_VERSION}.linux-amd64/amtool /opt/alertmanager/
    rm -rf alertmanager.tar.gz alertmanager-{ALERTMANAGER_VERSION}.linux-amd64
fi

# Write a placeholder alertmanager.yml so Alertmanager can start before
# Tantor renders the real one. _render_alerting_files runs right after.
if [ ! -f /etc/alertmanager/alertmanager.yml ]; then
    sudo bash -c 'cat > /etc/alertmanager/alertmanager.yml << "AMEOF"
route:
  receiver: tantor_default
receivers:
  - name: tantor_default
AMEOF'
fi

sudo bash -c 'cat > /etc/systemd/system/alertmanager.service << "SVCEOF"
[Unit]
Description=Alertmanager
After=network.target

[Service]
Type=simple
ExecStart=/opt/alertmanager/alertmanager --config.file=/etc/alertmanager/alertmanager.yml --storage.path=/var/lib/alertmanager --web.listen-address=:{port} --cluster.listen-address=
Restart=always

[Install]
WantedBy=multi-user.target
SVCEOF'

sudo systemctl daemon-reload
sudo systemctl enable alertmanager
sudo systemctl restart alertmanager
sleep 2
curl -sf http://localhost:{port}/-/healthy && echo "AM_OK" || echo "AM_FAIL"
"""
        _, stdout, stderr = MonitoringDeployer._ssh_exec(host, commands, timeout=180)
        if "AM_OK" not in stdout:
            raise RuntimeError(f"Alertmanager deploy failed: {stderr}")

    @staticmethod
    def _render_alerting_files(cluster_id: str, host: Host, db: Session) -> None:
        """Render Tantor's rules + alertmanager.yml and push them to the monitoring host.

        Both Prometheus and Alertmanager are reloaded via their HTTP APIs —
        no service restart needed, so concurrent scrapes / alert evals keep flowing.
        """
        rules = db.query(AlertRule).filter(AlertRule.cluster_id == cluster_id).all()
        channels = db.query(NotificationChannel).all()

        rules_yml = alert_manager.render_prometheus_rules(rules)
        am_yml = alert_manager.render_alertmanager_yaml(channels, alert_manager.tantor_webhook_url())

        # Heredoc-safe: keep deterministic markers and avoid embedded EOFs.
        push_cmd = f"""
sudo mkdir -p /etc/prometheus/rules
sudo bash -c 'cat > /etc/prometheus/rules/tantor.yml << "TANTORRULESEOF"
{rules_yml}TANTORRULESEOF'

sudo bash -c 'cat > /etc/alertmanager/alertmanager.yml << "TANTORAMEOF"
{am_yml}TANTORAMEOF'

# Validate before reloading; bail loudly so the operator sees the issue.
sudo /opt/prometheus/promtool check rules /etc/prometheus/rules/tantor.yml >/tmp/rules_check 2>&1 \\
  && echo "RULES_OK" \\
  || (echo "RULES_INVALID"; cat /tmp/rules_check; exit 1)
sudo /opt/alertmanager/amtool check-config /etc/alertmanager/alertmanager.yml >/tmp/am_check 2>&1 \\
  && echo "AM_CONFIG_OK" \\
  || (echo "AM_CONFIG_INVALID"; cat /tmp/am_check; exit 1)

# Reload via HTTP API (no restart, no scrape gap).
curl -sf -X POST http://localhost:{settings.PROMETHEUS_PORT}/-/reload && echo "PROM_RELOADED" || echo "PROM_RELOAD_FAILED"
curl -sf -X POST http://localhost:{settings.ALERTMANAGER_PORT}/-/reload && echo "AM_RELOADED" || echo "AM_RELOAD_FAILED"
"""
        _, stdout, stderr = MonitoringDeployer._ssh_exec(host, push_cmd, timeout=60)
        if "RULES_OK" not in stdout or "AM_CONFIG_OK" not in stdout:
            raise RuntimeError(
                f"Alerting config validation failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        # Reload failures are best-effort: log but don't fail the call. The
        # validated files are on disk; a daemon restart would pick them up.
        if "PROM_RELOADED" not in stdout:
            logger.warning("Prometheus did not acknowledge reload: %s", stdout)
        if "AM_RELOADED" not in stdout:
            logger.warning("Alertmanager did not acknowledge reload: %s", stdout)

    @staticmethod
    def reload_alerting(cluster_id: str, db: Session) -> dict:
        """Public entrypoint used by the alerts API after rule/channel CRUD."""
        config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
        if not config or not config.deployed or not config.monitoring_host_id:
            return {"reloaded": False, "reason": "monitoring stack not deployed"}
        host = db.query(Host).filter(Host.id == config.monitoring_host_id).first()
        if not host:
            return {"reloaded": False, "reason": "monitoring host not found"}
        MonitoringDeployer._render_alerting_files(cluster_id, host, db)
        return {"reloaded": True}
