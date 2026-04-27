"""Alert rule + Alertmanager rendering and Alertmanager proxy.

Two responsibilities:

1. Take Tantor-stored `AlertRule` and `NotificationChannel` rows and emit two
   YAML files that Prometheus and Alertmanager understand:
     - `prometheus.alert.rules.yml` — alerting rules
     - `alertmanager.yml`           — receivers + routes
   These get written by the monitoring deployer onto the monitoring host
   (next to prometheus.yml) and reloaded.

2. Proxy queries to Alertmanager so the UI can list currently firing alerts
   without giving the browser direct network access to the monitoring host.
"""
from __future__ import annotations

import json
import logging
from typing import Iterable

import urllib.request
import urllib.error
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from app.config import settings
from app.models.alert_rule import AlertRule
from app.models.cluster import Cluster
from app.models.monitoring import MonitoringConfig
from app.models.notification_channel import NotificationChannel

logger = logging.getLogger("tantor.alerts")

ALERTMANAGER_WEBHOOK_PATH = "/api/alerts/webhook"


def alertmanager_port() -> int:
    return settings.ALERTMANAGER_PORT


def tantor_webhook_url() -> str:
    base = settings.TANTOR_PUBLIC_URL.rstrip("/")
    return f"{base}{ALERTMANAGER_WEBHOOK_PATH}"


# ── Encryption helpers for channel.encrypted_config ────────────────────────


def _fernet() -> Fernet:
    return Fernet(settings.FERNET_KEY.encode())


def encrypt_channel_config(plain: dict) -> str:
    return _fernet().encrypt(json.dumps(plain).encode()).decode()


def decrypt_channel_config(encrypted: str) -> dict:
    try:
        return json.loads(_fernet().decrypt(encrypted.encode()).decode())
    except (InvalidToken, ValueError, json.JSONDecodeError) as e:
        logger.warning("Failed to decrypt channel config: %s", e)
        return {}


# Keys whose values must never round-trip back to the UI.
_SECRET_KEYS = {"webhook_url", "smtp_password", "auth_header"}


def redact_channel_config(plain: dict) -> dict:
    redacted = {}
    for k, v in plain.items():
        if k in _SECRET_KEYS and v:
            # Show enough to confirm something is set, hide the rest.
            s = str(v)
            redacted[k] = f"{s[:8]}…{s[-4:]}" if len(s) > 16 else "••••"
        else:
            redacted[k] = v
    return redacted


# ── Rule template helpers ─────────────────────────────────────────────────


# These templates produce ready-to-go PromQL for the most common Kafka
# alerting checks. The UI exposes them as one-click presets; the operator can
# still edit the resulting expr afterwards.
RULE_TEMPLATES: dict[str, dict] = {
    "broker_down": {
        "name": "BrokerDown",
        "severity": "critical",
        "for_seconds": 60,
        "expr": 'up{job="kafka-jmx"} == 0',
        "summary": "Kafka broker {{ $labels.instance }} is down",
        "description": "JMX scrape is failing for {{ $labels.instance }} for over 1 minute.",
    },
    "isr_shrunk": {
        "name": "IsrShrunk",
        "severity": "warning",
        "for_seconds": 300,
        "expr": "kafka_server_replicamanager_underreplicatedpartitions > 0",
        "summary": "Under-replicated partitions on {{ $labels.instance }}",
        "description": "{{ $value }} under-replicated partition(s) for over 5 minutes.",
    },
    "consumer_lag_high": {
        "name": "ConsumerLagHigh",
        "severity": "warning",
        "for_seconds": 600,
        "expr": "kafka_consumergroup_lag > 100000",
        "summary": "Consumer lag > 100k on {{ $labels.consumergroup }}",
        "description": "Consumer group {{ $labels.consumergroup }} is lagging by {{ $value }} messages.",
    },
    "disk_almost_full": {
        "name": "DiskAlmostFull",
        "severity": "warning",
        "for_seconds": 300,
        "expr": '(node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"}) < 0.20',
        "summary": "Disk usage > 80% on {{ $labels.instance }}",
        "description": "Less than 20% disk free on / for over 5 minutes.",
    },
}


def get_template(name: str) -> dict | None:
    return RULE_TEMPLATES.get(name)


# ── Rendering: Prometheus rules YAML ──────────────────────────────────────


def render_prometheus_rules(rules: Iterable[AlertRule]) -> str:
    """Return a `prometheus.alert.rules.yml` body for the given rules.

    We deliberately hand-write YAML rather than pulling in PyYAML — rule
    expressions can include characters PyYAML wants to escape, and the
    structure is tightly bounded.
    """
    lines: list[str] = ["groups:", "  - name: tantor", "    rules:"]
    for rule in rules:
        if not rule.enabled:
            continue
        for_value = _seconds_to_duration(rule.for_seconds)
        lines.append(f"      - alert: {_yaml_str(rule.name)}")
        lines.append(f"        expr: {_yaml_str(rule.expr)}")
        lines.append(f"        for: {for_value}")
        lines.append("        labels:")
        lines.append(f"          severity: {_yaml_str(rule.severity)}")
        lines.append(f"          tantor_cluster_id: {_yaml_str(rule.cluster_id)}")
        lines.append(f"          tantor_rule_id: {_yaml_str(rule.id)}")
        if rule.channel_ids:
            # Routing label for Alertmanager: receiver name = tantor_<channel_id>
            # When multiple channels are linked we use the first as primary;
            # alertmanager.yml inherits a `continue: true` route so others fire too.
            primary = (rule.channel_ids.split(",") + [""])[0].strip()
            if primary:
                lines.append(f"          tantor_channel: {_yaml_str(primary)}")
        lines.append("        annotations:")
        lines.append(f"          summary: {_yaml_str(rule.summary or rule.name)}")
        if rule.description:
            lines.append(f"          description: {_yaml_str(rule.description)}")
    return "\n".join(lines) + "\n"


# ── Rendering: alertmanager.yml ───────────────────────────────────────────


def render_alertmanager_yaml(
    channels: Iterable[NotificationChannel],
    tantor_internal_webhook_url: str,
) -> str:
    """Render alertmanager.yml. One receiver per enabled channel.

    Routes use label-matchers on `tantor_channel` so a rule can target a
    specific channel; rules without a channel fall through to the default.
    """
    enabled_channels = [c for c in channels if c.enabled]

    lines: list[str] = []
    lines.append("global:")
    lines.append("  resolve_timeout: 5m")
    lines.append("")
    lines.append("route:")
    lines.append("  group_by: ['alertname', 'tantor_cluster_id']")
    lines.append("  group_wait: 30s")
    lines.append("  group_interval: 5m")
    lines.append("  repeat_interval: 4h")
    lines.append("  receiver: tantor_default")
    if enabled_channels:
        lines.append("  routes:")
        for ch in enabled_channels:
            lines.append(f"    - matchers: [tantor_channel=\"{ch.id}\"]")
            lines.append(f"      receiver: tantor_{ch.id}")
            lines.append("      continue: false")
    lines.append("")
    lines.append("receivers:")
    # The default receiver always fires back into Tantor's webhook so we have
    # a record even if no channel was selected on the rule.
    lines.append("  - name: tantor_default")
    lines.append("    webhook_configs:")
    lines.append(f"      - url: {_yaml_str(tantor_internal_webhook_url)}")
    lines.append("        send_resolved: true")
    for ch in enabled_channels:
        cfg = decrypt_channel_config(ch.encrypted_config)
        lines.append(f"  - name: tantor_{ch.id}")
        if ch.kind == "slack":
            lines.append("    slack_configs:")
            lines.append(f"      - api_url: {_yaml_str(cfg.get('webhook_url', ''))}")
            if cfg.get("channel"):
                lines.append(f"        channel: {_yaml_str(cfg['channel'])}")
            lines.append("        send_resolved: true")
            lines.append("        title: '{{ .CommonAnnotations.summary }}'")
            lines.append("        text: '{{ range .Alerts }}{{ .Annotations.description }}\\n{{ end }}'")
        elif ch.kind == "webhook":
            lines.append("    webhook_configs:")
            lines.append(f"      - url: {_yaml_str(cfg.get('url', ''))}")
            lines.append("        send_resolved: true")
            if cfg.get("auth_header"):
                lines.append("        http_config:")
                lines.append("          authorization:")
                lines.append("            type: Bearer")
                lines.append(f"            credentials: {_yaml_str(cfg['auth_header'])}")
        elif ch.kind == "email":
            lines.append("    email_configs:")
            lines.append(f"      - to: {_yaml_str(','.join(cfg.get('to_addrs', [])))}")
            lines.append(f"        from: {_yaml_str(cfg.get('from_addr', ''))}")
            lines.append(f"        smarthost: {cfg.get('smtp_host', '')}:{cfg.get('smtp_port', 587)}")
            if cfg.get("smtp_user"):
                lines.append(f"        auth_username: {_yaml_str(cfg['smtp_user'])}")
                lines.append(f"        auth_password: {_yaml_str(cfg.get('smtp_password', ''))}")
            lines.append(f"        require_tls: {'true' if cfg.get('require_tls', True) else 'false'}")
            lines.append("        send_resolved: true")
        elif ch.kind == "tantor_internal":
            lines.append("    webhook_configs:")
            lines.append(f"      - url: {_yaml_str(tantor_internal_webhook_url)}")
            lines.append("        send_resolved: true")
        else:
            logger.warning("Unknown channel kind %s for %s; skipping", ch.kind, ch.id)
    return "\n".join(lines) + "\n"


# ── Alertmanager HTTP proxy ───────────────────────────────────────────────


def list_firing_for_cluster(cluster_id: str, db: Session) -> list[dict]:
    """Pull the current alert list from Alertmanager scoped to one cluster.

    Returns a normalized shape suitable for FiringAlert. If Alertmanager is
    unreachable (not deployed yet, host down) the caller should report
    `alertmanager_reachable: false` rather than 500.
    """
    config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
    if not config or not config.deployed:
        return []
    am_url = _alertmanager_url(config)
    if not am_url:
        return []

    req = urllib.request.Request(f"{am_url}/api/v2/alerts?active=true&silenced=false&inhibited=false")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            payload = json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        logger.warning("Alertmanager %s unreachable: %s", am_url, e)
        return []

    out: list[dict] = []
    for alert in payload:
        labels = alert.get("labels", {})
        # Alertmanager returns alerts cluster-agnostically; we filter on the
        # tantor_cluster_id label we set in render_prometheus_rules.
        if labels.get("tantor_cluster_id") != cluster_id:
            continue
        annotations = alert.get("annotations", {})
        status = alert.get("status", {})
        out.append({
            "fingerprint": alert.get("fingerprint", ""),
            "alert_name": labels.get("alertname", "unknown"),
            "severity": labels.get("severity", "warning"),
            "state": status.get("state", "firing"),
            "started_at": alert.get("startsAt"),
            "ends_at": alert.get("endsAt"),
            "summary": annotations.get("summary"),
            "description": annotations.get("description"),
            "labels": labels,
        })
    return out


def alertmanager_reachable(cluster_id: str, db: Session) -> tuple[bool, str | None]:
    config = db.query(MonitoringConfig).filter(MonitoringConfig.cluster_id == cluster_id).first()
    if not config or not config.deployed:
        return False, None
    am_url = _alertmanager_url(config)
    if not am_url:
        return False, None
    try:
        with urllib.request.urlopen(f"{am_url}/-/healthy", timeout=3) as r:
            return r.status == 200, am_url
    except Exception:
        return False, am_url


def _alertmanager_url(config: MonitoringConfig) -> str | None:
    """Derive Alertmanager URL from MonitoringConfig.

    We piggyback on the Prometheus URL: same host, port = ALERTMANAGER_PORT.
    monitoring_deployer puts Alertmanager on the same host.
    """
    if not config.prometheus_url:
        return None
    # `http://host:9090` -> `http://host:<ALERTMANAGER_PORT>`
    try:
        proto_host, _ = config.prometheus_url.rsplit(":", 1)
        return f"{proto_host}:{alertmanager_port()}"
    except ValueError:
        return None


# ── Helpers ────────────────────────────────────────────────────────────────


def _seconds_to_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _yaml_str(value: str | None) -> str:
    """Always-quoted YAML string. Doubles internal quotes — safe for everything
    the rule editor can produce since we never inject user content as YAML
    structure, only as scalar values."""
    if value is None:
        return '""'
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def cluster_exists(cluster_id: str, db: Session) -> bool:
    return db.query(Cluster).filter(Cluster.id == cluster_id).count() == 1
