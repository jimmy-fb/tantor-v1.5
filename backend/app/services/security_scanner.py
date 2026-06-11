"""Security Scanner — Vulnerability Assessment for Kafka Clusters."""
import json
import logging
from sqlalchemy.orm import Session
from app.models.cluster import Cluster
from app.models.service import Service
from app.models.host import Host
from app.services.ssh_manager import SSHManager
from app.config import settings

logger = logging.getLogger("tantor.security_scanner")


class SecurityScanner:
    def scan_cluster(self, cluster_id: str, db: Session) -> dict:
        """Run full security scan on a cluster. Returns categorized findings."""
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise ValueError("Cluster not found")

        is_external = (cluster.kind or "managed") == "external"
        services = [] if is_external else db.query(Service).filter(Service.cluster_id == cluster_id).all()
        hosts = {h.id: h for h in db.query(Host).all()}

        findings = []

        # Get config from first broker
        broker_configs = {}
        if is_external:
            broker_configs = self._get_external_broker_configs(cluster)
            if not broker_configs:
                findings.append(self._external_config_unavailable_finding(cluster))
        else:
            for svc in services:
                if svc.role in ("broker", "broker_controller"):
                    host = hosts.get(svc.host_id)
                    if host:
                        try:
                            with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                                exit_code, stdout, _ = SSHManager.exec_command(
                                    client, f"cat {settings.KAFKA_INSTALL_DIR}/config/server.properties", timeout=15
                                )
                                if exit_code == 0:
                                    broker_configs = self._parse_properties(stdout)
                        except Exception as e:
                            logger.error(f"Failed to read broker config: {e}")
                    break

        # Run all checks
        findings.extend(self._check_authentication(broker_configs))
        findings.extend(self._check_network_security(broker_configs))
        findings.extend(self._check_config_security(broker_configs))
        findings.extend(self._check_data_protection(broker_configs))

        # OS-level checks (run on each host)
        os_targets = self._external_os_targets(cluster, hosts) if is_external else [
            {"host": hosts.get(svc.host_id), "node_id": svc.node_id}
            for svc in services
            if svc.role in ("broker", "broker_controller")
        ]
        if is_external and not os_targets:
            findings.append(self._external_os_unavailable_finding(cluster))
        for target in os_targets:
            host = target.get("host")
            node_id = target.get("node_id", 0)
            if host:
                try:
                    with SSHManager.connect(host.ip_address, host.ssh_port, host.username, host.auth_type, host.encrypted_credential) as client:
                        findings.extend(self._check_os_security(client, host.ip_address, node_id))
                except Exception as e:
                    findings.append({
                        "id": f"os-err-{node_id}",
                        "category": "OS Security",
                        "check": f"OS Security Check (Node {node_id})",
                        "severity": "high",
                        "status": "error",
                        "message": f"Could not check OS security on {host.ip_address}: {e}",
                        "recommendation": "Verify SSH connectivity",
                    })

        # Calculate score
        total = len(findings)
        passed = sum(1 for f in findings if f["status"] == "pass")
        critical_fails = sum(1 for f in findings if f["status"] == "fail" and f["severity"] == "critical")
        high_fails = sum(1 for f in findings if f["status"] == "fail" and f["severity"] == "high")

        score = int((passed / total) * 100) if total > 0 else 0
        grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 40 else "F"

        return {
            "cluster_id": cluster_id,
            "cluster_name": cluster.name,
            "score": score,
            "grade": grade,
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "critical_issues": critical_fails,
            "high_issues": high_fails,
            "findings": findings,
            "summary": {
                "Authentication": self._category_summary(findings, "Authentication"),
                "Network Security": self._category_summary(findings, "Network Security"),
                "Configuration": self._category_summary(findings, "Configuration"),
                "Data Protection": self._category_summary(findings, "Data Protection"),
                "OS Security": self._category_summary(findings, "OS Security"),
            },
        }

    # ── Authentication Checks ────────────────────────────

    def _get_external_broker_configs(self, cluster: Cluster) -> dict:
        """Read external broker configs through Kafka AdminClient."""
        try:
            from app.services import external_admin

            broker_entries = external_admin.describe_broker_configs(cluster)
            if not broker_entries:
                return {}
            first = broker_entries[0]
            return {
                cfg["name"]: cfg["value"]
                for cfg in first.get("configs", [])
                if cfg.get("value") is not None
            }
        except Exception as e:
            logger.error("Failed to read external broker config: %s", e)
            return {}

    def _external_os_targets(self, cluster: Cluster, hosts: dict) -> list[dict]:
        """Return SSH targets registered for an external cluster, if any."""
        if not cluster.external_broker_hosts_json:
            return []
        try:
            entries = json.loads(cluster.external_broker_hosts_json)
        except Exception:
            return []
        targets = []
        for idx, entry in enumerate(entries):
            host = hosts.get(entry.get("host_id"))
            if host:
                targets.append({"host": host, "node_id": idx})
        return targets

    def _external_os_unavailable_finding(self, cluster: Cluster) -> dict:
        return {
            "id": "os-external-unavailable",
            "category": "OS Security",
            "check": "External Broker OS Checks",
            "severity": "medium",
            "status": "warning",
            "message": "OS-level checks were skipped because no SSH broker hosts are registered for this external cluster.",
            "recommendation": "Register broker hosts in the external cluster Lifecycle tab if you want Tantor to run host-level security checks.",
            "details": {
                "cluster_kind": "external",
                "bootstrap_servers": cluster.bootstrap_servers or "",
            },
        }

    def _external_config_unavailable_finding(self, cluster: Cluster) -> dict:
        return {
            "id": "cfg-external-unavailable",
            "category": "Configuration",
            "check": "External Broker Config Read",
            "severity": "high",
            "status": "error",
            "message": "Tantor could not read broker configs from this external cluster through the Kafka Admin API.",
            "recommendation": "Verify the external cluster connection, ACL permissions for DescribeConfigs, and the configured security protocol/credentials.",
            "details": {
                "cluster_kind": "external",
                "bootstrap_servers": cluster.bootstrap_servers or "",
            },
        }

    def _check_authentication(self, configs: dict) -> list[dict]:
        findings = []

        # auth-001: Inter-broker SASL authentication
        inter_broker_protocol = configs.get("security.inter.broker.protocol", "")
        has_sasl = "SASL" in inter_broker_protocol.upper()
        findings.append({
            "id": "auth-001",
            "category": "Authentication",
            "check": "SASL Inter-Broker Authentication",
            "severity": "critical",
            "status": "pass" if has_sasl else "fail",
            "message": f"Inter-broker protocol is set to '{inter_broker_protocol}'" if inter_broker_protocol else "security.inter.broker.protocol is not configured",
            "recommendation": "Set security.inter.broker.protocol to SASL_SSL or SASL_PLAINTEXT to enable inter-broker authentication.",
            "details": {"property": "security.inter.broker.protocol", "value": inter_broker_protocol or "(not set)"},
        })

        # auth-002: ACL authorizer enabled
        authorizer_class = configs.get("authorizer.class.name", "")
        has_authorizer = bool(authorizer_class)
        findings.append({
            "id": "auth-002",
            "category": "Authentication",
            "check": "ACL Authorizer Enabled",
            "severity": "critical",
            "status": "pass" if has_authorizer else "fail",
            "message": f"Authorizer class is set to '{authorizer_class}'" if has_authorizer else "No ACL authorizer configured, all operations are permitted by any client",
            "recommendation": "Set authorizer.class.name to kafka.security.authorizer.AclAuthorizer (or org.apache.kafka.metadata.authorizer.StandardAuthorizer for KRaft) to enable access control lists.",
            "details": {"property": "authorizer.class.name", "value": authorizer_class or "(not set)"},
        })

        # auth-003: SASL mechanism for inter-broker communication
        sasl_mechanism = configs.get("sasl.mechanism.inter.broker.protocol", "")
        has_sasl_mech = bool(sasl_mechanism)
        findings.append({
            "id": "auth-003",
            "category": "Authentication",
            "check": "SASL Mechanism Configured",
            "severity": "high",
            "status": "pass" if has_sasl_mech else "fail",
            "message": f"SASL mechanism is set to '{sasl_mechanism}'" if has_sasl_mech else "No SASL mechanism configured for inter-broker protocol",
            "recommendation": "Set sasl.mechanism.inter.broker.protocol to a strong mechanism such as SCRAM-SHA-512 or GSSAPI.",
            "details": {"property": "sasl.mechanism.inter.broker.protocol", "value": sasl_mechanism or "(not set)"},
        })

        # auth-004: Super users configured
        super_users = configs.get("super.users", "")
        has_super_users = bool(super_users)
        findings.append({
            "id": "auth-004",
            "category": "Authentication",
            "check": "Super Users Defined",
            "severity": "medium",
            "status": "pass" if has_super_users else "warning",
            "message": f"Super users configured: {super_users}" if has_super_users else "No super.users property set; consider defining admin users for ACL management",
            "recommendation": "Define super.users to designate administrative principals that bypass ACL checks for management tasks.",
            "details": {"property": "super.users", "value": super_users or "(not set)"},
        })

        # auth-005: Allow everyone if no ACL found
        allow_everyone = configs.get("allow.everyone.if.no.acl.found", "")
        is_restrictive = allow_everyone.lower() == "false" if allow_everyone else True
        findings.append({
            "id": "auth-005",
            "category": "Authentication",
            "check": "Default Deny When No ACLs Found",
            "severity": "high",
            "status": "pass" if is_restrictive else "fail",
            "message": "Default deny policy is active when no ACLs match" if is_restrictive else "allow.everyone.if.no.acl.found is true, granting unrestricted access when no ACLs exist for a resource",
            "recommendation": "Set allow.everyone.if.no.acl.found=false so that access is denied by default when no ACL rules match.",
            "details": {"property": "allow.everyone.if.no.acl.found", "value": allow_everyone or "(not set, defaults to restrictive behavior only if authorizer is set)"},
        })

        return findings

    # ── Network Security Checks ──────────────────────────

    def _check_network_security(self, configs: dict) -> list[dict]:
        findings = []

        # net-001: Plaintext listeners
        listeners = configs.get("listeners", "")
        has_plaintext = "PLAINTEXT" in listeners.upper() if listeners else True
        findings.append({
            "id": "net-001",
            "category": "Network Security",
            "check": "No Plaintext Listeners",
            "severity": "critical",
            "status": "fail" if has_plaintext else "pass",
            "message": f"Plaintext listener detected in listeners configuration: {listeners}" if has_plaintext else "No plaintext listeners found; all communication is encrypted",
            "recommendation": "Replace PLAINTEXT listeners with SSL or SASL_SSL to encrypt all client and inter-broker communication.",
            "details": {"property": "listeners", "value": listeners or "(not set)"},
        })

        # net-002: SSL keystore configured
        ssl_keystore = configs.get("ssl.keystore.location", "")
        has_keystore = bool(ssl_keystore)
        findings.append({
            "id": "net-002",
            "category": "Network Security",
            "check": "SSL Keystore Configured",
            "severity": "high",
            "status": "pass" if has_keystore else "fail",
            "message": f"SSL keystore configured at {ssl_keystore}" if has_keystore else "No SSL keystore configured; TLS encryption is not enabled",
            "recommendation": "Configure ssl.keystore.location with a valid JKS or PKCS12 keystore to enable TLS.",
            "details": {"property": "ssl.keystore.location", "value": ssl_keystore or "(not set)"},
        })

        # net-003: Advertised listeners use internal IPs
        advertised = configs.get("advertised.listeners", "")
        uses_all_interfaces = "0.0.0.0" in advertised if advertised else False
        findings.append({
            "id": "net-003",
            "category": "Network Security",
            "check": "Advertised Listeners Use Specific IPs",
            "severity": "medium",
            "status": "fail" if uses_all_interfaces else "pass",
            "message": "Advertised listeners contain 0.0.0.0, which may expose the broker to unintended networks" if uses_all_interfaces else "Advertised listeners use specific IP addresses or hostnames",
            "recommendation": "Use specific IP addresses or hostnames in advertised.listeners instead of 0.0.0.0.",
            "details": {"property": "advertised.listeners", "value": advertised or "(not set)"},
        })

        # net-004: SSL protocol version
        ssl_protocols = configs.get("ssl.enabled.protocols", "")
        has_old_tls = any(p in ssl_protocols for p in ["TLSv1,", "TLSv1.0", "TLSv1.1"]) if ssl_protocols else False
        findings.append({
            "id": "net-004",
            "category": "Network Security",
            "check": "Strong TLS Protocol Version",
            "severity": "high",
            "status": "fail" if has_old_tls else "pass",
            "message": f"Deprecated TLS versions found in ssl.enabled.protocols: {ssl_protocols}" if has_old_tls else "No deprecated TLS versions detected (TLSv1.0, TLSv1.1)",
            "recommendation": "Set ssl.enabled.protocols=TLSv1.2,TLSv1.3 and remove any older protocol versions.",
            "details": {"property": "ssl.enabled.protocols", "value": ssl_protocols or "(not set, JVM defaults apply)"},
        })

        # net-005: Listener binding to 0.0.0.0
        listener_val = configs.get("listeners", "")
        binds_all = "://0.0.0.0:" in listener_val or "://:9" in listener_val if listener_val else False
        findings.append({
            "id": "net-005",
            "category": "Network Security",
            "check": "Listeners Bound to Specific Interfaces",
            "severity": "medium",
            "status": "fail" if binds_all else "pass",
            "message": "Listeners are bound to all network interfaces (0.0.0.0), increasing attack surface" if binds_all else "Listeners are bound to specific interfaces or use hostname resolution",
            "recommendation": "Bind listeners to specific network interfaces instead of 0.0.0.0 to limit exposure.",
            "details": {"property": "listeners", "value": listener_val or "(not set)"},
        })

        return findings

    # ── Configuration Security Checks ────────────────────

    def _check_config_security(self, configs: dict) -> list[dict]:
        findings = []

        # cfg-001: auto.create.topics.enable
        auto_create = configs.get("auto.create.topics.enable", "")
        is_disabled = auto_create.lower() == "false" if auto_create else False
        findings.append({
            "id": "cfg-001",
            "category": "Configuration",
            "check": "Auto Topic Creation Disabled",
            "severity": "high",
            "status": "pass" if is_disabled else "fail",
            "message": "auto.create.topics.enable is disabled, preventing accidental topic creation" if is_disabled else "auto.create.topics.enable is true (or unset, defaults to true), topics can be created by any producing client",
            "recommendation": "Set auto.create.topics.enable=false in production to prevent uncontrolled topic proliferation.",
            "details": {"property": "auto.create.topics.enable", "value": auto_create or "(not set, defaults to true)"},
        })

        # cfg-002: unclean.leader.election.enable
        unclean = configs.get("unclean.leader.election.enable", "")
        unclean_disabled = unclean.lower() == "false" if unclean else True  # default is false in newer Kafka
        findings.append({
            "id": "cfg-002",
            "category": "Configuration",
            "check": "Unclean Leader Election Disabled",
            "severity": "high",
            "status": "pass" if unclean_disabled else "fail",
            "message": "Unclean leader election is disabled, preventing potential data loss" if unclean_disabled else "unclean.leader.election.enable is true, which may cause data loss during failover",
            "recommendation": "Set unclean.leader.election.enable=false to prevent out-of-sync replicas from becoming leaders.",
            "details": {"property": "unclean.leader.election.enable", "value": unclean or "(not set, defaults to false)"},
        })

        # cfg-003: min.insync.replicas
        min_isr = configs.get("min.insync.replicas", "")
        min_isr_val = 0
        try:
            min_isr_val = int(min_isr) if min_isr else 1
        except ValueError:
            min_isr_val = 1
        findings.append({
            "id": "cfg-003",
            "category": "Configuration",
            "check": "Minimum In-Sync Replicas >= 2",
            "severity": "high",
            "status": "pass" if min_isr_val >= 2 else "fail",
            "message": f"min.insync.replicas is set to {min_isr_val}" if min_isr else "min.insync.replicas is not set (defaults to 1), risking data durability",
            "recommendation": "Set min.insync.replicas=2 (or higher) combined with acks=all to ensure messages are written to multiple brokers before acknowledgment.",
            "details": {"property": "min.insync.replicas", "value": min_isr or "(not set, defaults to 1)"},
        })

        # cfg-004: default.replication.factor
        repl_factor = configs.get("default.replication.factor", "")
        repl_val = 0
        try:
            repl_val = int(repl_factor) if repl_factor else 1
        except ValueError:
            repl_val = 1
        findings.append({
            "id": "cfg-004",
            "category": "Configuration",
            "check": "Default Replication Factor >= 3",
            "severity": "medium",
            "status": "pass" if repl_val >= 3 else "fail",
            "message": f"default.replication.factor is {repl_val}" if repl_factor else "default.replication.factor is not set (defaults to 1), new topics will have no redundancy",
            "recommendation": "Set default.replication.factor=3 to ensure new topics are replicated across at least 3 brokers.",
            "details": {"property": "default.replication.factor", "value": repl_factor or "(not set, defaults to 1)"},
        })

        # cfg-005: delete.topic.enable
        delete_topic = configs.get("delete.topic.enable", "")
        delete_enabled = delete_topic.lower() != "false" if delete_topic else True
        findings.append({
            "id": "cfg-005",
            "category": "Configuration",
            "check": "Topic Deletion Control",
            "severity": "low",
            "status": "warning" if delete_enabled else "pass",
            "message": "Topic deletion is enabled; ensure ACLs restrict who can delete topics" if delete_enabled else "Topic deletion is disabled, protecting against accidental deletions",
            "recommendation": "If topic deletion is enabled (default), ensure ACL authorizer is configured to restrict delete operations to authorized principals only.",
            "details": {"property": "delete.topic.enable", "value": delete_topic or "(not set, defaults to true)"},
        })

        return findings

    # ── Data Protection Checks ───────────────────────────

    def _check_data_protection(self, configs: dict) -> list[dict]:
        findings = []

        # data-001: SSL truststore configured
        ssl_truststore = configs.get("ssl.truststore.location", "")
        has_truststore = bool(ssl_truststore)
        findings.append({
            "id": "data-001",
            "category": "Data Protection",
            "check": "SSL Truststore Configured",
            "severity": "high",
            "status": "pass" if has_truststore else "fail",
            "message": f"SSL truststore configured at {ssl_truststore}" if has_truststore else "No SSL truststore configured; mutual TLS (mTLS) is not enabled",
            "recommendation": "Configure ssl.truststore.location with a valid truststore containing trusted CA certificates to enable client certificate verification.",
            "details": {"property": "ssl.truststore.location", "value": ssl_truststore or "(not set)"},
        })

        # data-002: SSL client authentication
        ssl_client_auth = configs.get("ssl.client.auth", "")
        client_auth_ok = ssl_client_auth in ("required", "requested")
        findings.append({
            "id": "data-002",
            "category": "Data Protection",
            "check": "SSL Client Authentication Enabled",
            "severity": "medium",
            "status": "pass" if client_auth_ok else "fail",
            "message": f"SSL client authentication is set to '{ssl_client_auth}'" if client_auth_ok else "SSL client authentication is not configured or set to 'none'",
            "recommendation": "Set ssl.client.auth=required to enforce mutual TLS, ensuring clients present valid certificates.",
            "details": {"property": "ssl.client.auth", "value": ssl_client_auth or "(not set, defaults to none)"},
        })

        # data-003: Log segment encryption (check for encryption at rest indicators)
        log_dirs = configs.get("log.dirs", configs.get("log.dir", ""))
        findings.append({
            "id": "data-003",
            "category": "Data Protection",
            "check": "Log Directory Configured",
            "severity": "medium",
            "status": "pass" if log_dirs else "fail",
            "message": f"Log directories configured: {log_dirs}" if log_dirs else "No log.dirs or log.dir configured",
            "recommendation": "Configure log.dirs to dedicated storage and consider using encrypted filesystems (LUKS/dm-crypt) for encryption at rest.",
            "details": {"property": "log.dirs", "value": log_dirs or "(not set)"},
        })

        # data-004: SSL keystore password not empty
        keystore_pw = configs.get("ssl.keystore.password", "")
        truststore_pw = configs.get("ssl.truststore.password", "")
        has_pw = bool(keystore_pw) or bool(truststore_pw)
        findings.append({
            "id": "data-004",
            "category": "Data Protection",
            "check": "SSL Store Passwords Configured",
            "severity": "high",
            "status": "pass" if has_pw else "fail",
            "message": "SSL keystore/truststore passwords are configured" if has_pw else "SSL keystore or truststore passwords are not set; SSL stores may not be accessible or are unsecured",
            "recommendation": "Set ssl.keystore.password and ssl.truststore.password. Consider using password files or a credential store for production environments.",
            "details": {"keystore_password_set": bool(keystore_pw), "truststore_password_set": bool(truststore_pw)},
        })

        # data-005: SSL key password
        key_pw = configs.get("ssl.key.password", "")
        findings.append({
            "id": "data-005",
            "category": "Data Protection",
            "check": "SSL Private Key Password Configured",
            "severity": "medium",
            "status": "pass" if key_pw else "warning",
            "message": "SSL private key password is configured" if key_pw else "ssl.key.password is not set; the private key may share the keystore password or be unprotected",
            "recommendation": "Set ssl.key.password to protect the broker's private key independently from the keystore password.",
            "details": {"property": "ssl.key.password", "value": "(set)" if key_pw else "(not set)"},
        })

        return findings

    # ── OS-Level Security Checks ─────────────────────────

    def _check_os_security(self, client, host_ip: str, node_id: int) -> list[dict]:
        findings = []
        suffix = f" (Node {node_id}, {host_ip})"

        # os-001: Kafka running as non-root user
        try:
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                f"ps -eo user,pid,cmd | grep -E '[j]ava.*kafka' | head -1 | awk '{{print $1}}'",
                timeout=15,
            )
            kafka_user = stdout.strip() if exit_code == 0 else ""
            is_root = kafka_user == "root"
            findings.append({
                "id": f"os-001-n{node_id}",
                "category": "OS Security",
                "check": f"Kafka Running as Non-Root{suffix}",
                "severity": "critical",
                "status": "fail" if is_root else ("pass" if kafka_user else "warning"),
                "message": f"Kafka process is running as '{kafka_user}'" if kafka_user else "Could not determine Kafka process user (process may not be running)",
                "recommendation": "Run Kafka as a dedicated non-root user (e.g., 'kafka') to limit the impact of a potential compromise.",
                "details": {"user": kafka_user or "(unknown)", "host": host_ip},
            })
        except Exception as e:
            findings.append({
                "id": f"os-001-n{node_id}",
                "category": "OS Security",
                "check": f"Kafka Running as Non-Root{suffix}",
                "severity": "critical",
                "status": "error",
                "message": f"Failed to check Kafka process user: {e}",
                "recommendation": "Verify SSH access and process visibility.",
            })

        # os-002: Kafka install directory permissions
        try:
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                f"stat -c '%a %U %G' {settings.KAFKA_INSTALL_DIR} 2>/dev/null || stat -f '%Lp %Su %Sg' {settings.KAFKA_INSTALL_DIR}",
                timeout=15,
            )
            if exit_code == 0 and stdout.strip():
                parts = stdout.strip().split()
                perms = parts[0] if parts else ""
                owner = parts[1] if len(parts) > 1 else ""
                world_readable = perms.endswith("7") or perms.endswith("5") or perms.endswith("6") if len(perms) >= 3 else False
                findings.append({
                    "id": f"os-002-n{node_id}",
                    "category": "OS Security",
                    "check": f"Kafka Install Directory Permissions{suffix}",
                    "severity": "high",
                    "status": "fail" if world_readable else "pass",
                    "message": f"Kafka install directory permissions: {perms}, owner: {owner}" + (", world-accessible" if world_readable else ""),
                    "recommendation": f"Set restrictive permissions on {settings.KAFKA_INSTALL_DIR}: chmod 750 and chown to the kafka user.",
                    "details": {"permissions": perms, "owner": owner, "path": settings.KAFKA_INSTALL_DIR},
                })
            else:
                findings.append({
                    "id": f"os-002-n{node_id}",
                    "category": "OS Security",
                    "check": f"Kafka Install Directory Permissions{suffix}",
                    "severity": "high",
                    "status": "warning",
                    "message": f"Could not determine permissions for {settings.KAFKA_INSTALL_DIR}",
                    "recommendation": f"Verify the Kafka installation directory exists at {settings.KAFKA_INSTALL_DIR}.",
                })
        except Exception as e:
            findings.append({
                "id": f"os-002-n{node_id}",
                "category": "OS Security",
                "check": f"Kafka Install Directory Permissions{suffix}",
                "severity": "high",
                "status": "error",
                "message": f"Failed to check directory permissions: {e}",
                "recommendation": "Verify SSH access.",
            })

        # os-003: JMX port exposed without authentication
        try:
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                "ps -ef | grep -o 'com.sun.management.jmxremote[^ ]*' | tr ' ' '\\n' | sort -u",
                timeout=15,
            )
            jmx_output = stdout.strip() if exit_code == 0 else ""
            jmx_auth_disabled = "jmxremote.authenticate=false" in jmx_output
            jmx_ssl_disabled = "jmxremote.ssl=false" in jmx_output
            jmx_enabled = "jmxremote" in jmx_output

            if jmx_enabled and jmx_auth_disabled:
                status = "fail"
                msg = "JMX is enabled without authentication, allowing unauthenticated remote access to management interface"
            elif jmx_enabled and jmx_ssl_disabled:
                status = "warning"
                msg = "JMX is enabled with authentication but without SSL encryption"
            elif jmx_enabled:
                status = "pass"
                msg = "JMX is enabled with authentication"
            else:
                status = "pass"
                msg = "JMX remote access is not explicitly enabled via command line flags"

            findings.append({
                "id": f"os-003-n{node_id}",
                "category": "OS Security",
                "check": f"JMX Authentication{suffix}",
                "severity": "high",
                "status": status,
                "message": msg,
                "recommendation": "If JMX is enabled, set com.sun.management.jmxremote.authenticate=true and com.sun.management.jmxremote.ssl=true.",
                "details": {"jmx_flags": jmx_output or "(none detected)", "host": host_ip},
            })
        except Exception as e:
            findings.append({
                "id": f"os-003-n{node_id}",
                "category": "OS Security",
                "check": f"JMX Authentication{suffix}",
                "severity": "high",
                "status": "error",
                "message": f"Failed to check JMX settings: {e}",
                "recommendation": "Verify SSH access.",
            })

        # os-004: Log directory permissions
        try:
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                f"stat -c '%a %U %G' {settings.KAFKA_DATA_DIR} 2>/dev/null || stat -f '%Lp %Su %Sg' {settings.KAFKA_DATA_DIR} 2>/dev/null || echo 'NOT_FOUND'",
                timeout=15,
            )
            output = stdout.strip() if exit_code == 0 else ""
            if output and output != "NOT_FOUND":
                parts = output.split()
                perms = parts[0] if parts else ""
                owner = parts[1] if len(parts) > 1 else ""
                world_readable = perms.endswith("7") or perms.endswith("5") or perms.endswith("6") or perms.endswith("4") if len(perms) >= 3 else False
                findings.append({
                    "id": f"os-004-n{node_id}",
                    "category": "OS Security",
                    "check": f"Kafka Data Directory Permissions{suffix}",
                    "severity": "high",
                    "status": "fail" if world_readable else "pass",
                    "message": f"Data directory permissions: {perms}, owner: {owner}" + (", world-accessible" if world_readable else ""),
                    "recommendation": f"Set restrictive permissions on {settings.KAFKA_DATA_DIR}: chmod 700 and chown to the kafka user.",
                    "details": {"permissions": perms, "owner": owner, "path": settings.KAFKA_DATA_DIR},
                })
            else:
                findings.append({
                    "id": f"os-004-n{node_id}",
                    "category": "OS Security",
                    "check": f"Kafka Data Directory Permissions{suffix}",
                    "severity": "high",
                    "status": "warning",
                    "message": f"Data directory {settings.KAFKA_DATA_DIR} not found or inaccessible",
                    "recommendation": f"Verify that {settings.KAFKA_DATA_DIR} exists and has restrictive permissions.",
                })
        except Exception as e:
            findings.append({
                "id": f"os-004-n{node_id}",
                "category": "OS Security",
                "check": f"Kafka Data Directory Permissions{suffix}",
                "severity": "high",
                "status": "error",
                "message": f"Failed to check data directory permissions: {e}",
                "recommendation": "Verify SSH access.",
            })

        # os-005: Open Kafka ports via firewall check
        try:
            exit_code, stdout, _ = SSHManager.exec_command(
                client,
                "ss -tlnp 2>/dev/null | grep -E ':(9092|9093|9094|2181|8083)' || netstat -tlnp 2>/dev/null | grep -E ':(9092|9093|9094|2181|8083)'",
                timeout=15,
            )
            open_ports = stdout.strip() if exit_code == 0 else ""
            port_lines = [line.strip() for line in open_ports.split("\n") if line.strip()] if open_ports else []
            exposed_count = len(port_lines)
            has_all_interfaces = any("0.0.0.0" in line or ":::" in line for line in port_lines)

            findings.append({
                "id": f"os-005-n{node_id}",
                "category": "OS Security",
                "check": f"Network Port Exposure{suffix}",
                "severity": "medium",
                "status": "fail" if has_all_interfaces else "pass",
                "message": f"{exposed_count} Kafka-related port(s) detected" + (", some bound to all interfaces (0.0.0.0)" if has_all_interfaces else ", bound to specific interfaces"),
                "recommendation": "Bind Kafka ports to specific network interfaces and use firewall rules (iptables/firewalld) to restrict access to trusted networks only.",
                "details": {"port_bindings": port_lines, "host": host_ip},
            })
        except Exception as e:
            findings.append({
                "id": f"os-005-n{node_id}",
                "category": "OS Security",
                "check": f"Network Port Exposure{suffix}",
                "severity": "medium",
                "status": "error",
                "message": f"Failed to check open ports: {e}",
                "recommendation": "Verify SSH access and that ss or netstat is available.",
            })

        return findings

    # ── Utility Methods ──────────────────────────────────

    def _category_summary(self, findings: list, category: str) -> dict:
        cat_findings = [f for f in findings if f["category"] == category]
        total = len(cat_findings)
        passed = sum(1 for f in cat_findings if f["status"] == "pass")
        failed = sum(1 for f in cat_findings if f["status"] == "fail")
        warnings = sum(1 for f in cat_findings if f["status"] == "warning")
        errors = sum(1 for f in cat_findings if f["status"] == "error")
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "errors": errors,
            "score": int((passed / total) * 100) if total > 0 else 0,
        }

    def _parse_properties(self, content: str) -> dict:
        props = {}
        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                props[key.strip()] = value.strip()
        return props


security_scanner = SecurityScanner()
