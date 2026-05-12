# Architecture

This document gives a developer an end-to-end mental model of Tantor in roughly 15 minutes of reading. If you're picking up a bug ticket and need to find "where is X handled," start here.

---

## 1. The big picture

```
                       ┌───────────────────────┐
   Browser ── HTTPS ──▶│      nginx (:443)     │── /api/* ──┐
                       │  serves dist/ static  │            │
                       └───────────────────────┘            ▼
                                                ┌───────────────────────┐
                                                │   FastAPI backend     │
                                                │   uvicorn :8000       │
                                                │   2 workers           │
                                                └─┬───────┬───────┬─────┘
                                                  │       │       │
                                                  ▼       ▼       ▼
                                              SQLite   SSH+    kafka-python
                                              (DB)    Ansible  (TCP to brokers)
                                                        │            │
                                                        ▼            ▼
                                              ┌─────────────────────────┐
                                              │  Target Linux hosts     │
                                              │  ┌───────────────────┐  │
                                              │  │ kafka-prod-XYZ    │  │
                                              │  │ kafka-staging-ABC │  │  ◀── per-cluster systemd units
                                              │  │ schema-registry   │  │
                                              │  │ prometheus + grafana │
                                              │  └───────────────────┘  │
                                              └─────────────────────────┘
```

**Three runtime processes** on the Tantor host:
1. **nginx** — terminates TLS, serves the React bundle from `<TANTOR_HOME>/frontend/dist`, proxies `/api/*` to the backend.
2. **tantor-backend** — uvicorn running FastAPI; 2 workers; SQLite file at `<TANTOR_DATA>/db/tantor.db`.
3. **(optional)** Prometheus + Alertmanager + Grafana + JMX exporter, deployed *per Kafka cluster* on the broker hosts (not on the Tantor host by default).

---

## 2. Backend (`backend/`)

FastAPI + SQLAlchemy 2.0 + SQLite. The single-binary, single-machine choice. No external DB to provision; SQLite handles thousands of clusters before becoming a bottleneck.

### 2.1 Layers

| Layer | What lives here |
|---|---|
| `app/api/` | HTTP routes. Thin — parse the request, call a service, return the response. |
| `app/schemas/` | Pydantic request/response models. Public contract. |
| `app/models/` | SQLAlchemy ORM models. Database contract. |
| `app/services/` | Business logic. Most of the code. |
| `app/templates/` | Jinja2 templates (Ansible playbooks, systemd units, broker `server.properties`). |

### 2.2 Key services

| Service | Responsibility |
|---|---|
| `deployer.py` | Orchestrates `deploy_cluster` (managed) and `deploy_schema_registry`. Pre-flight checks (Java, ports, RF), generates Ansible playbook + inventory, runs it with `ansible-playbook`, streams logs into `DeploymentTask`. |
| `ssh_manager.py` | Paramiko SSH connection pool. One `SSHManager.connect()` context manager per cluster operation. Keepalive thread. |
| `kafka_admin.py` | All the day-2 ops on managed clusters: list/create/delete topics, produce/consume, consumer groups, ACLs, SCRAM users, broker config. Shells out via SSH for write operations; uses `kafka-python` over TCP for `list_topics` (the hot path) since v1.4.4. |
| `external_admin.py` | Same surface for external clusters via `kafka-python` only (no SSH). |
| `config_generator.py` | Renders the Jinja2 templates that produce `server.properties` per broker, systemd units, Apicurio config. |
| `cluster_manager.py` | Start / stop / status of all services in a cluster. Resolves per-cluster systemd unit names. |
| `cluster_paths.py` | The per-cluster paths trick: every cluster gets its own `/opt/kafka-<slug>-<id>/`, `/var/lib/kafka-<slug>-<id>/data/`, `kafka-<slug>-<id>.service`. Multi-cluster on the same host coexist instead of fighting over `/opt/kafka`. |
| `port_preflight.py` | SSH `ss -tnlp` + `systemctl status` to detect port conflicts before ansible runs. Resolves PID → owning systemd unit name. |
| `cert_manager.py` | Per-cluster CA (Tantor-generated or operator-uploaded), broker keystores (PKCS#12), truststores (PEM), Fernet-encrypted at rest. |
| `cluster_linking_manager.py` | MirrorMaker 2 deploy + config generation, with SASL/SSL bootstrap support for external destinations. |
| `monitoring_deployer.py` | Auto-deploys Prometheus + Alertmanager + Grafana + JMX exporter on cluster create. Per-cluster JMX port to avoid `:7071` collisions on multi-cluster hosts. |
| `auth_service.py` | bcrypt password hashing, JWT issue + verify, LDAP bind + group→role mapping. |
| `migrations.py` | Lightweight runtime ALTER TABLE for SQLite. New columns added in any release get added at startup; idempotent. |
| `port_preflight.py` | Pre-deploy port-conflict detection over SSH (`ss -tnlH`). Resolves PID → systemd unit name so error messages name the holding cluster. |

### 2.3 Deploy flow (managed cluster)

Customer clicks Deploy → POST `/api/clusters/{id}/deploy` → `init_task(task_id)` → `BackgroundTasks.add_task(deploy_cluster, ...)`. The background task:

1. **Pre-flight checks** (`deployer.py:181` onward):
   1. Unique node_ids
   2. Hosts exist + online
   3. KRaft needs at least one controller
   4. RF ≤ broker count
   5. **Check 5a**: Java + package manager present on every remote host (v1.4.3 #17)
   6. **Check 5b**: Stop this cluster's own previous systemd unit (idempotent on redeploy) + scan ports — fail fast with the holding unit name if anything's taken (v1.4.3 #16)
2. **Per-service config generation** (`config_generator.py`) — one `server.properties` per broker, rendered from `kraft_server.properties.j2` with the cluster's per-cluster install dir, data dir, port set, optional SSL listener.
3. **Ansible workspace** (`ansible_runner.py`) — `inventory.yml` (groups by role), `playbook.yml` (rendered from `deploy_kafka.yml.j2`), per-host config files, per-host systemd unit files.
4. **Ansible run** — `ansible-playbook` with stdout streamed into `DeploymentTask.logs` (line-buffered so the UI shows progress).
5. **Mark cluster running** if exit=0, else `state=error` with the ansible exit code in `error_message`.

### 2.4 Per-cluster paths

This is the key abstraction that makes multi-cluster-on-one-host work:

```python
# backend/app/services/cluster_paths.py
def assign_paths_for_new_cluster(cluster):
    short = cluster.id[:8]
    slug = _slug(cluster.name)
    suffix = f"{slug}-{short}"
    cluster.kafka_install_dir = f"/opt/kafka-{suffix}"
    cluster.kafka_data_dir    = f"/var/lib/kafka-{suffix}/data"
    cluster.kafka_unit_name   = f"kafka-{suffix}.service"
```

Every code path that touches systemd or Kafka binaries goes through `cluster_paths.unit_name(cluster)` / `.install_dir(cluster)` / `.data_dir(cluster)`. **Never hardcode `kafka.service` or `/opt/kafka`** in new code — that pattern caused the v1.4.2 state-sync regression where refresh always flipped clusters to "stopped".

### 2.5 Database schema (SQLite)

Core tables (each one is a SQLAlchemy model in `app/models/`):
- `users` — id, username, hashed_password, role (admin/monitor), auth_source (local/ldap), ldap_dn, token_version, is_active
- `hosts` — Linux machines registered for cluster deploy; ip_address, ssh_port, username, encrypted_credential (Fernet)
- `clusters` — name, kafka_version, mode (kraft/zookeeper), state, config_json, kind (managed/external), kafka_install_dir, kafka_data_dir, kafka_unit_name, ssl_enabled, mtls_required, encrypted_tls_password
- `services` — one row per Kafka role on a host: cluster_id, host_id, role, node_id, status
- `audit_logs` — security/user/ACL changes with actor_user_id, actor_username, action, resource_type, resource_name
- `config_audit_log` — broker config edits with old_value, new_value, changed_by, rollback support
- `monitoring_configs` — per-cluster prometheus_url + grafana_url + alertmanager_url
- `alert_rules` + `alert_incidents` — configured rule + firing/resolved history
- `deployment_tasks` — async deploy state + log lines + final exit code
- `kafka_users` — local mirror of SCRAM users we created (encrypted password) so we can show the list without re-querying Kafka
- `ldap_configs` — LDAP bind config + Fernet-encrypted bind_password + tls settings + group→role mapping
- `cluster_links` — MirrorMaker 2 link definitions + mm2.properties + deploy state

### 2.6 Auth — JWT + token_version (v1.4.3 #22)

Login returns `{access_token, refresh_token}` JWTs. Every request hits `get_current_user` which decodes the JWT and verifies `payload.tv == user.token_version`. Admins changing a user's role or deactivating them bumps `user.token_version`, so all in-flight tokens for that user get 401 on their next call. No more "I demoted Bob to monitor but he's still admin in his open tab".

---

## 3. Frontend (`frontend/`)

React 19 + Vite + TypeScript. Tailwind. ~70 routes/components, ~1 MB bundle (~275 KB gzip).

### 3.1 Top-level routes

| Route | Component | Purpose |
|---|---|---|
| `/` | `Dashboard.tsx` | Stat cards + recent activity |
| `/clusters` | `Clusters.tsx` | Cluster list, Quick Deploy button, filters |
| `/clusters/new` | `NewCluster.tsx` | Wizard (hosts → role assignment → config → review) |
| `/clusters/:id` | `ClusterDetail.tsx` | Tabs: Overview, Topics, Produce, Consume, Groups, Connect, ksqlDB, Schema Registry, Security, Validate, Config, Rebalance, Restart, Upgrade, Monitoring, Lifecycle, Capacity, Service Logs |
| `/external-clusters` | `ExternalClusters.tsx` | Import existing Kafka, edit, test connection, list topics |
| `/federation` | `Federation.tsx` | Cross-cluster overview, topic search |
| `/cluster-linking` | `ClusterLinking.tsx` | MirrorMaker 2 links + deploy progress |
| `/monitoring` | `Monitoring.tsx` | Cross-cluster monitoring overview |
| `/activity` | `Activity.tsx` | Combined audit (security + config edits) |
| `/alerts` | `Alerts.tsx` | Alert rules + incidents |
| `/users` | `UserManagement.tsx` | Admin user CRUD; LDAP source badge |
| `/ldap-settings` | `LdapSettings.tsx` | LDAP config wizard |
| `/versions` | `KafkaVersions.tsx` | Upload custom Kafka tarball |
| `/hosts` | `HostsPage.tsx` | SSH host registration + reachability test |

### 3.2 Data flow

- **`lib/api.ts`** — axios instance with bearer-token interceptor; one function per endpoint.
- **`lib/auth.ts`** — token storage (localStorage), `getAccessToken()`, role helpers.
- Pages own their state with `useState` + `useEffect`. No global store today (kept simple — Tantor pages are mostly independent).
- Polling: most list pages poll every 10–15s when visible. Background polls are "silent" — they update data without flashing the loading spinner (v1.4.3 #9).

### 3.3 Cluster Detail — the most complex page

`ClusterDetail.tsx` has 16 tabs. Each tab is its own component under `components/clusters/`. The `visibleTabs` filter at the top decides which tabs show based on:
- `cluster.kind` (managed vs external)
- `cluster.state` (some tabs require running)
- which `Service` roles exist (Connect/ksqlDB/Schema Registry tabs only show if those services were deployed)

This is where `cluster_paths` consistency matters most — if you add a feature that touches systemd or broker filesystem paths, the backend MUST resolve via `cluster_paths.unit_name(cluster)` not a hardcoded `kafka.service`.

---

## 4. Installer (`install.sh` + `build-installer.sh`)

The installer is a single self-extracting `.bin` that runs on a fresh Linux VM and produces a working Tantor.

### 4.1 What's in the .bin

`build-installer.sh` packages:
- `backend/` source + `requirements.txt`
- `frontend/dist/` pre-built (so the installer doesn't need Node.js to run)
- `install.sh` entrypoint
- `installer/systemd/`, `installer/config/` static files

The .bin uses a `tail +<line> | tar xz` self-extract trick, then runs `install.sh`.

### 4.2 install.sh phases

1. Parse args (`--force`, `--tls`, `--install-dir`, `--purge`, `--reinstall`)
2. Read `/etc/tantor/install.conf` if it exists (so `--reinstall` honors the previous `--install-dir`)
3. Detect OS (Debian-family vs RHEL-family)
4. Install system packages (python, nginx, ssh client)
5. Create `tantor` user + dirs
6. Copy backend + frontend into `$TANTOR_HOME`
7. `python3 -m venv` + `pip install -r requirements.txt`
8. Mint or copy TLS cert (if `--tls`)
9. Write nginx config (HTTPS server block if TLS, otherwise HTTP)
10. Write `tantor-backend.service` systemd unit with `Environment=TANTOR_HOME/...`
11. Start nginx + tantor-backend
12. Persist choices to `/etc/tantor/install.conf`

### 4.3 Purge (v1.4.3 #14/#15)

`--purge` is destructive but **atomic**:
1. `systemctl list-unit-files | awk '$1 ~ /^kafka-.*\.service/'` — find every per-cluster Kafka unit
2. `systemctl stop && disable` each
3. `pkill -9 -f /opt/kafka` — murder any forked JVMs that systemd didn't reach
4. `rm /etc/systemd/system/kafka-*.service`
5. `rm -rf /opt/kafka-* /var/lib/kafka-* /var/log/kafka-*`
6. `rm -rf $TANTOR_HOME $TANTOR_DATA $TANTOR_LOG`
7. `userdel -r tantor`
8. `dpkg --purge grafana` (or `dnf remove`) — Grafana ships as a package, so plain rm isn't enough

---

## 5. Where to find each customer feature

Quick lookup for "where do I edit X":

| Feature | Source path |
|---|---|
| Cluster create wizard | `frontend/src/components/clusters/ClusterWizard.tsx` |
| Cluster create API | `backend/app/api/clusters.py::create_cluster` + `deploy_cluster` |
| Quick Deploy (port auto-pick) | `backend/app/api/clusters.py::quick_deploy` |
| Per-cluster paths | `backend/app/services/cluster_paths.py` |
| Topic list (fast path) | `backend/app/services/kafka_admin.py::_list_topics_via_kafka_python` |
| Topic list cache | `backend/app/services/kafka_admin.py` (`_TOPIC_LIST_CACHE`) |
| Port pre-flight | `backend/app/services/port_preflight.py` |
| Rolling restart | `backend/app/services/rolling_restart_manager.py` |
| Audit log + actor | `backend/app/services/kafka_admin.py::_audit` + `app/api/activity.py` |
| SSL/mTLS toggle | `backend/app/api/security_tls.py` + `backend/app/services/cert_manager.py` |
| Cert upload UI | `frontend/src/components/clusters/SecurityManager.tsx` (Certificates tab) |
| LDAP auth + role mapping | `backend/app/services/auth_service.py::authenticate_ldap` + `app/services/ldap_service.py` |
| External cluster import | `backend/app/api/external_clusters.py` + `frontend/src/pages/ExternalClusters.tsx` |
| External cluster monitoring | `backend/app/api/monitoring.py::_external_cluster_metrics` |
| Cluster linking | `backend/app/services/cluster_linking_manager.py` + `frontend/src/pages/ClusterLinking.tsx` |
| Schema Registry per-cluster | `frontend/src/components/clusters/ClusterSchemaRegistry.tsx` + `backend/app/services/deployer.py::deploy_schema_registry` |
| Regression battery | `tests/regression/run_all.sh` |

If the answer isn't here, grep first then update this section in your PR.
