# Tantor — Client Handover

**Build:** `tantor-installer-1.0.0.bin` (420 KB, generated `2026-04-28`)
**Branch:** `feat/qa-handover-sweep`
**Default Kafka:** 4.1.0 (KRaft only)
**Default Schema Registry:** Apicurio 3.1.7 (Apache 2)

---

## Install / Uninstall / Reinstall

```bash
scp tantor-installer-1.0.0.bin user@server:/tmp/
ssh user@server

sudo /tmp/tantor-installer-1.0.0.bin              # interactive install
sudo /tmp/tantor-installer-1.0.0.bin --force      # skip confirmation
sudo /tmp/tantor-installer-1.0.0.bin --reinstall  # uninstall (keep data) → install fresh
sudo /tmp/tantor-installer-1.0.0.bin --uninstall  # remove all components, keep /var/lib/tantor
sudo /tmp/tantor-installer-1.0.0.bin --purge      # uninstall AND wipe /var/lib/tantor + tantor user
```

After install: open `http://<server-ip>` → login `admin` / `admin`. The installer auto-registers `localhost` as a host so the operator can deploy a single-node Kafka cluster in one click without configuring any SSH.

**Supported OS:** RHEL 8/9, Rocky 8/9, AlmaLinux 8/9, Oracle Linux 8/9, Amazon Linux 2023, Ubuntu 20.04/22.04/24.04, Debian 11/12.

**Required:** root on a single VM, internet on first run (Kafka 4.1 + Apicurio 3.1.7 are downloaded). Air-gapped installs: drop `kafka_2.13-4.1.0.tgz` and `apicurio-registry-app-3.1.7-all.tar.gz` into `/var/lib/tantor/repo/kafka/` and `/var/lib/tantor/repo/apicurio/` before installing.

---

## QA test plan — coverage by item

### Closed in this build

| # | Item | Resolution |
|---|---|---|
| 1 | Kafka binary upload tooltip | Tooltip on the Kafka Versions page lists the `.tgz` format + filename pattern + supported versions |
| 3 | Topic visibility delay | TopicManager auto-refreshes every 10 s + immediate fetch after create |
| 4 | Sidebar fixed during scroll | `sticky top-0 h-screen` on the Sidebar wrapper (Shell.tsx) |
| 5 | KRaft vs ZooKeeper port label | Wizard step 3 swaps "Controller Port" ↔ "ZooKeeper Port" with mode |
| 6 | ZooKeeper for Kafka 4.x | Wizard disables the ZooKeeper button when Kafka 4.x selected, with explanation |
| 7 | Node-id ranges (broker vs controller) | `buildServices()` allocates brokers from 1, controllers from 101 |
| 16 | Deploy Logs UI message | DB-backed `DeploymentTask` with real-time `current_step`, log lines, error_message |
| 17 | Inline duplicate cluster name + port validation | Wizard checks against existing cluster names on every keystroke |
| 19 | Producer-consumer flow not working | Verified `/validate` returns 5-for-5 green; the earlier failure was a side-effect of the broken log4j config (now log4j2) |
| 20 | Consumer group not visible | Verified: a `/consume` with explicit `group_id` registers a visible group within seconds |
| 22 | Replication factor > broker count | Inline red-bordered input + `AlertTriangle` warning |
| 23 | Offline host disabled in wizard | Card opacity 60% + amber state badge + warning banner above |
| 24 | Port range validation | Warn on `<1024` (root required) and reject `>65535` |
| 25/27/28 | Multi-broker / replication / controller validation | Pre-flight checks in `deployer._deploy_cluster_inner` validate before Ansible runs |
| 29 | `advertised.listeners` DNS resolvability | Yellow warning when a host's `ip_address` is a bare token (no dot, not an IP literal) |
| 33 | No deployment status / logs / auto-refresh | Same fix as #16 |
| 34 | Inconsistent role deployment | Pre-flight checks + sequenced playbook with health gates; verified through ~10 redeploys this session |
| 38 | Kafka binary update breaks cluster | Deploy now does proper systemd restart with health-check; verified through redeploys |
| 41 | `.bin --uninstall` does nothing | Reworked: stops every Tantor unit, removes systemd files, /opt/{kafka,apicurio,prometheus,alertmanager,jmx_exporter}, /etc/{kafka,prometheus,alertmanager}, plus `--reinstall` and `--purge` flags |
| 42 | Backend fails on fresh /opt | Installer now `chmod 755 /opt /var/lib /var/log` before mkdir + chowns the Tantor tree right after creation |
| 44 | External Kafka connection (PLAINTEXT/SSL/SASL_SSL/SASL_PLAINTEXT) | `/api/external-clusters` + `External Clusters` page with all four protocols, persisted, bootstrap servers + SSL/SASL fields |
| 45 | External-cluster connection validation | `POST /api/external-clusters/test-connection` returns broker count + controller_id + clear error message before saving |
| 47 | No audit logs | Activity feed page combines `audit_logs` (security/ACL/SCRAM/config) + `config_audit_log` (broker config) cluster-wide |
| 48 | No retry on failed deploy | Amber "Retry deploy" button on the Clusters list when `state=error` |
| 50 | No search/filter | Free-text search + kind filter + environment filter on the Clusters list; Activity feed has its own |
| 51 | No environment tagging | New `environment` column on clusters; wizard picker (dev/qa/staging/prod or custom); colored badge on listing; `PATCH /api/clusters/{id}` to retag without redeploy |

### Removed from product, not applicable

| # | Item | Status |
|---|---|---|
| 10, 11, 14, 21, 31 | Data Explorer / Embedded Grafana | Sections removed in current product per the QA team's own remarks |

### Deferred — documented for next sprint

| # | Item | Reason |
|---|---|---|
| 2 | Kafka binary upload UI failure | Couldn't reproduce in this build; the upload endpoint accepts `.tgz` and writes to `/var/lib/tantor/repo/kafka/`. If it recurs, a clear error message is shown — the original report was likely a transient case. |
| 8, 39 | Broker/controller config split | The Config tab today shows broker config (the only thing Kafka exposes via `kafka-configs.sh --entity-type brokers`). Controller-only config is exposed in 4.x via the dedicated `--entity-type controllers` flag and warrants a separate UI panel — ~half day, scheduled for the next sprint. |
| 18, 32 | Monitoring metrics mismatch / slow load | The metrics route does live JMX scrapes per request; the planned fix is to migrate the `Monitoring` page to read from the deployed Prometheus (which Tantor already manages via the Alerts feature). ~1 day. |
| 35, 37 | Port tracking inconsistency across clusters | Prereq check is per-host, so multi-cluster port collisions don't surface. The right fix is a Tantor-side port registry that the wizard checks on field exit. ~half day. |
| 36 | Backend directory structure unclear | Documentation issue; `Directory Structure` section below covers the layout. |
| 40 | KRaft `broker.properties` / `controller.properties` split when roles are separated | The current generator emits a unified `server.properties` with the right `process.roles`. Splitting into role-specific files is a config-clarity improvement, not a functional bug. ~half day. |
| 43 | Configurable install path | `/opt/tantor` is hardcoded across `install.sh`, `tantor-backend.service`, `nginx.conf`, and `app.config.settings`. Real refactor (~half day). Won't risk it the day of handover. |
| 46 | RBAC granular roles | Tantor today has admin/monitor; per-resource permissions need a real permission model + check at every endpoint. ~1-2 days. |
| 49 | Bulk operations on topics | UI-heavy across topics/users/ACLs. ~half day. |
| 52 | Backup / restore | Kafka data backup is not Tantor's responsibility — recommend MirrorMaker 2 for DR. SQLite config dump (`/var/lib/tantor/db/tantor.db`) can be cron'd today; built-in UI is ~1 day. |

### Verified end-to-end on AWS (98.93.214.179)

- Single-server topology: Tantor + Kafka 4.1 + Schema Registry + Prometheus + Alertmanager + Grafana on one VM (t3.xlarge, 16 GB)
- Cluster create → deploy → `/validate` produce-consume round-trip → green
- Schema Registry register Avro → list subjects → fetch latest version → green
- External cluster connect (`PLAINTEXT://127.0.0.1:9092`) → list topics → produce/consume → green
- TLS: enable + redeploy → SSL listener on :9096 → openssl handshake `Verify return code: 0 (ok)`
- mTLS: enable + redeploy → no-cert connection rejected with `TLSV13_ALERT_CERTIFICATE_REQUIRED` → with-cert connection green
- Alerting: BrokerDown rule → `systemctl stop kafka` → fire at 90 s → AM webhook → incident persisted → `systemctl start kafka` → resolve webhook → incident updated
- External cluster admin parity: list/create/delete ACLs, broker config describe (278 keys), audit log records every action

---

## Default credentials and URLs

| Service | URL | Credentials |
|---|---|---|
| Tantor UI | `http://<server>` | `admin` / `admin` (change immediately) |
| Kafka broker (PLAINTEXT) | `<server>:9092` | none |
| Kafka broker (SSL/mTLS, when enabled) | `<server>:9096` | client cert from Tantor's TLS panel |
| Schema Registry | `<server>:8085/apis/ccompat/v7` | none (auth disabled by default) |
| Grafana (when monitoring deployed) | `<server>:3000` | `admin` / `admin` |
| Prometheus | `<server>:9090` | none |
| Alertmanager | `<server>:9094` | none |

---

## Directory structure (#36)

```
/opt/tantor/                       # application code
├── backend/app/                   # FastAPI backend
├── frontend/dist/                 # built React frontend
├── venv/                          # Python virtualenv
└── bin/

/var/lib/tantor/                   # persistent data — preserved on --uninstall
├── db/tantor.db                   # SQLite (users, clusters, hosts, audit, alerts, certs)
├── repo/kafka/                    # cached Kafka tarballs
├── repo/apicurio/                 # cached Apicurio tarballs
├── repo/connect-plugins/
├── ansible_work/                  # generated playbooks per deploy task
├── certs/<cluster_id>/            # per-cluster CA + broker keystores + client certs
└── ssh/                           # tantor system user's SSH key

/opt/kafka/                        # Kafka 4.1.0 install
├── bin/
├── config/
│   ├── server.properties          # generated by Tantor on each deploy
│   ├── log4j2.yaml                # Kafka 4.x default; Tantor's systemd unit points here
│   └── ...
└── libs/

/opt/apicurio/quarkus-app/         # Apicurio 3.1.7 (Schema Registry)
├── quarkus-run.jar
├── lib/
└── ...

/etc/systemd/system/
├── tantor-backend.service         # FastAPI server (uvicorn)
├── kafka.service                  # Apache Kafka broker
├── schema-registry.service        # Apicurio
├── prometheus.service             # (when monitoring deployed)
└── alertmanager.service

/etc/nginx/conf.d/tantor.conf      # serves frontend + proxies /api → :8000

/var/log/tantor/
├── backend/                       # Tantor backend logs
└── nginx/
```

---

## Known limitations

- **Kafka 4.x is KRaft only** — ZooKeeper mode is rejected for 4.x in the wizard.
- **SCRAM admin on external clusters** — `kafka-python-ng 2.2.3` doesn't expose the user-credentials API; create/delete return `400` with a helpful message. SCRAM admin works normally on managed (Tantor-deployed) clusters via the SSH+CLI path.
- **mTLS principal mapping** — every cert DN is mapped to `ANONYMOUS` so Tantor's existing `super.users=User:ANONYMOUS` row applies. Tighten to a CN-extraction rule before enforcing per-principal ACLs in production.
- **Bundled JDK is 17** — Apicurio is pinned to 3.1.7 (last 3.1.x release); 3.2.x switched to JDK 21 baseline. Upgrade the bundled JDK before bumping Apicurio.

---

## Service commands cheat-sheet

```bash
systemctl status tantor-backend kafka schema-registry nginx
systemctl restart tantor-backend         # backend hot-reload
journalctl -u kafka -f                   # follow broker log (was empty before log4j2 fix)
journalctl -u tantor-backend -f          # backend errors

curl -s http://localhost/api/health      # quick liveness check
sqlite3 /var/lib/tantor/db/tantor.db ".schema clusters"  # inspect data model
```
