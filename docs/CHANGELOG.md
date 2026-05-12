# Changelog

Customer-facing release notes for Tantor. Issue numbers reference items in the feedback tracking sheet from the customer test pass. For internal commit history see the git log.

---

## 1.4.4 — 2026-05-12

### Performance

- **Topic list 2-3× faster.** Switched managed `list_topics` from `kafka-topics.sh` over SSH (3-4s of JVM cold start per call) to `kafka-python` over TCP directly. SSH+CLI kept as fallback for degraded environments where the backend can't reach the broker's listener port. Measured on AWS RHEL 9.7 with 4 topics: 3.0s → 1.3s.
- **Topic list cache.** 5s in-memory cache invalidated on create/delete so user actions are still snappy. The Topics tab polls every 10s so most polls hit cold; this mainly helps when an operator clicks Refresh themselves.

### Verified end-to-end

- **Custom install dir** (`--install-dir /data/tantor`) confirmed end-to-end. App at `/data/tantor/app/{backend,frontend,venv,bin}`, data at `/data/tantor/data/{db,certs,repo,ansible_work}`, logs at `/data/tantor/log/{backend,nginx}`. systemd unit + nginx + `/etc/tantor/install.conf` all point to the custom paths.
- **Purge** (`--purge`) confirmed atomic. Deployed 3 clusters (ports 9092/9192/9292), ran purge, zero residual systemd units, install dirs, data dirs, or Java processes.

---

## 1.4.3 — 2026-05-11

A focused fix release for the 20 items the customer flagged after v1.4.2 testing. **Stop-introducing-regressions** was the explicit goal — every prior fix from v1.4.0+ now has an automated assertion in `tests/regression/run_all.sh`.

### Critical — state sync (regression caused by v1.3.5 per-cluster systemd units)

- **#3 / #4 / #5 / #10**: Cluster no longer flips to "stopped"/"error" on Refresh. Root cause: `cluster_manager.py` and `monitoring._get_kafka_metrics` were probing `kafka.service` (the legacy unit name) instead of the per-cluster unit (`kafka-<slug>-<id>.service`). Refresh → `systemctl is-active kafka.service` → "inactive" → `svc.status = stopped` → cluster.state flipped. Now every probe resolves via `cluster_paths.unit_name(cluster)` and `get_cluster_status` doesn't mutate `cluster.state` (refresh is now read-only as it should be).
- **#7**: Monitoring tab no longer shows Kafka inactive for a running broker. Same root cause as #3 — `_get_kafka_metrics` now takes the cluster + service so it probes the right unit + install dir + listener port.

### Critical — regression I caused in 1.4.1, reverted

- **#19**: External cluster Config edit works again. v1.4.1 added a `hasattr(admin, "incremental_alter_configs")` refusal that was returning False for the customer's `kafka-python` build, blocking every config edit on every external cluster. Reverted to a try/except pattern — attempt incremental, fall back to read-then-merge `alter_configs` only if incremental raises `NotImplementedError` / `AttributeError`. Loud warning on fallback so it's visible.

### Other critical fixes

- **#6**: `InvalidReceiveException: message size 369296129` no longer spams the log. 369296129 = 0x16030101 = first 4 bytes of a TLS 1.x ClientHello — i.e. an SSL client connecting to a PLAINTEXT listener. Raised `socket.request.max.bytes` from 100MB → 200MB so the error doesn't fire on every reconnect attempt while still rejecting truly malformed traffic.

### Lifecycle — purge / port flicker / Java pre-flight

- **#14 / #15**: `--purge` now enumerates every `kafka-*.service`, stops + disables them, pkills `/opt/kafka-` JVMs, removes every `/etc/systemd/system/kafka-*.service`, `/opt/kafka-*`, `/var/lib/kafka-*`. Atomic — no accumulation of 15+ stale dirs across reinstalls.
- **#16**: Port-conflict messages now name the holding systemd unit. Previously: `port 9093 in use by java pid=32333`. Now: `port 9093 in use by java pid=32333 (kafka-prod-1ac9bbbe.service)`. Resolved via `systemctl status --pid` lookup after `ss -tnlp`.
- **#17**: Pre-flight runs `java -version` + package-manager probe on every target host before ansible starts. Missing Java surfaces in <10s with a per-host hint (`apt install openjdk-17-jre-headless` / `dnf install java-17-openjdk-headless`) instead of failing mid-deploy.

### UX

- **#2**: "service not found" warning on first deploy silenced. Ansible playbook now detects existing kafka unit with `systemctl list-unit-files` before calling stop.
- **#8**: Consume tab no longer shows consumer log lines as message values. kafka-console-consumer stderr redirected to `/dev/null` + a parser blacklist for log4j2-shaped lines that occasionally leak into stdout.
- **#9**: Topics page doesn't flash "loading" every 10s. Background poll is now silent (no spinner, no list wipe on transient error); user-triggered refetches still show the spinner. Single `fetchTopics` ref used by the interval so search-keystroke doesn't re-schedule.
- **#11**: Clusters list has a manual Refresh button with a spinner that stays visible ≥400ms.
- **#12**: External Clusters Test / List Topics buttons have per-row loading state and surface a success/failure toast.

### External cluster + auth

- **#20**: Cluster-linking error message explains exactly why broker resolution failed. External cluster with malformed `bootstrap_servers` (missing port, unbracketed IPv6) now produces: "Source cluster (foo): bootstrap_servers='kafka.example.com' rejected — every entry must look like 'host:port'." Managed cluster with no Service rows: "Source cluster (foo) has no broker services registered."
- **#21**: External cluster Grafana dashboards no longer show "No data" when scrape is healthy. Prometheus scrape config now includes `static_configs.labels.cluster` + `external_labels.cluster` so Grafana panels filter correctly.
- **#22**: Admin role change kills in-flight JWTs immediately. `users.token_version` column + `tv` claim in JWT; dep gate rejects tokens whose `tv != user.token_version`. Promoting / demoting / deactivating a user forces them to re-login.

### Tooling

- `tests/regression/run_all.sh` — automated regression battery covering every prior 1.4.x fix. Runs against any Tantor URL with admin credentials. ~20 assertions; returns 0 if all pass. **Used before every release going forward.**

---

## 1.4.2 — 2026-05-09

Port-conflict pre-flight + Quick Deploy auto-pick.

- **`POST /api/clusters/preflight-ports`**: Wizard "Check ports" button hits this. SSH to each host, returns conflicts with the holding process. Empty conflicts list = safe to deploy.
- **Cluster create / redeploy**: same pre-flight runs before ansible starts. Fails fast with the port + process name on conflict, instead of letting ansible silently fail mid-deploy.
- **Quick Deploy auto-pick**: scans existing clusters' config_json, walks `9092 → 9192 → 9292…` until it finds a free `{listener, controller, ssl_listener}` set. Two quick-deploys on the same host produce cluster-1 on 9092/9093 and cluster-2 on 9192/9193 — both run concurrently.
- Same pre-flight applied to Schema Registry port (8085 default).

---

## 1.4.1 — 2026-05-08

Hardening pass on the 1.4.0 fixes + RBAC / SSL / mTLS verification.

- **Schema Registry deploy hardening**: Apicurio download retries 3× with 180s timeout each; sanity-checks the downloaded size; re-deploy stops the old SR before re-running the playbook (no port 8085 fight); systemd `TimeoutStartSec=300` so Quarkus cold-start on small VMs doesn't trip the 90s default.
- **Cluster linking hardening**: `bootstrap_servers` shape validation (port present, IPv6 brackets); MM2 properties now emit `source.security.protocol` + SASL JAAS + SSL endpoint-id-algorithm for external SASL_SSL clusters.
- **External `alter_broker_config`** added strict `hasattr` refusal (REVERTED in 1.4.3 — see #19 above; this turned out to be the customer-blocker).
- **RBAC**: admin reads + writes pass; monitor reads pass, all writes 403. Verified on `/auth/users`, `/quick-deploy`, `/clusters/*` write endpoints.
- **SSL**: `ssl_enabled=true` → broker binds `:9096`, `openssl s_client` completes TLS 1.x handshake. CA chain verifies through Tantor-minted CA (`CN=kafka-broker-1` signed by `CN=Tantor CA · cluster-1`).
- **mTLS**: `mtls_required=true` → `ssl.client.auth=required` in `server.properties`.

---

## 1.4.0 — 2026-05-06

The big feedback-round release. 16 customer items.

### Regressions caught + fixed

- **Rolling Restart per-cluster unit** — 1.3.5 made systemd unit names per-cluster (`kafka-prod-XYZ.service`) but the Restart endpoint still hardcoded `kafka.service`.
- **Audit log captures the actor** — every `_audit` call now passes the calling user; `audit_logs.actor_user_id` + `actor_username` columns added.
- **Log4j errors during Kafka deploy** — Tantor ships its own `log4j2.yaml` + `tools-log4j2.yaml` (mirrored into the per-cluster install dir); `KAFKA_LOG4J_OPTS` pointed at them for both broker startup and `kafka-storage.sh format`.

### External-cluster parity

- **Cluster Linking accepts external Kafka** as source/dest. `_get_broker_addresses` falls back to `cluster.bootstrap_servers` when there are no Service rows. Frontend dropdown also includes `state=connected` + `kind=external`.
- **Cluster Linking page polls** links + clusters every 10s.
- **Federation Overview** shows real broker counts for external clusters via cached kafka-python probe.
- **Per-cluster Monitoring** synthesizes broker rows for external clusters from `describe_cluster()`; SSH-based metrics still require registered hosts.

### New features

- **Schema Registry per-cluster** deploy from cluster detail tab. Removed global sidebar entry (schemas are inherently tied to one cluster's bootstrap).
- **Certificate upload** in cluster Security → Certificates tab. List CA + broker keystores; upload PEM cert (+ optional key) to override Tantor's auto-generated CA.
- **Add NEW config keys via UI** — Config tab "+ Add Config" button (backend already supported appending unknown keys; gap was UI-only).
- **Bulk broker config** ("one for all") — same modal has "Apply to all brokers" checkbox. Backend: `POST /api/broker-config/clusters/{id}/bulk-config`.
- **LDAP source column** — `users.auth_source` + `ldap_dn` columns; Users page shows badge per row; password change UI hidden for LDAP-synced users (backend also rejects with explicit error).
- **HTTPS for Tantor UI** — `--tls` install flag (self-signed cert by default, accepts `--tls-cert` / `--tls-key`). nginx serves `:443` + `:80` permanently redirects.
- **Quick Deploy** — green button on Clusters page → `POST /api/clusters/quick-deploy` creates + deploys on every registered host with sane defaults.

---

## 1.3.5 — 2026-05-04

- **`--install-dir <BASE>`** — install everything under one customer-supplied directory; persisted to `/etc/tantor/install.conf` so `--reinstall` doesn't drop a parallel copy at `/opt/tantor`. SELinux contexts (`var_log_t`, `var_lib_t`, `bin_t`) applied automatically.
- **Per-cluster Kafka paths** — every managed cluster gets `/opt/kafka-<slug>-<id>`, `/var/lib/kafka-<slug>-<id>/data`, `kafka-<slug>-<id>.service`, and unique JMX port. Two managed clusters on the same broker host now coexist cleanly.
- **Service Logs tab populated** — `log_manager` now uses the cluster's actual systemd unit + `sudo -n journalctl` so the SSH user can read the kafka unit's logs.

---

## 1.3.4 — 2026-05-03

- Broker config save (500 → 200) — read uses `sudo -n cat`; write uses `sudo install -o kafka -g kafka -m 640`.
- Removed a nested `import ConfigAuditLog` that was breaking the managed-path with `UnboundLocalError`.
- External Kafka version detection improved — tries `ApiVersionRequest_v3` first, falls back to `≥ X.Y.Z` floor labelling.

---

## 1.3.3 — 2026-05-02

9 fixes for items in the v1.2.0 feedback sheet:

- **Generator didn't stop after `throw()`** — `SSHManager.connect`'s pool-reuse `try/except` was swallowing caller exceptions and yielding twice. Restructured.
- **Produce returned 500 even on success** — schema mismatch between external `produce_message` return shape and `ProduceResponse`.
- **Consume 500 on null Kafka values** — `ConsumedMessage.value` was required `str`. Made Optional.
- **Consumer Groups parser shows error logs** — `kafka-consumer-groups.sh --list` interleaves WARN/INFO into stdout. Added regex filter + prefix blocklist.
- **External Overview tab empty** — synthetic broker rows from `describe_cluster` now populate it.
- **SCRAM UI for external** — Create User disabled with info banner.
- **Capacity forecast never works** — `node_exporter` was never installed (Prometheus scraped `:9100` but nothing listened). Now installed alongside JMX.
- **Dashboard count excludes external** — counts both `running` (managed) and `connected` (external).

---

## 1.3.2 — 2026-05-01

- `--purge` truly cleans — dpkg-purges Grafana, removes `/home/tantor`, install log, restores stock `nginx.conf`. Fresh install validates the dist isn't empty.
- Kafka deploy timeout shows WHY — wait raised 60 → 180 s, fail-fast on `systemctl is-failed`, on timeout attaches `journalctl -u kafka -n 80`.
- Monitoring sidebar restored — cross-cluster CPU/mem overview.
- Topic create reflects in UI — optimistic insert + immediate refetch + 1.5s retry.
- UI freshness — Clusters list + Cluster Detail header poll every 15s while visible.
- Federation page is fast — parallel topic-count probes (8 workers) with a 30s cache.
- External Kafka version detected at add-time and on test-connection.
- Config save reflects in UI — optimistic update + 800ms refetch + 1.5s reconcile.

---

## 1.2.0 — 2026-04-25

Major feature drop from the customer's first feedback table:

- Intelligent Consumer Alerting (`ConsumerStalled`, `ConsumerFailed`)
- Topic-wise Performance Graphs (Grafana + topic dropdown)
- CDC Quickstart Wizard (Debezium MySQL/Postgres/Mongo/SQL Server templates)
- Capacity Trend Forecasting (linear projection + ETA to 85% full)
- Data Federation (cross-cluster overview + topic search)

---

## 1.1.x — 2026-04-15

- Install log + per-step timeouts + heartbeat + error trap
- Deploy logs in the UI
- External clusters get full management surface (monitoring, alerting, broker config edit)
- Monitoring + alerting auto-deploy on cluster create
- `--purge` wipes Kafka data dirs; Rocky/RHEL 9 Python 3.10+; Fernet across `--reinstall`

---

## 1.0.0 — 2026-03-15

Initial release.
