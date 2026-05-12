# Roadmap

What's NOT in v1.4 and what's planned for v1.5 / beyond. Items here are the deferred customer asks plus internal-driven improvements we know we want.

If you're picking up your first PR and want a "good first issue," start at the bottom of each section — earlier items typically need more context.

---

## v1.5 — deferred from v1.4 customer feedback

### 1. Per-node port configuration (item #13)

**Today**: Tantor applies a single `listener_port` / `controller_port` across all brokers in a cluster. Works for homogeneous environments.

**Gap**: customer has hosts where different ports are available per machine (e.g. Node1 uses 9092/9093, Node2 has those reserved so needs 9094/9095). Kafka itself supports this via per-broker `advertised.listeners`.

**Scope** (~4-6h):
- Frontend: `ClusterWizard` step 3 → per-row port overrides under each assigned host
- Backend: `Service` model gets `port_overrides_json` column
- `config_generator.py` reads the override per service when rendering `server.properties`
- Wizard "Check ports" pre-flight validates per-host

**Watch out for**: `quorum.voters` must include the per-controller port — gets rendered cluster-wide today. Update `_build_quorum_voters` to take the overrides.

---

### 2. Guided dependency remediation flow (item #18)

**Today**: v1.4.3 pre-flight detects missing Java + prints a hint, but ansible still tries to install via apt/dnf. If that fails (corp proxy, RHEL not subscribed), deploy fails mid-way.

**Gap**: customer wants Tantor to surface missing deps BEFORE the deploy starts, list them per host, explain what each is, what Tantor will install, and only proceed after explicit user confirmation. Specifically: NEVER auto-install in production environments without an "I consent" click.

**Scope** (~4-6h):
- New endpoint `POST /api/clusters/{id}/preflight/dependencies` — SSH each host, return JSON per-host with `{java: bool, openssl: bool, systemctl: bool, hostname: str, os: str}`
- Frontend: a modal between "click Deploy" and the actual ansible run, showing the per-host result + an "Install X on these N hosts" button OR "I'll install manually, retry" button
- Persist consent: write a `dependency_install_consent` row so re-runs don't re-prompt unnecessarily
- Tutorial / docs for production customers: "to use this safely, register a Tantor-only sudo rule that allows JUST `dnf install java-17-openjdk-headless`"

**Watch out for**: false negatives — `which java` could find a JRE without javac. Probe with `java -version 2>&1` and require Java 17+.

---

### 3. Adopt-existing-cluster (item #23)

**Today**: only two paths exist: deploy a new cluster (risky on a host that already has Kafka) or import as external (read-mostly, no per-cluster monitoring).

**Gap**: customer wants Tantor to SSH into a registered host, auto-discover an existing Kafka install (process scan + KRaft cluster id + `server.properties` parse), and offer to onboard it under management without downtime or data changes.

**Scope** (~8-12h — real feature, not a bug):
- New endpoint `POST /api/hosts/{id}/discover` — SSH probe: scan `ps`, parse `server.properties`, infer mode (KRaft / ZK), version, listeners, log_dirs
- "Adoption" wizard: review discovered info → confirm → Tantor adds a `Service` row pointing at the existing systemd unit (NOT a new install) + leaves `kafka_install_dir` pointed at whatever path the existing install uses
- Adoption mode skips the ansible deploy entirely — just records the cluster + lets day-2 ops run against it
- Edge case: adopted cluster's systemd unit name might be plain `kafka.service` (legacy) instead of `kafka-<slug>-<id>.service`. Store actual unit name in `cluster.kafka_unit_name` regardless of slug logic.

**Watch out for**: if the customer has multiple Kafka installs on one host, we need to disambiguate by port. Adoption probe should enumerate every listening `:9092` / `:9192` / etc. and let the operator pick.

---

## Internal improvements (not customer-driven)

### 4. Switch broker config alter to kafka-python everywhere

**Today**: only `list_topics` uses kafka-python (v1.4.4). Most write ops still shell out to `kafka-topics.sh` / `kafka-configs.sh` over SSH.

**Gap**: each SSH+CLI call is 3-4s of JVM cold start. Multiplied by every config change, ACL create, etc. — the UI feels slow.

**Scope** (~6-8h):
- `create_topic`, `delete_topic`, `alter_topic_config` → kafka-python `AdminClient.create_topics` / `delete_topics` / `incremental_alter_configs`
- ACL create/delete → `AdminClient.create_acls` / `delete_acls`
- Keep SSH+CLI as fallback (mirrors what `list_topics` already does)
- Removes the dependency on `KAFKA_INSTALL_DIR/bin/*.sh` being available on the broker host's PATH for CLI tools

**Watch out for**: SCRAM user create still has no kafka-python equivalent (`alter_user_scram_credentials` was added in newer versions but kafka-python-ng 2.2.x doesn't expose it). Keep SCRAM on the SSH path.

---

### 5. Multi-broker monitoring drilldown

**Today**: Monitoring tab shows per-broker rows from the SSH probe. Grafana dashboards filter by cluster but don't break down per broker.

**Gap**: when a customer's cluster is misbehaving, the question is usually "which broker?" not "is the cluster up?". Need per-broker panels: bytes-in/out per broker, ISR per broker, GC time per broker.

**Scope** (~6-8h):
- Update `monitoring_deployer.py` Grafana provisioning to add a "per-broker" dashboard alongside the cluster dashboard
- JMX exporter config already labels samples with `instance` (host:port); add a Grafana variable `$broker` and templatize the panels
- Surface a "Drill into broker N" link on the Cluster Detail → Monitoring tab

---

### 6. RBAC for cluster ownership

**Today**: any Tantor admin can deploy on any host, edit any cluster, delete any cluster. Two-tier (admin/monitor) for everyone.

**Gap**: multi-tenant operators want "Team A owns cluster prod-1, Team B owns cluster prod-2; an admin in Team A can manage prod-1 but only sees prod-2 read-only".

**Scope** (~10-15h):
- New `cluster_acls` table: (cluster_id, user_id_or_group, role)
- Dep middleware: every cluster-scoped endpoint resolves `current_user`'s access level for this cluster
- Frontend: a per-cluster Settings tab where the cluster admin can add other users with admin / monitor / no access
- UI shows only clusters the user has access to (filter on `/api/clusters` response)

**Watch out for**: doesn't compose well with LDAP unless we also surface group-based ACL rules. Probably worth gating behind a feature flag until there's a real demand.

---

### 7. Compaction + retention policy templates

**Today**: every topic config is set ad-hoc by the operator typing keys into the Add Config modal.

**Gap**: operators want "apply our standard retention policy to this topic" without remembering every key.

**Scope** (~3-4h):
- Templates stored in a new `topic_config_templates` table (operator-defined)
- "Apply Template" dropdown on the topic config UI
- Built-in starter templates: "Audit log", "Stream processing", "Event store", "Click stream"

---

### 8. Streaming deploy log via WebSocket (not just polling)

**Today**: Deploy progress polls `/api/clusters/{id}/deploy/{task}` every 2-5s. Works but feels laggy on long deploys.

**Gap**: real-time ansible task names + their stdout would feel much more responsive.

**Scope** (~3-4h):
- New WebSocket endpoint `/api/ws/deploy/{task_id}` — pushes each new log line as it's written
- Frontend `DeployProgress` switches from polling to WS subscription
- Reuse the existing `_append_log(task_id, line)` write site as the broadcast trigger

---

## Stretch — bigger bets

| Idea | Effort | Why it matters |
|---|---|---|
| **Kubernetes operator** for Tantor-deployed Kafka | 4-6 weeks | Customers running on K8s today have to deploy Tantor in a VM next to the cluster. An operator lets them keep everything in K8s. |
| **CRUD API** for ACLs at the topic level (UI: per-topic ACL editor) | 2 weeks | Today ACLs are a flat list; managing 500 topics × 20 principals is unworkable. |
| **Multi-region cluster linking** | 2-3 weeks | MirrorMaker 2 supports it but our UI assumes a flat list. |
| **Tantor-on-Tantor** (HA for the Tantor backend itself) | 4-6 weeks | Today Tantor is a single-machine SPOF. A primary-replica setup with state replication unlocks production-critical use cases. |

---

## How to propose adding something to this list

PR against this file. Each entry should have:
- **What today** (current behavior)
- **What gap** (specific customer or developer pain)
- **Scope** (rough hour estimate + key files / endpoints affected)
- **Watch out for** (gotchas you anticipate)

If you can't fill in all three, the item probably isn't well-enough scoped to start — sharpen the ask first.
