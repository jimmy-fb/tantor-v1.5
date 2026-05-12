# Tantor — Self-Hosted Kafka Cluster Manager

Tantor is a single-binary, browser-driven control plane for Apache Kafka. One installer on a Linux VM gives you cluster deployment, topic + consumer management, security (SCRAM, ACLs, mTLS), schema registry, monitoring, cluster linking, and an audit log — all running on infrastructure you control. No SaaS, no agent rollouts, no vendor lock-in.

**Latest release:** **v1.4.4** · [installer .bin](https://github.com/jimmy-fb/tantor-installer/raw/main/tantor-installer-1.4.4.bin) · supports Kafka 4.1 (KRaft) and 3.x (KRaft or ZooKeeper)

---

## What you get

- **Cluster deploy in 5 clicks** — pick hosts, roles, ports, click deploy. Tantor ships per-cluster Kafka installs (multi-cluster on the same host coexist cleanly), SSH-driven Ansible playbook, live deploy log streaming in the UI.
- **Day-2 ops** — topics with auto-refresh, produce/consume from the browser, consumer groups + lag, rolling restart, partition rebalance, broker config editor with audit log + rollback, capacity forecast (linear projection on Prometheus disk-used).
- **Security** — SCRAM users + ACLs, mTLS toggle (Tantor mints CA + broker keystores), operator-supplied CA upload, RBAC (admin/monitor) with JWT + token_version session invalidation, Active Directory / LDAP integration with group→role mapping.
- **Schema Registry** — per-cluster Apicurio Registry, Confluent ccompat API, browser-based subject browsing + registration.
- **External clusters** — bring an existing Kafka under management via bootstrap servers. Tantor speaks kafka-python over TCP for topics / consumer groups / ACLs / config edits. Federation page + cluster-linking (MirrorMaker 2) support managed→external and external→external.
- **Monitoring** — auto-deployed Prometheus + Alertmanager + Grafana + JMX exporter on cluster create. Per-cluster system + Kafka metrics, capacity forecast, configurable alert rules.
- **HTTPS for the UI** — `--tls` flag mints a self-signed cert at install time; `--tls-cert` / `--tls-key` accept your own PEMs.

---

## Quickstart — install on a Linux VM

```bash
# Fetch the latest installer
curl -fL -o tantor.bin \
  https://github.com/jimmy-fb/tantor-installer/raw/main/tantor-installer-1.4.4.bin
chmod +x tantor.bin

# Install with HTTPS (default layout: /opt/tantor, /var/lib/tantor, /var/log/tantor)
sudo ./tantor.bin --force --tls

# Custom install layout (collapses app/data/log under one base dir)
sudo ./tantor.bin --force --tls --install-dir /data/tantor

# Open in browser
# https://<server-ip>  →  admin / admin  (change immediately)
```

| Flag | Effect |
|---|---|
| `--force` | Skip the confirmation prompt |
| `--tls` | Generate a self-signed cert and serve nginx on `:443` (with `:80` redirect) |
| `--tls-cert <path>` `--tls-key <path>` | Bring your own PEM cert + key |
| `--install-dir <BASE>` | Use `BASE/{app,data,log}` instead of the FHS default |
| `--reinstall` | Uninstall preserving data, then fresh install |
| `--purge` | Uninstall + wipe data + DB + repos + every per-cluster Kafka unit |

**Supported OS:** Ubuntu 22.04/24.04, Debian 11/12, RHEL 8/9, Rocky 8/9, AlmaLinux 8/9, Oracle Linux 8/9, Amazon Linux 2023.
**Hardware minimum:** 4 vCPU, 8 GB RAM, 30 GB disk (t3.xlarge equivalent).

---

## Repository layout

```
tantor/
├── backend/                    FastAPI + SQLAlchemy backend
│   ├── app/
│   │   ├── api/                HTTP routes (clusters, topics, security, monitoring…)
│   │   ├── models/             SQLAlchemy models (User, Cluster, Service, AuditLog…)
│   │   ├── schemas/            Pydantic request/response schemas
│   │   ├── services/           Business logic (deployer, ssh_manager, kafka_admin…)
│   │   └── templates/          Jinja2 templates (Ansible playbooks, systemd units, server.properties)
│   └── requirements.txt
├── frontend/                   React + Vite + TypeScript UI
│   ├── src/
│   │   ├── pages/              Top-level routes (Clusters, ClusterDetail, Activity…)
│   │   ├── components/         Reusable + cluster-scoped components
│   │   └── lib/                API client, auth helpers
│   └── package.json
├── tests/regression/           Automated regression battery (bash + curl + jq)
├── docs/                       Architecture, security, build, testing, roadmap
├── install.sh                  The installer entrypoint that ships inside the .bin
├── build-installer.sh          Wraps the source into a self-extracting .bin
└── README.md                   (you are here)
```

Full architecture overview: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Develop locally

```bash
# Backend
cd backend
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev  # http://localhost:5173

# Or run everything in Docker (for the e2e smoke test)
docker compose up
```

Full build instructions: [docs/BUILD.md](docs/BUILD.md).

---

## Build the installer .bin from source

```bash
./build-installer.sh
# → tantor-installer-1.4.4.bin in the repo root
```

That .bin is a self-extracting tarball that runs `install.sh` against the target host's package manager.

---

## Run the regression battery

Every prior fix has an automated assertion. Run it against any Tantor URL with admin credentials:

```bash
TANTOR_URL=https://<your-tantor-host> \
TANTOR_ADMIN=admin \
TANTOR_PASS=admin \
  bash tests/regression/run_all.sh
```

Returns 0 if all 20+ checks pass. The recipe used before every release: full pass on RHEL 9.7 and Ubuntu 22.04. Details: [docs/TESTING.md](docs/TESTING.md).

---

## Documentation

| Doc | What's in it |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Component overview, deploy flow, per-cluster systemd unit layout, SSH manager pool, JWT auth, RBAC, audit log |
| [CHANGELOG.md](docs/CHANGELOG.md) | Every customer item + version it landed in (v1.0 → v1.4.4) |
| [BUILD.md](docs/BUILD.md) | How to build the .bin from source; dev hot-patch recipe |
| [TESTING.md](docs/TESTING.md) | The regression battery, multi-OS AWS recipe, what each assertion covers |
| [SECURITY.md](docs/SECURITY.md) | RBAC matrix, JWT lifecycle + token_version, SSL/mTLS broker config, CA management, LDAP, Fernet secrets |
| [ROADMAP.md](docs/ROADMAP.md) | v1.5 features in flight (per-node ports, dependency remediation, adopt-existing-cluster) |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Branch + PR flow, where each piece lives, how to add a regression assertion |

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
