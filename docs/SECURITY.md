# Security

How Tantor handles authentication, authorization, secrets, TLS, and Kafka cluster security. Audience: ops + new devs picking up a security-related ticket.

---

## TL;DR

- **Tantor UI**: JWT auth + RBAC (admin/monitor). HTTPS via self-signed cert by default (`--tls`); accepts BYO cert (`--tls-cert` / `--tls-key`).
- **Kafka cluster security**: optional SSL listener + mTLS. Tantor mints a per-cluster CA, signs broker certs, ships keystores via SSH at deploy. Operator can replace the CA via UI upload.
- **LDAP**: Active Directory / OpenLDAP bind + group → role mapping. LDAP-synced users can't have local passwords.
- **Secrets at rest**: Fernet (AES-128-CBC + HMAC-SHA256) on the encrypted columns (SSH credentials, LDAP bind password, SCRAM passwords, TLS keystore passwords).
- **Audit log**: every security-relevant action (user create/delete, password rotation, ACL create/delete, CA upload) is logged with actor.

---

## 1. Tantor UI authentication

### Login flow

1. `POST /api/auth/login {username, password}` → backend tries LDAP first (if configured), falls back to local users.
2. On success, response: `{access_token, refresh_token, role}`. Both JWTs.
3. Frontend stores tokens in `localStorage` and adds `Authorization: Bearer <access>` to every request.
4. Access token expires (default 60 min); refresh token (default 7 days) gets a new access token via `POST /api/auth/refresh`.

### JWT structure

```json
{
  "sub": "<user-id-uuid>",
  "role": "admin",
  "type": "access",          // or "refresh"
  "tv": 0,                   // token_version, v1.4.3+
  "exp": 1730000000
}
```

Signed with HS256 + `settings.JWT_SECRET_KEY` (32+ bytes, generated at first install into `<TANTOR_DATA>/secrets/jwt.key`).

### token_version session invalidation (v1.4.3 #22)

Every user row has `token_version` (defaults to 0). The dep gate (`backend/app/api/deps.py::get_current_user`) verifies `payload["tv"] == user.token_version` and returns 401 if not.

Bumping the version invalidates every JWT for that user instantly:
- Role change (`PUT /api/auth/users/{id} {role: "admin"}`) bumps it.
- Deactivation (`PUT /api/auth/users/{id} {is_active: false}`) bumps it.
- Login + refresh always issue new tokens carrying the CURRENT `token_version`, so legitimate sessions keep working.

This solves the "I demoted Bob but his tab is still admin" problem from item #22.

---

## 2. RBAC (admin vs monitor)

Two roles today:

| Role | Read endpoints | Write endpoints |
|---|---|---|
| **admin** | All | All |
| **monitor** | All | None (403) |

Implemented via two FastAPI deps in `backend/app/api/deps.py`:
- `require_monitor_or_above` — any logged-in user passes
- `require_admin` — only `role == "admin"` passes; else 403

Every router endpoint chooses one of these. Pattern in `backend/app/api/clusters.py`:

```python
@router.get("", response_model=list[ClusterResponse])
def list_clusters(db = Depends(get_db), _: User = Depends(require_monitor_or_above)):
    ...

@router.post("/quick-deploy", ...)
def quick_deploy(req, db, current_user: User = Depends(require_admin)):
    ...
```

Verified by the regression battery: monitor reads return 200, monitor writes return 403 on 4 key endpoints.

---

## 3. LDAP / Active Directory

Configured via Settings → LDAP page (`/api/ldap` endpoints + `LdapConfig` model). One config row at a time.

### Bind flow

1. Tantor binds to the LDAP server using stored bind DN + bind password (Fernet-encrypted in `ldap_configs.encrypted_bind_password`).
2. Searches for the user under `base_dn` matching the configured `user_filter` (typically `(sAMAccountName=%s)` for AD, `(uid=%s)` for OpenLDAP).
3. If found, bind as the user with the supplied password to verify.
4. Read the user's memberOf groups.
5. Map groups → Tantor role using the configured `admin_group_dn` and `monitor_group_dn` (DN equality, case-insensitive).
6. Either create a fresh local `users` row with `auth_source="ldap"` + `ldap_dn=<the user's DN>`, or update an existing row's role + last_login.

### TLS / LDAPS

- `tls_validate_cert` (default true) — verify the LDAP server's cert chain against the system trust store + the optional `tls_ca_cert` PEM (uploaded via the LDAP settings UI).
- Use `ldaps://` for SSL or `ldap://` + StartTLS (toggle on the settings page).

### LDAP-synced users vs local users

- Local password change blocked server-side for `auth_source="ldap"` rows (`PUT /api/auth/users/{id}` rejects with 400).
- Frontend hides the password change Key icon for LDAP rows (greyed-out tooltip explains why).
- A user can be DEMOTED from admin to monitor either via LDAP group change (next login re-evaluates) or admin override (UI Role toggle, bumps token_version).

---

## 4. Kafka cluster security

Per-cluster opt-in via `Cluster Detail → Security → Enable TLS / mTLS`.

### TLS (SSL listener)

When `cluster.ssl_enabled=true`:
- A second listener `SSL://0.0.0.0:9096` is added to `server.properties` alongside the default PLAINTEXT.
- `listener.security.protocol.map` gets `SSL:SSL` appended.
- Broker keystore is `/etc/kafka/ssl/broker.p12` (PKCS#12, mode 640 owned by `kafka:kafka`).
- Truststore is `/etc/kafka/ssl/truststore.pem` (PEM, the cluster CA cert).
- `ssl.keystore.password` is the Fernet-decrypted TLS password Tantor generated at cluster create (`cluster.encrypted_tls_password`).

### mTLS

When `cluster.mtls_required=true`:
- Adds `ssl.client.auth=required` so clients without a valid cert get rejected at TLS handshake.
- `ssl.principal.mapping.rules` defaults to `RULE:^.*$/ANONYMOUS/,DEFAULT` so all mTLS-authed clients land on the same Kafka principal Tantor's `super.users` line allows. Tighten this rule once you start enforcing per-principal ACLs.

### CA management

`backend/app/services/cert_manager.py`:

| Function | Purpose |
|---|---|
| `ensure_cluster_ca(cluster)` | Generate a 4096-bit RSA CA cert (CN=`Tantor CA · <cluster-name>`) on first use. Stored at `/var/lib/tantor/certs/<cluster-id>/{ca.crt,ca.key}`. |
| `upload_cluster_ca(cluster, cert_pem, key_pem)` | Replace with operator-supplied PEM. If only cert provided, Tantor still signs broker certs with the original auto-gen CA. |
| `list_cluster_certificates(cluster)` | Returns CA subject + fingerprint + not_after + uploaded flag for the Security → Certificates tab. |
| `materialize_broker_keystores(cluster, db, services)` | For each broker service: issue a cert signed by the CA, package into a PKCS#12 with the cluster TLS password, write to the workspace so Ansible can copy it to `/etc/kafka/ssl/broker.p12`. |

The `ca.key` is filesystem-mode 600 root:root. Anyone reading it can mint broker certs that the cluster trusts.

---

## 5. Secrets at rest

Tantor encrypts these columns with Fernet (`cryptography.fernet.Fernet`):

| Column | What it holds |
|---|---|
| `hosts.encrypted_credential` | SSH password OR private key for the user that ansible runs as |
| `ldap_configs.encrypted_bind_password` | LDAP bind DN's password |
| `kafka_users.encrypted_password` | SCRAM password (we keep a local copy so the UI can display "rotate" without re-prompting the operator) |
| `clusters.encrypted_tls_password` | Keystore password for the SSL listener |
| `clusters.encrypted_connection_secrets` | External cluster's SASL username/password + SSL PEMs |

The Fernet key is generated on first install into `<TANTOR_DATA>/secrets/fernet.key` (mode 600, owned by `tantor`). Code that needs to decrypt imports `from app.services.crypto import decrypt`.

**Key rotation**: not automated yet. To rotate manually, write a script that decrypts every encrypted column under the old key, re-encrypts under the new key, then swaps `fernet.key`. Out of scope for v1.4.

---

## 6. Audit log (v1.4.3 #11)

`audit_logs` table — one row per security-relevant action:

| Column | Example |
|---|---|
| `cluster_id` | cluster the action applied to |
| `action` | `user_created`, `user_password_rotated`, `acl_created`, `acl_deleted`, `ca_uploaded` |
| `resource_type` | `user`, `acl`, `certificate` |
| `resource_name` | the username, the ACL principal, `cluster_ca` |
| `actor_user_id` + `actor_username` | **WHO did it** — added in v1.4.3 |
| `details` | JSON blob with action-specific data |
| `created_at` | UTC timestamp |

Surfaces in two places:
- Cluster Detail → Security → Audit Log (cluster-scoped, paginated)
- Sidebar → Activity (combined with `config_audit_log` for broker config edits)

Every kafka_admin write site calls `KafkaAdmin._audit(db, cluster_id, action, resource_type, resource_name, details, actor=current_user)`. The `actor=current_user` part is the v1.4.3 fix — every API endpoint that takes a write dep must thread `current_user` through.

---

## 7. Common mistakes when adding security-relevant features

1. **Forgetting `actor=current_user`** in `_audit` calls. The kafka_admin method signature accepts `actor` but every code path needs to pass it. Grep `_audit(` for missing actor args before merging.
2. **Hardcoding `kafka.service`** in a new service that probes the broker. ALWAYS go through `cluster_paths.unit_name(cluster)`. The v1.4.2 state-sync regression came from this.
3. **Returning `User` model directly in API responses.** Use the `UserResponse` Pydantic schema — it explicitly omits `hashed_password` and `token_version`. A leak there has compounding consequences.
4. **Skipping the `is_active` check.** `get_current_user` already enforces it, but if you write a path that decodes the JWT and looks up the user without the dep, replicate the check.
5. **Trusting the `role` claim in the JWT.** It's there for convenience but the dep ALWAYS looks up `user.role` from the DB. Don't write code that trusts the claim alone — it would prevent token_version invalidation from working.
6. **Logging secrets.** Logger format strings sometimes pull in dict reprs that include passwords. When adding a `logger.info(...)` in a security path, prefix with `# DO NOT log secrets` and verify the message.

---

## 8. Threat model — what Tantor IS and IS NOT

### IS protected against

- Compromised end-user's browser → JWT alone is enough to impersonate them for up to 60 min. Forcing re-login on role change limits blast radius.
- Unencrypted at-rest secrets in the SQLite file (a SQLite copy alone is not enough; you also need the Fernet key on disk).
- Replay attacks against the Tantor UI — JWT `exp` claim is enforced.
- Anonymous Kafka clients hitting an mTLS-protected listener — Kafka rejects at TLS handshake.

### NOT protected against

- **Root on the Tantor host** owns everything. The Fernet key sits on disk. Treat the Tantor host as a Tier-0 admin asset.
- **Anyone with the Fernet key file** can decrypt all stored secrets offline.
- **SQL injection** — we use SQLAlchemy ORM bound parameters everywhere. If you write raw SQL, use `bindparam` or `text(...)` with named params. Grep `text(f"` for f-string SQL — that's the dangerous pattern.
- **CSRF** — JWT in Authorization header is by-design CSRF-immune (cookies are not used for auth). If we ever add cookie auth, we need CSRF tokens.
- **DoS** — no per-IP rate limit on `/api/auth/login` today. A motivated attacker can brute-force bcrypt at ~10 req/s. Consider adding `slowapi` rate limiting if exposed to the public internet.
- **Untrusted Kafka cluster operators** — anyone with admin role on Tantor can deploy a Kafka cluster on any registered host. There's no concept of "cluster owner" or per-cluster RBAC. v1.5 candidate.

If your customer's threat model needs the second-column items, raise it as a v1.5 ticket.
