# Testing

How we test Tantor. The big idea: **stop introducing regressions** by running a layered test pass before every release. This doc is the contract.

---

## The 3-layer test pass

| Layer | What it covers | When it runs | Where to find it |
|---|---|---|---|
| **L1 — Automated regression battery** | ~20 API-level assertions. Every prior 1.4.x customer fix has at least one. | Before every release, against a live Tantor URL | `tests/regression/run_all.sh` |
| **L2 — Multi-OS install pass** | L1 + verify the .bin installs cleanly on both major OS families | Before every release, against fresh AWS instances | `tests/regression/multi_os_pass.sh` (recipe below — not yet committed as one script; run by hand for now) |
| **L3 — Manual UI walkthrough** | Things automation can't easily verify — spinners, button states, the topic re-loading flicker | Before every release that touches the frontend | Checklist in this doc, run once on each OS |

**Release rule:** All three layers pass on RHEL 9.7 AND Ubuntu 22.04 before the .bin is published to `jimmy-fb/tantor-installer`. No "ship and we'll fix in the next one" releases — that's what got us into the v1.4.2 mess.

---

## L1 — Automated regression battery

```bash
TANTOR_URL=https://<your-tantor-host> \
TANTOR_ADMIN=admin \
TANTOR_PASS=admin \
  bash tests/regression/run_all.sh
```

Returns 0 if all assertions pass; non-zero with a failure summary otherwise.

### What's covered today

```
== Connectivity + auth ==
  admin login

== Prior fix regression — audit actor + TLS UI ==
  HTTP→HTTPS redirect or HTTPS direct

== Quick deploy + state sync (#3/#4/#5/#10) ==
  Quick Deploy #1 returns cluster_id
  Quick Deploy #1 listener=9092 (default)
  Quick Deploy #1 completed
  Refresh ×10 — cluster.state stays running

== 1.4.2 port auto-pick ==
  Quick Deploy #2 auto-picked listener=9192

== Cluster validation ==
  Validate 5/5 steps PASS

== RBAC matrix ==
  monitor user created + login
  monitor GET /clusters = 200
  monitor POST /auth/users = 403
  monitor POST /quick-deploy = 403

== Audit log actor (#11) ==
  audit row has actor=admin

== Config edit (#8 add, #5 bulk) ==
  Add config compression.type=lz4 (old=null)
  Bulk config 1/N brokers

== Rolling restart (#1 1.4.0 regression check) ==
  Rolling restart kicked off (status=running)

== Monitoring metrics report broker as running (#7) ==
  Monitoring kafka.status=active

== Federation overview ==
  Federation lists N clusters

== Port preflight (#16/1.4.2) ==
  Preflight flags occupied 9092
  Preflight leaves free 55555 alone
```

### Adding a new assertion

Every PR that fixes a customer item or adds a feature **must** add a new assertion. Pattern:

```bash
group "MY-FEATURE — description"
RESP=$(curl $CURL_OPTS -H "$(H)" -X POST -H 'Content-Type: application/json' \
  -d '{"foo":"bar"}' "$TANTOR/api/whatever")
RESULT=$(echo "$RESP" | jq -r '.expected_field // empty')
[ "$RESULT" = "expected_value" ] && pass "MY-FEATURE behaves correctly" \
  || fail "MY-FEATURE" "$RESP"
```

`pass` / `fail` / `group` helpers + the global `PASS_COUNT` / `FAIL_COUNT` counters are already at the top of the script.

### Pre-conditions

The battery creates a `regression_viewer` user and 2-3 clusters with names `cluster-1`, `cluster-2`, etc. Run it against a **fresh Tantor** for the first time; subsequent runs may have leftover state (clusters with `regression` in the env tag), which is harmless but means some auto-pick assertions need adjustment if you customize.

If you run it twice in a row, the 2nd run may report the listener=9192 (instead of 9092) for Quick Deploy #1 because the first run claimed 9092. That's expected behavior — the auto-pick works correctly, just isn't idempotent. To get a clean run: terminate the host and start fresh.

---

## L2 — Multi-OS install pass

Two AWS instances, two installs in parallel, then run L1 against each.

### Recipe

```bash
# Boot pair
RHEL_AMI=ami-0d5e8769671b48387        # RHEL 9.7
UBUNTU_AMI=ami-00403f401ee6a4b98       # Ubuntu 22.04
RHEL_ID=$(aws ec2 run-instances --region us-east-1 --image-id $RHEL_AMI \
    --instance-type t3.xlarge --key-name tantor-test-key \
    --security-group-ids <sg> ...)
UBUNTU_ID=$(aws ec2 run-instances --region us-east-1 --image-id $UBUNTU_AMI ...)

# Wait running + ssh up
until ssh -i key.pem ec2-user@$RHEL_IP echo ready 2>/dev/null; do sleep 5; done
until ssh -i key.pem ubuntu@$UBUNTU_IP echo ready 2>/dev/null; do sleep 5; done

# Upload + install on both
scp tantor-installer-1.4.4.bin ec2-user@$RHEL_IP:/tmp/
ssh ec2-user@$RHEL_IP 'sudo /tmp/tantor-installer-1.4.4.bin --force --tls'

scp tantor-installer-1.4.4.bin ubuntu@$UBUNTU_IP:/tmp/
ssh ubuntu@$UBUNTU_IP 'sudo -n /tmp/tantor-installer-1.4.4.bin --force --tls'

# Stage Kafka tarball (saves 2+ min of archive.apache.org download)
scp kafka_2.13-4.1.0.tgz ec2-user@$RHEL_IP:/tmp/
ssh ec2-user@$RHEL_IP '
  sudo mv /tmp/kafka_2.13-4.1.0.tgz /var/lib/tantor/repo/kafka/
  sudo chown tantor:tantor /var/lib/tantor/repo/kafka/kafka_2.13-4.1.0.tgz
'
# Same on Ubuntu

# Run L1 against each
TANTOR_URL=https://$RHEL_IP TANTOR_ADMIN=admin TANTOR_PASS=admin \
  bash tests/regression/run_all.sh

TANTOR_URL=https://$UBUNTU_IP TANTOR_ADMIN=admin TANTOR_PASS=admin \
  bash tests/regression/run_all.sh

# Tear down
aws ec2 terminate-instances --instance-ids $RHEL_ID $UBUNTU_ID
```

### Gotchas the recipe handles

- **Ubuntu sudo** wants `-n` (or tty); `sudo -n` works because cloud-init sets NOPASSWD for `ubuntu`.
- **Apache mirror** for Kafka tarball is slow (~3min for 130MB). Pre-stage to skip.
- **Test-script flakiness on re-runs**: re-running on the same host produces "expected 9092, got 9192" — that's auto-pick correctly bumping. Don't treat as a regression.

---

## L3 — Manual UI walkthrough

Open both URLs (RHEL + Ubuntu) and click through this list. Each item: pass/fail.

### Browser tab basics
- [ ] HTTPS UI loads, browser warns once on self-signed cert, dismissing the warning lands on /login
- [ ] `admin / admin` logs in successfully
- [ ] Dashboard shows stat cards (clusters, hosts, recent activity)

### Clusters list
- [ ] Quick Deploy button (green) starts a deploy. Spinner visible while deploying.
- [ ] Manual Refresh button (between Quick Deploy and New Cluster) shows spinner for ≥400ms
- [ ] Cluster row updates `state` from "deploying" → "running" without a full reload

### Cluster Detail tabs (deployed cluster)
- [ ] **Topics**: loads in 1-2s (kafka-python fast path). NO loading spinner re-flash every 10s. Search box filters live.
- [ ] **Produce**: send a message → success toast
- [ ] **Consume**: NO "Reconfiguration failed" or other log-line junk in message values
- [ ] **Groups**: consumer groups list loads
- [ ] **Validate**: 5/5 PASS
- [ ] **Config**: existing keys are editable in-place. "+ Add Config" opens modal. "Apply to all brokers" checkbox visible. Submit shows N/N brokers updated.
- [ ] **Security → Users**: SCRAM user create returns generated password
- [ ] **Security → ACLs**: ACL create surfaces in the list
- [ ] **Security → Audit Log**: every action has an Actor column populated
- [ ] **Security → Certificates**: shows current CA fingerprint + Upload form
- [ ] **Monitoring**: kafka.status = active, broker_count populated. Refresh keeps state stable.
- [ ] **Restart**: rolling restart kicks off, log streams in
- [ ] **Service Logs**: tail shows journal entries from the cluster's kafka-*.service
- [ ] **Schema Registry**: tab visible. If not deployed, shows Deploy form. If deployed, lists subjects.

### State stability
- [ ] Refresh the page (browser F5) — cluster doesn't flip to "stopped"
- [ ] Click the cluster's Refresh button 10x in a row — state stays "running"

### External cluster
- [ ] External Clusters → Add → fill bootstrap_servers, security_protocol → Test Connection succeeds
- [ ] List Topics button shows spinner + topic count
- [ ] External cluster Config tab edits a key successfully
- [ ] Federation page shows the external with `broker_count` populated (not "—")

### RBAC
- [ ] Create a monitor user
- [ ] In incognito, log in as monitor — can read clusters, can't see Users page
- [ ] Admin promotes monitor to admin → next API call from old monitor tab returns 401 (token_version)

### LDAP (only if you have LDAP test infra)
- [ ] User logs in via LDAP → row appears in Users page with **LDAP** badge
- [ ] Key icon (password change) is greyed out for LDAP-synced users

### Install variants
- [ ] `--install-dir /data/tantor` produces `/data/tantor/{app,data,log}`, all services healthy
- [ ] `--purge` followed by `ls /opt/kafka-* /var/lib/kafka-* /etc/systemd/system/kafka-*.service` shows zero leftover. `ps aux | grep -c /opt/kafka-` is 0.

If any item fails, file a bug with the URL + step number + screenshot. Don't ship.

---

## What's NOT covered yet

Items deferred from the regression battery (would-be-nice but require either non-AWS env or significant work):

| Gap | Why deferred | Workaround |
|---|---|---|
| mTLS handshake-rejection of anonymous client | Test host port reuse / zombie restart issues during the v1.4.1 verify made this flaky | Config-level verified: `ssl.client.auth=required` in `server.properties` |
| LDAP integration | Needs an LDAP server | Manual test against the customer's AD when they install |
| Schema Registry deploy end-to-end | Apicurio download is slow on AWS test hosts | Manually triggered + verified |
| Long-running Kafka stability | Tantor is a control plane; broker stability is upstream Kafka's problem | None |
| Frontend visual regression | No screenshot tooling wired up | Use L3 checklist |

PRs welcome to add automated assertions for any of these.
