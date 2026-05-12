# Build

How to go from a clean source checkout to a working `.bin` installer, and how to iterate fast against a running install.

---

## Prerequisites

| Tool | Version | What it's for |
|---|---|---|
| Python | 3.10+ | Backend runtime + venv build |
| Node.js | 20.x | Frontend build (`vite build`) |
| Bash | 4.x | `install.sh` + `build-installer.sh` |
| GNU tar | any | Self-extracting payload |
| OpenSSL | any | TLS cert generation in `--tls` mode |

macOS: `brew install python@3.11 node` is enough.
Linux dev box: your distro package manager.

---

## One-shot build

```bash
# From repo root
./build-installer.sh
```

That runs:
1. `npm ci && npm run build` in `frontend/` → produces `frontend/dist/` (the static bundle nginx serves).
2. Validates the dist isn't empty (catches the silent-failure case where vite errored but the script kept going).
3. `tar czf` over `backend/`, `frontend/dist/`, `installer/`, `install.sh` to produce a payload.
4. Prepends a self-extracting bash header that does `tail +N | tar xzf -` then runs `install.sh`.

Output: `tantor-installer-1.4.4.bin` (~485 KB) in the repo root. The version is read from `install.sh`'s `VERSION=` line and the backend's `APP_VERSION`.

---

## Step-by-step build (for understanding)

### 1. Backend dependencies

```bash
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Running locally:
```bash
uvicorn app.main:app --reload --port 8000
# http://localhost:8000/docs for the auto-generated OpenAPI
```

### 2. Frontend build

```bash
cd frontend
npm ci
npm run build           # production build into frontend/dist/
npm run dev             # dev server with HMR on :5173 (proxies /api to :8000)
```

Type-check only:
```bash
npx tsc --noEmit
```

### 3. Package

```bash
# What build-installer.sh does under the hood
mkdir -p /tmp/tantor-payload
cp -r backend frontend/dist installer install.sh /tmp/tantor-payload/
tar czf /tmp/payload.tgz -C /tmp/tantor-payload .
cat installer/sfx-header.sh /tmp/payload.tgz > tantor-installer-${VERSION}.bin
chmod +x tantor-installer-${VERSION}.bin
```

### 4. Smoke test

```bash
# Verify the .bin extracts and runs --info
./tantor-installer-1.4.4.bin --info
```

---

## Dev hot-patch (skip the full rebuild)

When you're iterating on a backend service file and have a Tantor instance running on a test VM, you don't need to rebuild + redeploy every time:

```bash
# Patch a single backend file onto a running install
scp backend/app/services/kafka_admin.py user@tantor-host:/tmp/
ssh user@tantor-host '
  sudo install -o tantor -g tantor -m 644 \
    /tmp/kafka_admin.py /opt/tantor/backend/app/services/kafka_admin.py
  sudo systemctl restart tantor-backend
'
```

This is the recipe used during development to validate every fix in 1.4.x before rebuilding the .bin.

For the frontend, you need a full `npm run build` + scp the entire `frontend/dist/` directory — there's no equivalent single-file hot-patch.

---

## Version bump

Three places to update on a release:

```bash
sed -i '' 's/VERSION="1.4.4"/VERSION="1.4.5"/g' install.sh build-installer.sh
sed -i '' 's/APP_VERSION = "1.4.4"/APP_VERSION = "1.4.5"/' backend/app/main.py
```

(Drop the `''` after `-i` on Linux GNU sed.)

Then `./build-installer.sh`. The .bin name and the in-app version both pick up the new value.

---

## Releasing

1. **Bump version** (above).
2. **Update `docs/CHANGELOG.md`** — one section per release with customer-item numbers + what changed.
3. **Build `.bin`** (`./build-installer.sh`).
4. **Run regression battery** on a fresh AWS instance:
   ```bash
   TANTOR_URL=https://<test-host> TANTOR_ADMIN=admin TANTOR_PASS=admin \
     bash tests/regression/run_all.sh
   ```
   Must return 0 on both RHEL 9.7 and Ubuntu 22.04. See [TESTING.md](TESTING.md).
5. **Commit + tag** in the source repo.
6. **Push `.bin`** to the installer distribution repo (`jimmy-fb/tantor-installer`).
7. **Update HANDOFF.md** in the installer repo (customer-facing one-pager).

---

## Embedded artifacts

The .bin does NOT include the Kafka tarball (~130 MB) by default. On first deploy, the installer either:
- Uses a tarball you've staged at `/var/lib/tantor/repo/kafka/kafka_2.13-4.1.0.tgz`, OR
- Auto-downloads from `archive.apache.org` (slow on first deploy; ~2 min on a typical VM).

If you want to embed the tarball (air-gapped customer ship), drop it into `backend/repo/kafka/` before building and the .bin will package it. Adds ~130 MB to the .bin size.

---

## Troubleshooting

- **`npm ci` fails** — delete `frontend/package-lock.json` and re-run with `npm install`. The lockfile may be stale if you bumped a dep without `npm install`-ing.
- **`vite build` error about type assertions** — run `npx tsc --noEmit` separately to see the type error. Vite swallows them by default.
- **`.bin` runs but Tantor fails to start** — check `/var/log/tantor-install.log`. The installer streams everything there with timestamps. Common cause: SELinux denial on `/data/tantor/log/backend/stdout.log` (fix: `chcon -R -t var_log_t /data/tantor/log`).
- **`install.sh: line N: syntax error`** — usually a single-quoted heredoc that got an apostrophe inside (the regression that bit 1.3.5). `bash -n install.sh` catches it.
