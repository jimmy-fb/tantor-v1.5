# Contributing

Welcome. Tantor is built like a small product team's codebase — pragmatic, with the rough edges visible in the comments. New contributors should be able to ship a fix in their first day. This doc is the on-ramp.

---

## Setup (10 minutes)

```bash
git clone https://github.com/jimmy-fb/tantor.git
cd tantor

# Backend
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd ..

# Frontend
cd frontend
npm ci
cd ..

# Run both (two terminals)
# Terminal 1
cd backend && source venv/bin/activate
uvicorn app.main:app --reload --port 8000

# Terminal 2
cd frontend && npm run dev    # http://localhost:5173

# Visit http://localhost:5173 → admin / admin
```

The frontend dev server proxies `/api/*` to the backend on `:8000` automatically.

If you want a fully self-contained environment for an integration test, use the Docker compose at the repo root:

```bash
docker compose up
# UI at http://localhost (port 80)
```

---

## Where to find each piece

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) has the full map. Quick version:

- **An API change** → start in `backend/app/api/<area>.py`. The schema is in `backend/app/schemas/`. The actual work happens in `backend/app/services/<area>_manager.py` or similar.
- **A UI change** → `frontend/src/pages/` or `frontend/src/components/clusters/`.
- **A deployment change** → `backend/app/templates/ansible/deploy_kafka.yml.j2` or `backend/app/templates/systemd/*.j2`.
- **A new database column** → add to the SQLAlchemy model + add a migration row in `backend/app/services/migrations.py` (lightweight ALTER TABLE at startup).

---

## Workflow

### 1. Branch

```bash
git checkout -b fix/short-description
# or
git checkout -b feat/short-description
```

We use `main` as the trunk. No long-running release branches.

### 2. Write the code

Match the existing style. Pragmatic comments explaining WHY, not WHAT. Re-read your diff before pushing — `git diff main` is your friend.

### 3. Test locally

- Run `cd backend && python3 -m pytest tests/` if you added tests (unit-level)
- Type-check the frontend: `cd frontend && npx tsc --noEmit`
- Bash-syntax-check shell changes: `bash -n install.sh`

### 4. Add a regression assertion

**Every fix to a customer-reported bug should add a check in `tests/regression/run_all.sh`.** This is how we stopped the v1.4.2 regression spiral. See [docs/TESTING.md](docs/TESTING.md) for the assertion pattern.

If your change is a pure refactor or internal cleanup with no behavior change, you don't need a regression assertion — but say so in the PR description.

### 5. Update docs if user-facing

- API change → `docs/ARCHITECTURE.md` section 2.2 ("Key services") if you added a service
- New endpoint → mention in the relevant doc; the OpenAPI at `/docs` auto-updates
- Feature add → `docs/CHANGELOG.md` under "Unreleased" (or the next version section)

### 6. PR

Open against `main`. PR template (also enforced as `.github/PULL_REQUEST_TEMPLATE.md`):

```
## What changes
Brief — what is the user-visible behavior change?

## Why
Link the customer issue number or describe the bug.

## How
The technical approach. Especially: any architectural decisions, why this path over another.

## Risk / blast radius
What could break? Which other paths touch this code?

## Test plan
- [ ] `npx tsc --noEmit` clean
- [ ] `bash -n install.sh` clean
- [ ] Regression battery passes locally (or note why it doesn't apply)
- [ ] Added a new assertion for the fix
- [ ] Verified end-to-end on at least one OS (RHEL or Ubuntu)
```

### 7. Review

At least one reviewer's approval before merge. Reviewer looks for:
- Does the code do what the description says
- Are there obvious bugs (off-by-one, null deref, missing await)
- Does it break existing assertions
- Is the new assertion meaningful (not just "endpoint returned 200")
- Are comments explaining non-obvious decisions

### 8. Merge

Squash-merge is fine; the commit message becomes the PR body. Don't squash if the PR has 5+ meaningful commits that each tell a story.

---

## Coding conventions

### Python

- `from __future__ import annotations` at the top of files using `|` union types in pre-3.10 contexts (we target 3.10+ but be explicit)
- Type hints on function signatures; not always on locals
- Docstrings on public service methods, especially anything customers can trigger
- f-strings everywhere; no `%` formatting
- `logging` not `print` in service code
- Comments lead with the WHY: `# v1.4.3 #22 — bump token_version so in-flight JWTs are rejected on next API call` — the version + issue-number prefix lets reviewers find the original context quickly. (Drop the version prefix if you're adding something new.)

### TypeScript

- Strict mode enabled; resolve all `tsc --noEmit` errors before pushing
- Tailwind for styling (no CSS modules / styled-components)
- `useState` + `useEffect` patterns are fine — we don't have a global store
- Polling pages: use the `silent` pattern from `TopicManager.tsx` so background polls don't flash the loading spinner

### SQL / Migrations

- New columns are NULLable or have a `DEFAULT`. Required-not-null columns can't be added to populated tables in SQLite without a table-rewrite (we don't do that today).
- Add the migration row to `backend/app/services/migrations.py` in the same PR as the model change.

### Ansible / Jinja

- Idempotency is non-negotiable. Every task should be safe to run twice in a row.
- Errors that are expected on first run (e.g. "service doesn't exist yet") should be detected and skipped, not `ignore_errors: true` blanket-suppressed. See the v1.4.3 #2 fix for the pattern.

---

## Bug filing

Customer-reported bugs are tracked externally (issue tracker / Slack / spreadsheet). When we triage one and decide to fix it:

1. Create a GitHub issue with the customer's reproduction (sanitize any customer-specific names / IPs first).
2. Reference the issue number in the PR title and commit messages.
3. After merge, the customer-facing changelog entry (in `docs/CHANGELOG.md`) gets the issue number too so customers can map releases back to their reports.

---

## Releasing

[docs/BUILD.md](docs/BUILD.md) has the full process. Quick version:

1. Bump VERSION in `install.sh` + `build-installer.sh` + `backend/app/main.py`
2. Update `docs/CHANGELOG.md`
3. `./build-installer.sh` → produces `tantor-installer-X.Y.Z.bin`
4. Run regression battery on RHEL + Ubuntu
5. Commit + tag
6. Push the .bin to the installer distribution repo

---

## Code of conduct

Be kind. Reviews are about the code, not the person. If something's confusing, ask. If a comment is dismissive, call it out. The bar is the same regardless of seniority — make your reasoning visible.

---

## Asking for help

- Stuck on a fix? Comment on the issue with what you've tried.
- Don't know where the code lives? Grep first — and if you don't find it, update `docs/ARCHITECTURE.md` section 5 in your PR so the next person doesn't have to repeat the search.
- Found a security issue? Email the maintainer directly instead of opening a public issue.
