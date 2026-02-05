# GitLab → Forgejo migration plan (Docker/Compose)

## Goal

Given a GitLab backup directory at `~/pleromagit-backup` (currently `/Users/lainsoykaf/pleromagit-backup`), bring up a fresh Forgejo instance via Docker Compose and migrate selected GitLab projects into it in a **repeatable** way, with **TDD** coverage using small fixtures derived from the backup.

Initial focus: **only the `pleroma` GitLab group and its subgroups/projects**, migrating:

- repos + wikis
- users + groups + memberships
- issues + merge requests (MRs)

## Observations from the current backup

- GitLab version: `18.4.6` (docker install); backup id: `1770183352_2026_02_04_18.4.6`.
- Repository data is stored as `*.bundle` + `*.refs` under `repositories/<storage>/<disk_path>.git/<backup_id>/`.
- Repository disk paths come from `public.project_repositories` (not `public.projects`).
- There is a lot of likely spam content in namespaces/projects → migration should support **allow/deny lists**.

## Milestones & tasks

### M1 — Scaffold & developer workflow

- [x] Add `README.md` with goals and quickstart.
- [x] Add `.gitignore` and basic repo layout (`src/`, `tests/`, `fixtures/`, `scripts/`).
- [x] Add `mise.toml` tasks (use `mise run ...` instead of `make`).
- [x] Pick Python toolchain (venv + pip) and add `pyproject.toml`.
- [x] Add unit-test runner (`pytest`) + lint/format (`ruff`).

### M2 — Minimal Forgejo Compose environment (clean-slate capable)

- [x] Add `docker-compose.yml` running Forgejo pinned to a specific tag.
- [x] Add `mise run up` and `mise run reset` (`docker compose down -v`) tasks.
- [x] Add a bootstrap step to create an admin user + access token (token stored in a shared volume/file).
- [x] Add a “health wait” helper (used by bootstrap, migrator, integration tests).

Acceptance:
- [x] `mise run reset && mise run up && mise run bootstrap` results in a running Forgejo with an admin token available to other steps.

### M3 — GitLab backup parser → deterministic manifest

- [x] Implement a streaming parser for `db/database.sql.gz` COPY blocks.
- [x] Parse required tables (minimum):
  - [x] `public.namespaces` (build full paths, handle nesting)
  - [x] `public.projects` (id, path, namespace_id, visibility, archived, etc.)
  - [x] `public.project_repositories` (project_id → disk_path + object_format)
- [ ] Parse additional tables needed for users/memberships/issues/MRs (pleroma-only scope):
  - [x] `public.users`
  - [x] `public.members`
  - [x] `public.issues`
  - [x] `public.merge_requests`
  - [x] `public.notes` (issue/MR comments)
  - [x] `public.labels` + `public.label_links`
  - [ ] `public.milestones` + join tables as needed
- [x] Build deterministic in-memory plan data describing what will be created in Forgejo.
- [ ] Add a deterministic manifest (JSON/YAML) serializer for the plan (for debugging and review).
- [x] Add “pleroma-only” selection rules:
  - [x] Locate the `pleroma` namespace by `path='pleroma'` and include all descendants (prefer `traversal_ids` when present).
  - [x] Include only projects under those namespaces.
  - [x] Include only users who **interacted** with that scope: membership, issue/MR authors/assignees/reviewers, and commenters.
- [x] Namespace mapping: **flatten** GitLab group paths to Forgejo org names (join with `-`), and create **one Forgejo org per GitLab group/subgroup** in the `pleroma` tree.
- [ ] Add allow/deny filters for namespaces/projects (configurable).

TDD / fixtures:
- [x] Create small committed fixtures extracted from the real backup:
  - [x] mini SQL dump containing only the needed COPY blocks for a few rows
  - [x] 1–2 tiny repository bundle fixtures matching those rows
- [x] Unit tests for:
  - [x] COPY parsing
  - [x] namespace path reconstruction (nested groups)
  - [x] joining projects ↔ project_repositories
  - [x] bundle path resolution in the backup layout
  - [x] plan determinism (stable output)

Acceptance:
- [x] Plan builder produces the same output from the same fixtures every time.
- [ ] `migrator plan` produces the same serialized manifest from the same fixtures every time.

### M4 — Apply manifest to Forgejo (orgs/users/memberships)

- [x] Bootstrap Forgejo:
  - [x] create admin user (deterministic credentials for local dev)
  - [x] generate admin access token (written to a known path in a shared volume)
- [x] Forgejo client (API wrapper):
  - [x] create org if missing
  - [x] create user if missing
  - [x] upload user avatars from `uploads.tar.gz` (admin `sudo` via `/api/v1/user/avatar`)
  - [x] create teams for access levels and add members
  - [x] ensure deterministic ordering so ids/numbers are stable across clean-slate runs

Acceptance:
- [x] After `mise run migrate` (fixtures), orgs/users/teams exist and membership is correct.

### M5 — Apply manifest to Forgejo (repos + wikis + git data)

- [x] Create repositories and push Git data:
  - [x] create repo if missing
  - [x] set repo settings (private + default branch)
- [x] Git push pipeline:
  - [x] restore a mirror repo from `*.bundle` (`git clone --mirror <bundle>`)
  - [x] push refs to Forgejo (heads/tags only; ignore GitLab `refs/keep-around/*`)
  - [x] optional: handle wiki bundles (`.wiki.git`) if present
- [ ] Robustness:
  - [ ] concurrency control (safe parallel pushes)
  - [ ] retries/backoff for Forgejo API
  - [ ] clear progress + summary report

Integration tests:
- [ ] Bring up Forgejo via Compose, run bootstrap, run migrator against fixtures.
- [ ] Verify via API that expected repos exist.
- [ ] Clone a migrated repo and verify commit hash/branch exists.

Acceptance:
- [ ] `mise run reset && mise run migrate` produces a Forgejo instance that can be browsed and cloned from locally.

### M6 — Issues + merge requests (MRs)

- [x] Implement issue import (MVP):
  - [x] labels
  - [ ] milestones, assignees
  - [x] comments (notes)
  - [x] migrate GitLab `/uploads/...` attachments referenced in Markdown (issue bodies + comments)
  - [x] state (open/closed) + timestamps (`created_at`/`updated_at`/`closed_at`) backfilled via Forgejo DB post-processing
- [x] Implement MR import (as Forgejo pull requests) (MVP):
  - [x] ensure PR head branches exist:
    - [x] create deterministic `gitlab-mr-iid-<iid>` branches for merged MRs when the source branch is missing
    - [x] best-effort SHA sourcing via `merge_request_diffs.head_commit_sha` (fallback: GitLab `refs/merge-requests/<iid>/head` when present)
    - [x] fall back to importing non-merged MRs as issues when the source branch is missing
  - [x] labels
  - [ ] milestone/assignees/reviewers where possible
  - [x] comments and review notes (best-effort)
  - [ ] state (open/closed/merged) best-effort
- [x] Use admin “sudo”/impersonation where supported to attribute authors correctly.

Acceptance:
- [ ] A sample repo migrated from fixtures has its issues and PRs visible in Forgejo and link to existing commits/branches.

### M7 — Repeatability & idempotency

- [ ] Enforce “clean slate” expectation:
  - [ ] migrator refuses to run if instance isn’t empty unless `--force`
  - [ ] `mise run reset` is the supported way to start over
- [ ] Add integration test to run migration twice with a reset in between and compare results (repo list + HEAD SHAs).

Acceptance:
- [ ] A clean-slate run is deterministic and repeatable.

### M8 — Optional: LFS and other assets

- [ ] Decide on LFS strategy (upload via `git lfs push` vs direct storage import).
- [ ] Add fixture coverage for at least one LFS object if feasible.
- [ ] Add smoke test: clone with LFS enabled and verify object download.

## Open questions to resolve early

1. **MR fidelity**: how far do we go (reviews, approvals, pipelines), vs “PR + comments + labels/milestones”?
2. **User identities**: preserve emails verbatim (same-domain migration).
3. **Subgroup mapping**: confirmed — each GitLab subgroup becomes its own Forgejo org via flattened names.
