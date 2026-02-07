# Changelog

All notable changes to this project will be documented in this file.

The format is based on *Keep a Changelog*.

## [Unreleased]

### Added

- `AGENTS.md` development rules and TDD/fixtures policy.
- `agent.md` development rules (for editors expecting `agent.md`).
- Initial migration plan (see `PLAN.md`).
- Python project scaffold (`pyproject.toml`, `src/`, `tests/`) + `mise.toml` tasks for `fmt`, `lint`, `test`.
- Mini GitLab backup fixture derived from the real backup (`fixtures/gitlab-mini/`).
- Streaming Postgres COPY parser (`src/gitlab_to_forgejo/copy_parser.py`) with unit tests.
- Pleroma-scoped plan builder (namespaces/projects/users/issues/MRs/notes) (`src/gitlab_to_forgejo/plan_builder.py`) with unit tests.
- Docker Compose Forgejo + Postgres stack (`docker-compose.yml`) with `mise run up/reset` tasks.
- Forgejo bootstrap scripts to create an admin user and token (`scripts/bootstrap_forgejo.sh`, `scripts/wait_for_url.sh`).
- Forgejo API client (`src/gitlab_to_forgejo/forgejo_client.py`) + migrator apply step for users/orgs/teams (`src/gitlab_to_forgejo/migrator.py`).
- CLI entrypoint (`src/gitlab_to_forgejo/cli.py`, `src/gitlab_to_forgejo/__main__.py`) with `migrate` command.
- `mise` tasks: `mise run migrate` (fixture) and `mise run migrate-real` (real backup).
- Forgejo repo creation + git bundle push (`src/gitlab_to_forgejo/git_push.py`, `src/gitlab_to_forgejo/git_refs.py`, `src/gitlab_to_forgejo/migrator.py`).
- Issue + pull request import (from GitLab issues/MRs) with comments/notes attribution via Forgejo `sudo` (`src/gitlab_to_forgejo/migrator.py`, `src/gitlab_to_forgejo/forgejo_client.py`).
- GitLab label migration: parses `labels` + `label_links`, creates Forgejo repo labels, and assigns them to imported issues/PRs.
- GitLab user avatar migration: reads avatar files from `uploads.tar.gz` and uploads them via Forgejo `/api/v1/user/avatar` (admin `sudo`).
- GitLab issue/comment upload migration: rewrites `/uploads/<hash>/<file>` links by uploading the referenced files as Forgejo attachments and patching issue/comment bodies.
- Wiki repo push: best-effort push of GitLab `*.wiki.git` bundles to Forgejo wiki remotes, mapping the GitLab wiki default branch to Forgejo `main`.
- Forgejo wiki git repo initializer (`src/gitlab_to_forgejo/forgejo_wiki.py`) so wiki pushes work on a fresh Forgejo volume.
- MR head-branch preparation: pushes deterministic `gitlab-mr-iid-<iid>` branches for merged MRs whose source branches are missing.
- Git HTTP push chunking to avoid OS command-line length limits when pushing many refs.
- `--only-repo` / `GITLAB_ONLY_REPO` filter to run a fast single-repo migration for debugging.
- Forgejo DB post-processing (`src/gitlab_to_forgejo/forgejo_db.py`) to backfill historical issue/comment timestamps and issue open/closed state (including PR opening posts and MRs imported as issues).
- Optional GitLab password hash migration: `--migrate-password-hashes` (or `FORGEJO_MIGRATE_PASSWORD_HASHES=1`) copies GitLab `users.encrypted_password` bcrypt hashes into Forgejo via direct DB update so users can keep their existing passwords (best-effort; GitLab password “pepper” setups may not be compatible).
- Contextual error logging in the migrator (includes repo + GitLab IDs/branches) to make long-running failures easier to pinpoint.
- Best-effort migration runs: the migrator continues on per-entity failures and writes errors to `state/errors.log` (override via `--errors-log` / `FORGEJO_ERRORS_LOG`).
- Migration progress logging: phase/timing logs at INFO level to make long runs easier to follow.
- Optional issue/comment DB fast-path import: `--fast-db-issues` (or `FORGEJO_FAST_DB_ISSUES=1`) inserts issues/comments directly into Forgejo DB tables for significantly faster large-repo migration runs, with fallback to API mode on DB-step failure.
- GitLab user SSH key migration (`keys` table): user-owned SSH keys are imported through Forgejo's user key API.
- 2FA portability warning: logs users with GitLab 2FA enabled and notes that backup-only migration cannot preserve GitLab TOTP/WebAuthn credentials.
- `docker-compose.ngrok.yml` override file to set Forgejo `DOMAIN`/`ROOT_URL`/`SSH_DOMAIN` from `FORGEJO_PUBLIC_DOMAIN` for ngrok demos.
- README handoff notes for moving a completed local migration into a Coolify-hosted Forgejo deployment (DB + `/data` lift-and-shift, canonical URL/secrets caveats).
- README notes on post-migration CI/CD direction (Woodpecker-first for GitLab pipeline parity) and Docker image/OCI registry placement in Forgejo package storage.
- Final migration DB sequence resync step: the migrator now runs a PostgreSQL owned-sequence reset (`setval` to `MAX(id)+1`) after import/backfill to prevent post-migration 500s from sequence drift after direct DB inserts.

### Fixed

- `migrate-real` no longer crashes when a GitLab repo has an empty/missing `001.refs` (no `refs/heads/*` or `refs/tags/*`); such repos are now treated as empty and skipped during git push.
- Non-group-member users who commented/authored issues/MRs are now added as `Reporters` in the relevant org so their issues/comments can be created under their identity (private repos + `sudo`).
- Wiki push no longer fails with `fatal: Could not read from remote repository` on a fresh Forgejo instance (Forgejo wiki git repos are now created before pushing).
- MR import no longer sends raw SHAs as Forgejo PR `head` (Forgejo requires branch names); merged MRs with missing source branches now use synthetic branches.
- Non-merged MRs missing their source branches are imported as issues (`MR: ...`) instead of crashing PR creation.
- PR creation retries transient Forgejo 404s (`"The target couldn't be found."` with empty `errors`) which can happen right after pushing refs.
- MR import no longer crashes when Forgejo rejects PR creation with `422` "There are no changes between the head and the base"; such MRs are imported as issues.
- Default branch detection prefers `develop`/`trunk`/`stable` when `main`/`master` are absent.
- Notes with `project_id` missing are no longer dropped when the project can be inferred from the issue/MR.
- GitLab `/uploads/...` comment attachment migration no longer fails when the comment author lacks write permission; the migrator retries comment-attachment uploads as the admin user.
- MR import no longer crashes when the GitLab target branch is missing from the backup; the migrator uses `merge_request_diffs.base_commit_sha` as the PR base when available, otherwise falls back to importing the MR as an issue.
- MR import no longer triggers Forgejo 500s when targeting a missing base branch: the migrator now pushes a synthetic base branch `gitlab-mr-base-iid-<iid>` pointing at `merge_request_diffs.base_commit_sha` and uses that as the PR base.
- MR import no longer fails with Forgejo 409 "pull request already exists for these targets" when GitLab reuses branch names: the migrator now prefers per-MR synthetic head branches `gitlab-mr-iid-<iid>` sourced from `merge_request_diffs.head_commit_sha` or `refs/merge-requests/<iid>/head` when available.
- Docker Compose now defaults to migration-friendly Forgejo settings (`queue.TYPE=channel`, `indexer.ISSUE_INDEXER_TYPE=db`) to speed up large imports.
- Docker Compose now also defaults to migration-friendly notification settings (`service.AUTO_WATCH_NEW_REPOS=false`, `service.AUTO_WATCH_ON_CHANGES=false`, `actions.ENABLED=false`, `service.ENABLE_NOTIFY_MAIL=false`, `other.ENABLE_FEED=false`) to reduce issue/comment import fan-out overhead.
- Fast DB issue/comment mode now skips redundant issue/comment metadata backfill SQL and only backfills MR metadata.
