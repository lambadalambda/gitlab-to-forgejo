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
