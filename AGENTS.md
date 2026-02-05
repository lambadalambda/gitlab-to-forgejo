# gitlab-to-forgejo: agent & development rules

## Non-negotiables

- **TDD first**: write/adjust tests before implementation for any new behavior.
- **Fixtures come from the real backup** at `~/pleromagit-backup`, but keep committed fixtures **small** (hand-picked subset).
- **Repeatable migration**: a clean-slate run (fresh Forgejo volumes) must produce the same result deterministically.
- **Small, topical commits**: one concern per commit; update `CHANGELOG.md` with each commit.
- **Commit after each feature/fix**: don’t accumulate large uncommitted diffs; land work in a sequence of small commits as you go.
- **Historical correctness**: issue open/closed state and timestamps (issues + comments) must match GitLab; if Forgejo’s API can’t set them, use DB post-processing.
- **Passwords**: default is a single known `--user-password` for created users (dev-friendly). If you need to preserve GitLab passwords, enable `--migrate-password-hashes` / `FORGEJO_MIGRATE_PASSWORD_HASHES=1` to copy GitLab bcrypt hashes into Forgejo via direct DB update (best-effort; may be incompatible with GitLab “pepper” setups).

## Repository conventions

- Prefer **Python** for the migrator (simple to run locally and in containers).
- Keep logic pure and testable:
  - Parsing GitLab backup → deterministic “manifest” data structures.
  - Applying manifest to Forgejo (API + `git push`) behind small interfaces.
- Tests are split:
  - **Unit tests**: fast, no Docker, only committed fixtures under `fixtures/`.
  - **Integration tests**: may use Docker Compose and can optionally point at the full backup.
- Use **mise** as the task runner (`mise run <task>`). Do not add `Makefile` targets.

## Fixtures policy

- Committed fixtures live under `fixtures/` and must be:
  - minimal (a few projects/namespaces, 1–2 tiny repos)
  - deterministic (checked into git)
  - safe to share within this repo
- Provide a script (to be written) to (re)generate fixtures from the full backup, e.g.:
  - `scripts/extract_fixtures.py --backup ~/pleromagit-backup --out fixtures/`

## Commands (planned)

- `mise run test` runs unit tests.
- `mise run itest` runs integration tests (Docker required).
- `mise run up` starts Forgejo for local testing.
- `mise run reset` wipes Forgejo volumes (clean slate).
- `mise run migrate` runs the migrator (users/orgs/teams + repos + git push).
- `mise run migrate-real` runs the migrator against `~/pleromagit-backup`.
- To migrate only a single repo for faster iteration, set `GITLAB_ONLY_REPO` (or pass `--only-repo` to the CLI):
  - `GITLAB_ONLY_REPO=pleroma/docs mise run migrate-real`

## Migration scope (initial)

- Focus only on the **`pleroma`** GitLab group and its subgroups/projects at first.
- Migrate: repos, wikis, users, groups, memberships, issues, and merge requests (MRs).
- Only migrate users who **interacted with the pleroma group** (membership and/or authored/commented on issues/MRs in that scope).
- Each GitLab subgroup becomes its own Forgejo org via flattened names (e.g. `pleroma-elixir-libraries`).
- Preserve user emails as-is (same-domain migration).
