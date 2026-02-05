# gitlab-to-forgejo

Docker Compose + Python migrator to import a **GitLab backup** (repos, wikis, users, groups, memberships, issues, merge requests) into a fresh **Forgejo** instance.

## Scope (initial)

- Only the GitLab **`pleroma`** group and its subgroups/projects.
- Each GitLab subgroup becomes its own Forgejo org with a flattened name (e.g. `pleroma-elixir-libraries`).
- Only users who interacted with the `pleroma` scope are migrated.

## Status

- Implemented: create Forgejo users/orgs/teams, upload user avatars, create org repos, push Git data from `*.bundle`, initialize + push wiki git data when present, import issues/MRs with comments, migrate labels, migrate `/uploads/...` attachments referenced in issue/comment Markdown, and backfill issue/comment timestamps + issue open/closed state via DB post-processing.

## Development

- Rules: `AGENTS.md` / `agent.md`
- Plan: `PLAN.md`
- Changelog: `CHANGELOG.md`

## Tasks

This repo uses `mise`:

- `mise run fmt`
- `mise run lint`
- `mise run test`
- `mise run up`
- `mise run reset`
- `mise run bootstrap`
- `mise run migrate` (fixture)
- `mise run migrate-real` (real backup)

For faster iteration, restrict migration to a single repo:

- `GITLAB_ONLY_REPO=pleroma/docs mise run migrate-real`
- `GITLAB_ONLY_REPO=pleroma/docs mise run migrate` (fixture)

The migrator is best-effort: it continues on per-entity failures and logs errors to the terminal and `state/errors.log` (override via `FORGEJO_ERRORS_LOG` or `--errors-log`).

To (best-effort) preserve GitLab user passwords by copying bcrypt hashes into Forgejo via DB update (opt-in):

- `FORGEJO_MIGRATE_PASSWORD_HASHES=1 mise run migrate-real`

If ports are already in use, override with:

- `FORGEJO_HTTP_PORT=3001 FORGEJO_SSH_PORT=2223 mise run up`

## Forgejo login

- URL: `http://localhost:${FORGEJO_HTTP_PORT:-3000}`
- Admin username/password (dev): `root` / `admin1234` (see `scripts/bootstrap_forgejo.sh`)
