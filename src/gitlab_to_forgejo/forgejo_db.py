from __future__ import annotations

import subprocess
from collections.abc import Mapping

from gitlab_to_forgejo.plan_builder import Plan


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


_BCRYPT_HASH_PREFIXES = ("$2a$", "$2b$", "$2y$")


def build_password_hash_fix_sql(
    plan: Plan,
    *,
    forgejo_username_by_gitlab_username: Mapping[str, str],
    skip_forgejo_usernames: set[str] | None = None,
) -> str:
    """Build SQL to overwrite Forgejo password hashes with GitLab bcrypt hashes.

    This is a bespoke migration helper and intentionally uses direct DB updates because
    Forgejo's API does not provide an endpoint to set password hashes.
    """
    updates: list[str] = []
    skip = skip_forgejo_usernames or set()

    for user in plan.users:
        hashed = (user.gitlab_encrypted_password or "").strip()
        if not hashed:
            continue
        if not hashed.startswith(_BCRYPT_HASH_PREFIXES):
            continue

        forgejo_username = forgejo_username_by_gitlab_username.get(user.username)
        if not forgejo_username or forgejo_username in skip:
            continue

        updates.extend(
            [
                f"-- gitlab user {user.username} -> forgejo user {forgejo_username}",
                'UPDATE "user" u',
                "SET",
                f"  passwd = {_sql_literal(hashed)},",
                "  passwd_hash_algo = 'bcrypt'",
                f"WHERE u.lower_name = lower({_sql_literal(forgejo_username)});",
            ]
        )

    if not updates:
        return ""

    return "\n".join(["BEGIN;", *updates, "COMMIT;", ""])


def build_metadata_fix_sql(
    plan: Plan,
    *,
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
    comment_id_by_gitlab_note_id: Mapping[int, int],
    include_issues: bool = True,
    include_merge_requests: bool = True,
    include_notes: bool = True,
) -> str:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    lines: list[str] = ["BEGIN;"]

    if include_issues:
        for issue in plan.issues:
            issue_number = issue_number_by_gitlab_issue_id.get(issue.gitlab_issue_id)
            if issue_number is None:
                continue
            repo = repo_by_project_id.get(issue.gitlab_project_id)
            if repo is None:
                continue

            created_unix = int(issue.created_unix or 0)
            updated_unix = int(issue.updated_unix or 0)
            closed_unix = int(issue.closed_unix or 0)
            is_closed = issue.state_id != 1 and issue.state_id != 0

            if updated_unix <= 0:
                updated_unix = created_unix
            if created_unix <= 0:
                created_unix = updated_unix
            if is_closed and closed_unix <= 0:
                closed_unix = updated_unix or created_unix
            if not is_closed:
                closed_unix = 0

            lines.extend(
                [
                    (
                        f"-- gitlab issue #{issue.gitlab_issue_iid} → "
                        f"{repo.owner}/{repo.name} #{issue_number}"
                    ),
                    "UPDATE issue i",
                    "SET",
                    f"  created = {created_unix},",
                    f"  created_unix = {created_unix},",
                    f"  updated_unix = {updated_unix},",
                    f"  closed_unix = {closed_unix},",
                    f"  is_closed = {'TRUE' if is_closed else 'FALSE'}",
                    "FROM repository r",
                    'JOIN "user" u ON u.id = r.owner_id',
                    "WHERE i.repo_id = r.id",
                    f"  AND u.lower_name = lower({_sql_literal(repo.owner)})",
                    f"  AND r.lower_name = lower({_sql_literal(repo.name)})",
                    f'  AND i."index" = {int(issue_number)}',
                    "  AND i.is_pull = FALSE;",
                ]
            )

    if include_merge_requests:
        for mr in plan.merge_requests:
            pr_number = pr_number_by_gitlab_mr_id.get(mr.gitlab_mr_id)
            if pr_number is None:
                continue
            repo = repo_by_project_id.get(mr.gitlab_target_project_id)
            if repo is None:
                continue

            created_unix = int(mr.created_unix or 0)
            updated_unix = int(mr.updated_unix or 0)
            closed_unix = int(mr.closed_unix or 0)
            is_closed = mr.state_id != 1 and mr.state_id != 0

            if updated_unix <= 0:
                updated_unix = created_unix
            if created_unix <= 0:
                created_unix = updated_unix
            if is_closed and closed_unix <= 0:
                closed_unix = updated_unix or created_unix
            if not is_closed:
                closed_unix = 0

            lines.extend(
                [
                    (
                        f"-- gitlab mr !{mr.gitlab_mr_iid} → "
                        f"{repo.owner}/{repo.name} #{int(pr_number)}"
                    ),
                    "UPDATE issue i",
                    "SET",
                    f"  created = {created_unix},",
                    f"  created_unix = {created_unix},",
                    f"  updated_unix = {updated_unix},",
                    f"  closed_unix = {closed_unix},",
                    f"  is_closed = {'TRUE' if is_closed else 'FALSE'}",
                    "FROM repository r",
                    'JOIN "user" u ON u.id = r.owner_id',
                    "WHERE i.repo_id = r.id",
                    f"  AND u.lower_name = lower({_sql_literal(repo.owner)})",
                    f"  AND r.lower_name = lower({_sql_literal(repo.name)})",
                    f'  AND i."index" = {int(pr_number)};',
                ]
            )

    if include_notes:
        for note in plan.notes:
            comment_id = comment_id_by_gitlab_note_id.get(note.gitlab_note_id)
            if comment_id is None:
                continue

            created_unix = int(note.created_unix or 0)
            updated_unix = int(note.updated_unix or 0)
            if updated_unix <= 0:
                updated_unix = created_unix

            lines.extend(
                [
                    f"-- gitlab note {note.gitlab_note_id} → forgejo comment {int(comment_id)}",
                    "UPDATE comment",
                    f"SET created_unix = {created_unix}, updated_unix = {updated_unix}",
                    f"WHERE id = {int(comment_id)};",
                ]
            )

    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def build_fast_issue_import_sql(
    plan: Plan,
    *,
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    forgejo_username_by_gitlab_user_id: Mapping[int, str],
) -> str:
    repo_by_project_id = {repo.gitlab_project_id: repo for repo in plan.repos}

    max_index_by_repo: dict[tuple[str, str], int] = {}
    lines: list[str] = ["BEGIN;"]
    inserted = 0

    for issue in plan.issues:
        issue_number = issue_number_by_gitlab_issue_id.get(issue.gitlab_issue_id)
        if issue_number is None:
            continue
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            continue
        poster_username = forgejo_username_by_gitlab_user_id.get(issue.author_id)
        if not poster_username:
            continue

        created_unix = int(issue.created_unix or 0)
        updated_unix = int(issue.updated_unix or 0)
        closed_unix = int(issue.closed_unix or 0)
        is_closed = issue.state_id != 1 and issue.state_id != 0

        if updated_unix <= 0:
            updated_unix = created_unix
        if created_unix <= 0:
            created_unix = updated_unix
        if is_closed and closed_unix <= 0:
            closed_unix = updated_unix or created_unix
        if not is_closed:
            closed_unix = 0

        lines.extend(
            [
                (
                    f"-- gitlab issue #{issue.gitlab_issue_iid} ({issue.gitlab_issue_id}) → "
                    f"{repo.owner}/{repo.name} #{issue_number}"
                ),
                "INSERT INTO issue (",
                '  repo_id, "index", poster_id, name, content, content_version,',
                "  is_closed, is_pull, num_comments, ref, pin_order,",
                "  created, created_unix, updated_unix, closed_unix, is_locked",
                ")",
                "SELECT",
                (
                    "  r.id, "
                    f"{int(issue_number)}, "
                    f"u.id, {_sql_literal(issue.title)}, {_sql_literal(issue.description)}, 0,"
                ),
                (
                    "  "
                    f"{'TRUE' if is_closed else 'FALSE'}, FALSE, 0, '', 0,"
                ),
                f"  {created_unix}, {created_unix}, {updated_unix}, {closed_unix}, FALSE",
                "FROM repository r",
                'JOIN "user" owner_u ON owner_u.id = r.owner_id',
                f'JOIN "user" u ON u.lower_name = lower({_sql_literal(poster_username)})',
                f"WHERE owner_u.lower_name = lower({_sql_literal(repo.owner)})",
                f"  AND r.lower_name = lower({_sql_literal(repo.name)})",
                'ON CONFLICT (repo_id, "index") DO NOTHING;',
            ]
        )
        max_index_by_repo[(repo.owner, repo.name)] = max(
            int(issue_number), max_index_by_repo.get((repo.owner, repo.name), 0)
        )
        inserted += 1

    if inserted == 0:
        return ""

    for (owner, repo), max_index in sorted(max_index_by_repo.items()):
        lines.extend(
            [
                f"-- sync issue_index for {owner}/{repo}",
                "INSERT INTO issue_index (group_id, max_index)",
                f"SELECT r.id, {int(max_index)}",
                "FROM repository r",
                'JOIN "user" owner_u ON owner_u.id = r.owner_id',
                f"WHERE owner_u.lower_name = lower({_sql_literal(owner)})",
                f"  AND r.lower_name = lower({_sql_literal(repo)})",
                "ON CONFLICT (group_id)",
                "DO UPDATE SET max_index = GREATEST(issue_index.max_index, EXCLUDED.max_index);",
            ]
        )

    lines.append("COMMIT;")
    return "\n".join(lines) + "\n"


def build_fast_note_import_sql(
    plan: Plan,
    *,
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
    forgejo_username_by_gitlab_user_id: Mapping[int, str],
) -> tuple[str, dict[int, int]]:
    repo_by_project_id = {repo.gitlab_project_id: repo for repo in plan.repos}

    comment_id_by_gitlab_note_id: dict[int, int] = {}
    touched_repos: set[tuple[str, str]] = set()
    lines: list[str] = ["BEGIN;"]

    for note in plan.notes:
        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None:
            continue
        poster_username = forgejo_username_by_gitlab_user_id.get(note.author_id)
        if not poster_username:
            continue

        if note.noteable_type == "Issue":
            issue_number = issue_number_by_gitlab_issue_id.get(note.noteable_id)
        elif note.noteable_type == "MergeRequest":
            issue_number = pr_number_by_gitlab_mr_id.get(note.noteable_id)
        else:
            continue
        if issue_number is None:
            continue

        created_unix = int(note.created_unix or 0)
        updated_unix = int(note.updated_unix or 0)
        if updated_unix <= 0:
            updated_unix = created_unix

        comment_id = int(note.gitlab_note_id)
        comment_id_by_gitlab_note_id[note.gitlab_note_id] = comment_id
        touched_repos.add((repo.owner, repo.name))

        lines.extend(
            [
                (
                    f"-- gitlab note {note.gitlab_note_id} "
                    f"({note.noteable_type} {note.noteable_id})"
                    f" → {repo.owner}/{repo.name} #{int(issue_number)}"
                ),
                "INSERT INTO comment (",
                (
                    "  id, type, poster_id, issue_id, content, content_version, "
                    "created_unix, updated_unix"
                ),
                ")",
                "SELECT",
                (
                    f"  {comment_id}, 0, u.id, i.id, {_sql_literal(note.body)}, 0, "
                    f"{created_unix}, {updated_unix}"
                ),
                "FROM issue i",
                "JOIN repository r ON r.id = i.repo_id",
                'JOIN "user" owner_u ON owner_u.id = r.owner_id',
                f'JOIN "user" u ON u.lower_name = lower({_sql_literal(poster_username)})',
                f"WHERE owner_u.lower_name = lower({_sql_literal(repo.owner)})",
                f"  AND r.lower_name = lower({_sql_literal(repo.name)})",
                f'  AND i."index" = {int(issue_number)}',
                "ON CONFLICT (id) DO NOTHING;",
            ]
        )

    if not comment_id_by_gitlab_note_id:
        return "", {}

    for owner, repo in sorted(touched_repos):
        lines.extend(
            [
                f"-- refresh comment counters for {owner}/{repo}",
                "UPDATE issue i",
                "SET num_comments = (",
                "  SELECT COUNT(*)::int",
                "  FROM comment c",
                "  WHERE c.issue_id = i.id",
                "    AND c.type = 0",
                ")",
                "FROM repository r",
                'JOIN "user" owner_u ON owner_u.id = r.owner_id',
                "WHERE i.repo_id = r.id",
                f"  AND owner_u.lower_name = lower({_sql_literal(owner)})",
                f"  AND r.lower_name = lower({_sql_literal(repo)});",
            ]
        )

    lines.append("COMMIT;")
    return "\n".join(lines) + "\n", comment_id_by_gitlab_note_id


def apply_metadata_fix_sql(sql: str) -> None:
    if not sql.strip():
        return
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            "forgejo",
            "-d",
            "forgejo",
            "-v",
            "ON_ERROR_STOP=1",
        ],
        input=sql,
        text=True,
        check=True,
    )
