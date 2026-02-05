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
) -> str:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    lines: list[str] = ["BEGIN;"]

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
                f"-- gitlab mr !{mr.gitlab_mr_iid} → {repo.owner}/{repo.name} #{int(pr_number)}",
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
