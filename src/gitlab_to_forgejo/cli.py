from __future__ import annotations

import argparse
import os
from pathlib import Path

from gitlab_to_forgejo.forgejo_client import ForgejoClient
from gitlab_to_forgejo.migrator import migrate_plan
from gitlab_to_forgejo.plan_builder import Plan, build_plan


def _default_backup_root() -> Path:
    return Path(os.environ.get("GITLAB_BACKUP_ROOT", "~/pleromagit-backup")).expanduser()


def _default_forgejo_url() -> str:
    if url := os.environ.get("FORGEJO_HTTP"):
        return url.rstrip("/")
    port = os.environ.get("FORGEJO_HTTP_PORT", "3000")
    return f"http://localhost:{port}"


def _default_token_file() -> Path:
    return Path(os.environ.get("FORGEJO_TOKEN_FILE", "state/forgejo/admin_token")).expanduser()


def _default_git_username() -> str:
    return (
        os.environ.get("FORGEJO_GIT_USERNAME") or os.environ.get("FORGEJO_ADMIN_USERNAME") or "root"
    )


def _read_token_file(path: Path) -> str:
    token = path.read_text(encoding="utf-8", errors="replace").strip()
    if not token:
        raise ValueError(f"token file is empty: {path}")
    return token


def _parse_only_repo(value: str) -> tuple[str | None, str]:
    raw = value.strip().strip("/")
    if not raw:
        raise ValueError("only-repo must not be empty")
    parts = [p for p in raw.split("/") if p]
    if len(parts) == 1:
        return None, parts[0]
    return "-".join(parts[:-1]), parts[-1]


def _filter_plan_to_single_repo(plan: Plan, *, only_repo: str) -> Plan:
    owner, repo = _parse_only_repo(only_repo)

    matches = [
        r
        for r in plan.repos
        if r.name == repo and (owner is None or r.owner == owner)
    ]
    if not matches:
        hint = f"{owner + '/' if owner else ''}{repo}"
        raise ValueError(f"--only-repo {hint!r} did not match any planned repo")
    if owner is None and len(matches) > 1:
        options = ", ".join(sorted({f"{r.owner}/{r.name}" for r in matches}))
        raise ValueError(f"--only-repo {repo!r} is ambiguous; choose one of: {options}")
    if len(matches) > 1:
        raise ValueError(f"--only-repo {only_repo!r} matched multiple repos unexpectedly")

    selected_repo = matches[0]
    selected_project_ids = {selected_repo.gitlab_project_id}
    selected_orgs = {selected_repo.owner}

    orgs = [o for o in plan.orgs if o.name in selected_orgs]
    repos = [r for r in plan.repos if r.gitlab_project_id in selected_project_ids]
    org_members = {k: v for k, v in plan.org_members.items() if k in selected_orgs}
    issues = [i for i in plan.issues if i.gitlab_project_id in selected_project_ids]
    merge_requests = [
        mr for mr in plan.merge_requests if mr.gitlab_target_project_id in selected_project_ids
    ]
    notes = [n for n in plan.notes if n.gitlab_project_id in selected_project_ids]

    selected_issue_ids = {i.gitlab_issue_id for i in issues}
    selected_mr_ids = {mr.gitlab_mr_id for mr in merge_requests}
    issue_label_ids_by_gitlab_issue_id = {
        issue_id: label_ids
        for issue_id, label_ids in plan.issue_label_ids_by_gitlab_issue_id.items()
        if issue_id in selected_issue_ids
    }
    mr_label_ids_by_gitlab_mr_id = {
        mr_id: label_ids
        for mr_id, label_ids in plan.mr_label_ids_by_gitlab_mr_id.items()
        if mr_id in selected_mr_ids
    }
    referenced_label_ids: set[int] = set()
    for label_ids in issue_label_ids_by_gitlab_issue_id.values():
        referenced_label_ids.update(label_ids)
    for label_ids in mr_label_ids_by_gitlab_mr_id.values():
        referenced_label_ids.update(label_ids)

    labels = [
        label for label in plan.labels if label.gitlab_label_id in referenced_label_ids
    ]

    member_usernames: set[str] = set()
    for members in org_members.values():
        member_usernames.update(members.keys())

    interacting_user_ids: set[int] = set()
    for issue in issues:
        interacting_user_ids.add(issue.author_id)
    for mr in merge_requests:
        interacting_user_ids.add(mr.author_id)
    for note in notes:
        interacting_user_ids.add(note.author_id)

    users = [
        u
        for u in plan.users
        if u.gitlab_user_id in interacting_user_ids or u.username in member_usernames
    ]

    issue_label_ids_by_gitlab_issue_id_sorted = {
        issue_id: issue_label_ids_by_gitlab_issue_id[issue_id]
        for issue_id in sorted(issue_label_ids_by_gitlab_issue_id)
    }
    mr_label_ids_by_gitlab_mr_id_sorted = {
        mr_id: mr_label_ids_by_gitlab_mr_id[mr_id]
        for mr_id in sorted(mr_label_ids_by_gitlab_mr_id)
    }

    return Plan(
        backup_id=plan.backup_id,
        orgs=sorted(orgs, key=lambda o: o.name),
        repos=sorted(repos, key=lambda r: (r.owner, r.name)),
        users=sorted(users, key=lambda u: u.username),
        org_members=org_members,
        issues=sorted(issues, key=lambda i: (i.gitlab_project_id, i.gitlab_issue_iid)),
        merge_requests=sorted(
            merge_requests, key=lambda mr: (mr.gitlab_target_project_id, mr.gitlab_mr_iid)
        ),
        notes=sorted(notes, key=lambda n: (n.gitlab_project_id, n.noteable_type, n.noteable_id)),
        uploads_tar_path=plan.uploads_tar_path,
        labels=sorted(labels, key=lambda label: (label.title.lower(), label.gitlab_label_id)),
        issue_label_ids_by_gitlab_issue_id=issue_label_ids_by_gitlab_issue_id_sorted,
        mr_label_ids_by_gitlab_mr_id=mr_label_ids_by_gitlab_mr_id_sorted,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gitlab-to-forgejo")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate = sub.add_parser("migrate", help="Migrate GitLab backup into Forgejo")
    migrate.add_argument(
        "--backup", type=Path, default=_default_backup_root(), help="GitLab backup root"
    )
    migrate.add_argument(
        "--root-group",
        default=os.environ.get("GITLAB_ROOT_GROUP", "pleroma"),
        help="Root GitLab group path to migrate (default: pleroma)",
    )
    migrate.add_argument(
        "--forgejo-url",
        default=_default_forgejo_url(),
        help="Forgejo base URL (default: FORGEJO_HTTP or http://localhost:$FORGEJO_HTTP_PORT)",
    )
    migrate.add_argument(
        "--only-repo",
        default=os.environ.get("GITLAB_ONLY_REPO"),
        help=(
            "Restrict migration to a single repo for faster iteration. Accepts either "
            "'<owner>/<repo>' (Forgejo-style) or '<group>/<subgroup>/<repo>' (GitLab-style; "
            "groups are flattened with '-')."
        ),
    )

    token_group = migrate.add_mutually_exclusive_group()
    token_group.add_argument("--token", default=os.environ.get("FORGEJO_TOKEN"))
    token_group.add_argument("--token-file", type=Path, default=_default_token_file())

    migrate.add_argument(
        "--user-password",
        default=os.environ.get("FORGEJO_DEFAULT_USER_PASSWORD", "temp1234"),
        help="Password assigned to newly created users (local/dev only)",
    )
    migrate.add_argument(
        "--git-username",
        default=_default_git_username(),
        help=(
            "Forgejo username for HTTP git push (default: FORGEJO_GIT_USERNAME or "
            "FORGEJO_ADMIN_USERNAME)"
        ),
    )
    migrate.add_argument(
        "--private-repos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create repositories as private (default: true)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "migrate":
        backup_root: Path = args.backup.expanduser()
        token: str = args.token or _read_token_file(args.token_file.expanduser())

        plan = build_plan(backup_root, root_group_path=args.root_group)
        if args.only_repo:
            plan = _filter_plan_to_single_repo(plan, only_repo=args.only_repo)
        client = ForgejoClient(base_url=args.forgejo_url, token=token)
        migrate_plan(
            plan,
            client,
            user_password=args.user_password,
            private_repos=args.private_repos,
            forgejo_url=args.forgejo_url,
            git_username=args.git_username,
            git_token=token,
        )
        return 0

    raise AssertionError(f"unhandled command: {args.command!r}")
