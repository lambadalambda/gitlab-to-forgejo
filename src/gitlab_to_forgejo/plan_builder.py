from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from gitlab_to_forgejo.copy_parser import iter_copy_rows


@dataclass(frozen=True)
class OrgPlan:
    name: str
    full_path: str
    gitlab_namespace_id: int
    description: str | None


@dataclass(frozen=True)
class RepoPlan:
    owner: str
    name: str
    gitlab_project_id: int
    bundle_path: Path
    refs_path: Path
    wiki_bundle_path: Path
    wiki_refs_path: Path


@dataclass(frozen=True)
class UserPlan:
    gitlab_user_id: int
    username: str
    email: str
    full_name: str
    state: str
    avatar: str | None = None


@dataclass(frozen=True)
class LabelPlan:
    gitlab_label_id: int
    title: str
    color: str
    description: str


@dataclass(frozen=True)
class IssuePlan:
    gitlab_issue_id: int
    gitlab_issue_iid: int
    gitlab_project_id: int
    title: str
    description: str
    author_id: int
    state_id: int = 0
    created_unix: int = 0
    updated_unix: int = 0
    closed_unix: int = 0


@dataclass(frozen=True)
class MergeRequestPlan:
    gitlab_mr_id: int
    gitlab_mr_iid: int
    gitlab_target_project_id: int
    source_branch: str
    target_branch: str
    title: str
    description: str
    author_id: int
    gitlab_source_project_id: int | None = None
    state_id: int = 0
    head_commit_sha: str = ""
    created_unix: int = 0
    updated_unix: int = 0
    closed_unix: int = 0


@dataclass(frozen=True)
class NotePlan:
    gitlab_note_id: int
    gitlab_project_id: int
    noteable_type: str
    noteable_id: int
    author_id: int
    body: str
    created_unix: int = 0
    updated_unix: int = 0


@dataclass(frozen=True)
class Plan:
    backup_id: str
    orgs: list[OrgPlan]
    repos: list[RepoPlan]
    users: list[UserPlan]
    org_members: dict[str, dict[str, int]]
    issues: list[IssuePlan]
    merge_requests: list[MergeRequestPlan]
    notes: list[NotePlan]
    uploads_tar_path: Path | None = None
    labels: list[LabelPlan] = field(default_factory=list)
    issue_label_ids_by_gitlab_issue_id: dict[int, tuple[int, ...]] = field(default_factory=dict)
    mr_label_ids_by_gitlab_mr_id: dict[int, tuple[int, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class _GroupNamespace:
    id: int
    name: str
    path: str
    parent_id: int | None
    traversal_ids: tuple[int, ...]
    description: str | None


_BACKUP_ID_RE = re.compile(r"^:backup_id:\s*(\S+)\s*$", re.MULTILINE)
_TZ_OFFSET_NO_COLON_RE = re.compile(r"([+-]\d{2})(\d{2})$")
_TZ_OFFSET_HOURS_ONLY_RE = re.compile(r"[+-]\d{2}$")


def _read_backup_id(backup_root: Path) -> str:
    info_path = backup_root / "backup_information.yml"
    m = _BACKUP_ID_RE.search(info_path.read_text(encoding="utf-8", errors="replace"))
    if not m:
        raise ValueError(f"could not find :backup_id: in {info_path}")
    return m.group(1)


def _parse_pg_int_array(raw: str | None) -> tuple[int, ...]:
    if raw is None or raw == "{}" or raw == "":
        return ()
    if not (raw.startswith("{") and raw.endswith("}")):
        raise ValueError(f"unexpected array literal: {raw!r}")
    inner = raw[1:-1]
    if not inner:
        return ()
    return tuple(int(part) for part in inner.split(",") if part)


def _parse_timestamp_unix(raw: str | None) -> int:
    if raw is None:
        return 0
    value = raw.strip()
    if not value:
        return 0

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    elif _TZ_OFFSET_NO_COLON_RE.search(value):
        value = value[:-2] + ":" + value[-2:]
    elif _TZ_OFFSET_HOURS_ONLY_RE.search(value):
        value = value + ":00"

    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return int(parsed.timestamp())


def _find_root_group_id(groups: dict[int, _GroupNamespace], root_group_path: str) -> int:
    candidates = [g for g in groups.values() if g.path == root_group_path]
    if not candidates:
        raise ValueError(f"no group namespace found with path={root_group_path!r}")
    # Prefer the top-level group (no parent). Fall back to lowest id.
    candidates.sort(key=lambda g: (g.parent_id is not None, g.id))
    return candidates[0].id


def _is_descendant(
    groups: dict[int, _GroupNamespace], *, root_id: int, group: _GroupNamespace
) -> bool:
    if group.id == root_id:
        return True
    if group.traversal_ids:
        return root_id in group.traversal_ids

    cur = group.parent_id
    while cur is not None:
        if cur == root_id:
            return True
        parent = groups.get(cur)
        if parent is None:
            break
        cur = parent.parent_id
    return False


def _full_group_path(groups: dict[int, _GroupNamespace], group_id: int) -> str:
    group = groups[group_id]
    if group.parent_id is None or group.parent_id not in groups:
        return group.path
    return f"{_full_group_path(groups, group.parent_id)}/{group.path}"


def build_plan(backup_root: Path, *, root_group_path: str) -> Plan:
    backup_id = _read_backup_id(backup_root)

    db_path_gz = backup_root / "db/database.sql.gz"
    db_path_plain = backup_root / "db/database.sql"
    if db_path_gz.exists():
        db_path = db_path_gz
    elif db_path_plain.exists():
        db_path = db_path_plain
    else:
        raise FileNotFoundError("expected db/database.sql.gz (or db/database.sql)")

    shards: dict[int, str] = {}
    project_repos: dict[int, tuple[int, str]] = {}
    groups: dict[int, _GroupNamespace] = {}

    # pass 1: orgs + repos
    descendant_group_ids: set[int] | None = None
    orgs: list[OrgPlan] = []
    org_name_by_ns_id: dict[int, str] = {}
    repos: list[RepoPlan] = []
    selected_project_ids: set[int] = set()

    for table, row in iter_copy_rows(
        db_path, tables={"shards", "project_repositories", "namespaces", "projects"}
    ):
        if table == "shards":
            shards[int(row["id"])] = row["name"] or ""
        elif table == "project_repositories":
            project_id = int(row["project_id"])
            shard_id = int(row["shard_id"])
            disk_path = row["disk_path"] or ""
            project_repos[project_id] = (shard_id, disk_path)
        elif table == "namespaces":
            if row["type"] != "Group":
                continue
            ns_id = int(row["id"])
            parent_id = int(row["parent_id"]) if row["parent_id"] is not None else None
            traversal_ids = _parse_pg_int_array(row.get("traversal_ids"))
            groups[ns_id] = _GroupNamespace(
                id=ns_id,
                name=row["name"] or "",
                path=row["path"] or "",
                parent_id=parent_id,
                traversal_ids=traversal_ids,
                description=row.get("description"),
            )
        elif table == "projects":
            if descendant_group_ids is None:
                root_id = _find_root_group_id(groups, root_group_path)
                descendant_group_ids = {
                    gid
                    for gid, g in groups.items()
                    if _is_descendant(groups, root_id=root_id, group=g)
                }
                for gid in sorted(descendant_group_ids, key=lambda i: _full_group_path(groups, i)):
                    full_path = _full_group_path(groups, gid)
                    org_name = full_path.replace("/", "-")
                    org_name_by_ns_id[gid] = org_name
                    g = groups[gid]
                    orgs.append(
                        OrgPlan(
                            name=org_name,
                            full_path=full_path,
                            gitlab_namespace_id=gid,
                            description=g.description,
                        )
                    )

            assert descendant_group_ids is not None
            namespace_id = int(row["namespace_id"])
            if namespace_id not in descendant_group_ids:
                continue

            project_id = int(row["id"])
            selected_project_ids.add(project_id)

            shard_id, disk_path = project_repos[project_id]
            storage = shards.get(shard_id, "default")
            bundle_path = (
                backup_root
                / "repositories"
                / storage
                / f"{disk_path}.git"
                / backup_id
                / "001.bundle"
            )
            refs_path = bundle_path.with_suffix(".refs")
            wiki_bundle_path = (
                backup_root
                / "repositories"
                / storage
                / f"{disk_path}.wiki.git"
                / backup_id
                / "001.bundle"
            )
            wiki_refs_path = wiki_bundle_path.with_suffix(".refs")

            repos.append(
                RepoPlan(
                    owner=org_name_by_ns_id[namespace_id],
                    name=row["path"] or "",
                    gitlab_project_id=project_id,
                    bundle_path=bundle_path,
                    refs_path=refs_path,
                    wiki_bundle_path=wiki_bundle_path,
                    wiki_refs_path=wiki_refs_path,
                )
            )

    if descendant_group_ids is None:
        raise ValueError("did not find any projects; unable to derive descendant group set")

    # pass 2: members/issues/MRs/notes/users
    direct_members: dict[int, dict[int, int]] = {gid: {} for gid in descendant_group_ids}
    interacting_user_ids: set[int] = set()

    issue_project_by_issue_id: dict[int, int] = {}
    target_project_by_mr_id: dict[int, int] = {}
    issues: list[IssuePlan] = []
    merge_request_rows: list[
        tuple[int, int, int, int | None, str, str, str, str, int, int, int | None, int, int, int]
    ] = []
    merge_request_diff_ids: set[int] = set()
    notes: list[NotePlan] = []
    users_by_id: dict[int, UserPlan] = {}
    labels_by_id: dict[int, LabelPlan] = {}

    for table, row in iter_copy_rows(
        db_path, tables={"members", "issues", "merge_requests", "notes", "users", "labels"}
    ):
        if table == "members":
            if row["source_type"] != "Namespace":
                continue
            source_id = int(row["source_id"])
            if source_id not in descendant_group_ids:
                continue
            if row["user_id"] is None:
                # Invited members (email-only) and other non-user rows.
                continue
            if row["access_level"] is None:
                continue
            user_id = int(row["user_id"])
            access_level = int(row["access_level"])
            cur = direct_members[source_id].get(user_id)
            direct_members[source_id][user_id] = (
                access_level if cur is None else max(cur, access_level)
            )
            interacting_user_ids.add(user_id)
        elif table == "issues":
            issue_id = int(row["id"])
            project_id = int(row["project_id"])
            if project_id not in selected_project_ids:
                continue
            author_id = int(row["author_id"])
            issue_project_by_issue_id[issue_id] = project_id
            interacting_user_ids.add(author_id)
            issues.append(
                IssuePlan(
                    gitlab_issue_id=issue_id,
                    gitlab_issue_iid=int(row["iid"]),
                    gitlab_project_id=project_id,
                    title=row["title"] or "",
                    description=row["description"] or "",
                    author_id=author_id,
                    state_id=int(row.get("state_id") or 0),
                    created_unix=_parse_timestamp_unix(row.get("created_at")),
                    updated_unix=_parse_timestamp_unix(row.get("updated_at")),
                    closed_unix=_parse_timestamp_unix(row.get("closed_at")),
                )
            )
        elif table == "merge_requests":
            mr_id = int(row["id"])
            target_project_id = int(row["target_project_id"])
            if target_project_id not in selected_project_ids:
                continue
            author_id = int(row["author_id"])
            target_project_by_mr_id[mr_id] = target_project_id
            interacting_user_ids.add(author_id)
            latest_diff_id_raw = row.get("latest_merge_request_diff_id")
            latest_diff_id = int(latest_diff_id_raw) if latest_diff_id_raw is not None else None
            if latest_diff_id is not None:
                merge_request_diff_ids.add(latest_diff_id)

            source_project_id_raw = row.get("source_project_id")
            source_project_id = (
                int(source_project_id_raw) if source_project_id_raw is not None else None
            )
            state_id = int(row.get("state_id") or 0)
            created_unix = _parse_timestamp_unix(row.get("created_at"))
            updated_unix = _parse_timestamp_unix(row.get("updated_at"))
            closed_unix = _parse_timestamp_unix(row.get("closed_at")) or _parse_timestamp_unix(
                row.get("merged_at")
            )

            merge_request_rows.append(
                (
                    mr_id,
                    int(row["iid"]),
                    target_project_id,
                    source_project_id,
                    row["source_branch"] or "",
                    row["target_branch"] or "",
                    row["title"] or "",
                    row["description"] or "",
                    author_id,
                    state_id,
                    latest_diff_id,
                    created_unix,
                    updated_unix,
                    closed_unix,
                )
            )
        elif table == "notes":
            if row["system"] == "t":
                continue
            noteable_type = row["noteable_type"] or ""
            if noteable_type not in {"Issue", "MergeRequest"}:
                continue
            if row["author_id"] is None or row["noteable_id"] is None or row["id"] is None:
                continue
            noteable_id = int(row["noteable_id"])

            project_id_raw = row.get("project_id")
            project_id = int(project_id_raw) if project_id_raw is not None else None
            if project_id is None and noteable_type == "Issue":
                project_id = issue_project_by_issue_id.get(noteable_id)
            if project_id is None and noteable_type == "MergeRequest":
                project_id = target_project_by_mr_id.get(noteable_id)
            if project_id is None or project_id not in selected_project_ids:
                continue

            author_id = int(row["author_id"])
            interacting_user_ids.add(author_id)
            notes.append(
                NotePlan(
                    gitlab_note_id=int(row["id"]),
                    gitlab_project_id=project_id,
                    noteable_type=noteable_type,
                    noteable_id=noteable_id,
                    author_id=author_id,
                    body=row["note"] or "",
                    created_unix=_parse_timestamp_unix(row.get("created_at")),
                    updated_unix=_parse_timestamp_unix(row.get("updated_at")),
                )
            )
        elif table == "users":
            user_id = int(row["id"])
            if user_id not in interacting_user_ids:
                continue
            users_by_id[user_id] = UserPlan(
                gitlab_user_id=user_id,
                username=row["username"] or "",
                email=row["email"] or "",
                full_name=row["name"] or "",
                state=row["state"] or "",
                avatar=row.get("avatar") or None,
            )
        elif table == "labels":
            label_id_raw = row.get("id")
            if label_id_raw is None:
                continue
            label_id = int(label_id_raw)
            title = row.get("title") or ""
            color = row.get("color") or ""
            description = row.get("description") or ""

            project_id_raw = row.get("project_id")
            group_id_raw = row.get("group_id")

            include = False
            if project_id_raw is not None:
                try:
                    include = int(project_id_raw) in selected_project_ids
                except ValueError:
                    include = False
            if not include and group_id_raw is not None:
                try:
                    include = int(group_id_raw) in descendant_group_ids
                except ValueError:
                    include = False
            if not include:
                continue

            labels_by_id[label_id] = LabelPlan(
                gitlab_label_id=label_id,
                title=title,
                color=color,
                description=description,
            )

    users = sorted(users_by_id.values(), key=lambda u: u.username)

    # pass 2.5: label_links (issue/MR label assignments)
    issue_label_ids: dict[int, list[int]] = {}
    mr_label_ids: dict[int, list[int]] = {}
    if issue_project_by_issue_id or target_project_by_mr_id:
        for _, row in iter_copy_rows(db_path, tables={"label_links"}):
            target_type = (row.get("target_type") or "").strip()
            if target_type not in {"Issue", "MergeRequest"}:
                continue

            target_id_raw = row.get("target_id")
            label_id_raw = row.get("label_id")
            if target_id_raw is None or label_id_raw is None:
                continue
            try:
                target_id = int(target_id_raw)
                label_id = int(label_id_raw)
            except ValueError:
                continue
            if label_id not in labels_by_id:
                continue

            if target_type == "Issue":
                if target_id not in issue_project_by_issue_id:
                    continue
                issue_label_ids.setdefault(target_id, []).append(label_id)
            else:  # MergeRequest
                if target_id not in target_project_by_mr_id:
                    continue
                mr_label_ids.setdefault(target_id, []).append(label_id)

    issue_label_ids_by_gitlab_issue_id = {
        issue_id: tuple(sorted(set(label_ids)))
        for issue_id, label_ids in sorted(issue_label_ids.items())
    }
    mr_label_ids_by_gitlab_mr_id = {
        mr_id: tuple(sorted(set(label_ids)))
        for mr_id, label_ids in sorted(mr_label_ids.items())
    }

    labels = sorted(
        labels_by_id.values(),
        key=lambda label: (label.title.lower(), label.gitlab_label_id),
    )

    # pass 3: merge_request_diffs (head commit SHAs)
    head_sha_by_diff_id: dict[int, str] = {}
    if merge_request_diff_ids:
        for _, row in iter_copy_rows(db_path, tables={"merge_request_diffs"}):
            diff_id_raw = row.get("id")
            if diff_id_raw is None:
                continue
            diff_id = int(diff_id_raw)
            if diff_id not in merge_request_diff_ids:
                continue
            head_sha_by_diff_id[diff_id] = row.get("head_commit_sha") or ""
            if len(head_sha_by_diff_id) >= len(merge_request_diff_ids):
                break

    merge_requests: list[MergeRequestPlan] = []
    for (
        mr_id,
        mr_iid,
        target_project_id,
        source_project_id,
        source_branch,
        target_branch,
        title,
        description,
        author_id,
        state_id,
        latest_diff_id,
        created_unix,
        updated_unix,
        closed_unix,
    ) in merge_request_rows:
        head_commit_sha = (
            head_sha_by_diff_id.get(latest_diff_id, "") if latest_diff_id is not None else ""
        )
        merge_requests.append(
            MergeRequestPlan(
                gitlab_mr_id=mr_id,
                gitlab_mr_iid=mr_iid,
                gitlab_target_project_id=target_project_id,
                source_branch=source_branch,
                target_branch=target_branch,
                title=title,
                description=description,
                author_id=author_id,
                gitlab_source_project_id=source_project_id,
                state_id=state_id,
                head_commit_sha=head_commit_sha,
                created_unix=created_unix,
                updated_unix=updated_unix,
                closed_unix=closed_unix,
            )
        )

    # Compute effective membership for each org: direct + ancestor memberships.
    def iter_group_ancestors(group_id: int) -> Iterable[int]:
        cur: int | None = group_id
        while cur is not None:
            yield cur
            parent = groups.get(cur)
            if parent is None:
                break
            cur = parent.parent_id
            if cur is not None and cur not in groups:
                break

    org_members: dict[str, dict[str, int]] = {}
    for gid in descendant_group_ids:
        effective: dict[int, int] = {}
        for ancestor_id in iter_group_ancestors(gid):
            for uid, lvl in direct_members.get(ancestor_id, {}).items():
                cur = effective.get(uid)
                effective[uid] = lvl if cur is None else max(cur, lvl)

        by_username: dict[str, int] = {}
        for uid, lvl in effective.items():
            user = users_by_id.get(uid)
            if user is None:
                continue
            by_username[user.username] = lvl

        org_members[org_name_by_ns_id[gid]] = by_username

    uploads_tar_path = backup_root / "uploads.tar.gz"
    if not uploads_tar_path.exists():
        uploads_tar_path = None

    return Plan(
        backup_id=backup_id,
        orgs=sorted(orgs, key=lambda o: o.name),
        repos=sorted(repos, key=lambda r: (r.owner, r.name)),
        users=users,
        org_members=org_members,
        issues=sorted(issues, key=lambda i: (i.gitlab_project_id, i.gitlab_issue_iid)),
        merge_requests=sorted(
            merge_requests, key=lambda mr: (mr.gitlab_target_project_id, mr.gitlab_mr_iid)
        ),
        notes=sorted(notes, key=lambda n: (n.gitlab_project_id, n.noteable_type, n.noteable_id)),
        uploads_tar_path=uploads_tar_path,
        labels=labels,
        issue_label_ids_by_gitlab_issue_id=issue_label_ids_by_gitlab_issue_id,
        mr_label_ids_by_gitlab_mr_id=mr_label_ids_by_gitlab_mr_id,
    )
