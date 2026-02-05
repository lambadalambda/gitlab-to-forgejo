from __future__ import annotations

import base64
import re
import time
from collections.abc import Mapping
from typing import Protocol

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.forgejo_db import apply_metadata_fix_sql, build_metadata_fix_sql
from gitlab_to_forgejo.forgejo_wiki import ensure_wiki_repo_exists
from gitlab_to_forgejo.git_push import push_bundle_http
from gitlab_to_forgejo.git_refs import guess_default_branch, list_wiki_push_refspecs, read_ref_shas
from gitlab_to_forgejo.gitlab_uploads import (
    GitLabProjectUpload,
    iter_gitlab_upload_urls,
    read_project_uploads_from_uploads,
    read_user_avatars_from_uploads,
    replace_gitlab_upload_urls,
)
from gitlab_to_forgejo.plan_builder import Plan


class _ForgejoOps(Protocol):
    def ensure_user(self, *, username: str, email: str, full_name: str, password: str) -> None: ...

    def update_user_avatar(self, *, image_b64: str, sudo: str | None) -> None: ...

    def ensure_org(self, *, org: str, full_name: str, description: str | None) -> None: ...

    def get_owner_team_id(self, org: str) -> int: ...

    def ensure_team(
        self,
        *,
        org: str,
        name: str,
        permission: str,
        includes_all_repositories: bool,
    ) -> int: ...

    def add_team_member(self, *, team_id: int, username: str) -> None: ...


class _ForgejoRepoOps(_ForgejoOps, Protocol):
    def ensure_org_repo(
        self,
        *,
        org: str,
        name: str,
        private: bool,
        default_branch: str | None,
    ) -> None: ...

    def create_issue(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def create_pull_request(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def create_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def edit_issue_body(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def edit_pull_request_body(
        self,
        *,
        owner: str,
        repo: str,
        pr_number: int,
        body: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def edit_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        body: str,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def create_issue_attachment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        filename: str,
        content: bytes,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def create_issue_comment_attachment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        filename: str,
        content: bytes,
        sudo: str | None,
    ) -> Mapping[str, object]: ...

    def list_repo_labels(self, *, owner: str, repo: str) -> list[Mapping[str, object]]: ...

    def create_repo_label(
        self,
        *,
        owner: str,
        repo: str,
        name: str,
        color: str,
        description: str,
    ) -> Mapping[str, object]: ...

    def replace_issue_labels(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        labels: list[str],
        sudo: str | None,
    ) -> list[Mapping[str, object]]: ...


def _iter_members_by_level(
    members: Mapping[str, int],
) -> tuple[list[str], list[str], list[str], list[str]]:
    owners: list[str] = []
    maintainers: list[str] = []
    developers: list[str] = []
    reporters: list[str] = []

    for username, lvl in members.items():
        if lvl >= 50:
            owners.append(username)
        elif lvl >= 40:
            maintainers.append(username)
        elif lvl >= 30:
            developers.append(username)
        else:
            reporters.append(username)

    owners.sort()
    maintainers.sort()
    developers.sort()
    reporters.sort()
    return owners, maintainers, developers, reporters


_NON_USERNAME_CHARS_RE = re.compile(r"[^a-z0-9_.-]+", re.IGNORECASE)


def _fallback_username(gitlab_username: str, gitlab_user_id: int) -> str:
    base = _NON_USERNAME_CHARS_RE.sub("-", gitlab_username).strip("._-").lower()
    if not base:
        base = "user"

    prefix = "gitlab-"
    suffix = f"-{gitlab_user_id}"
    max_total = 39
    max_base = max_total - len(prefix) - len(suffix)
    if max_base < 1:
        max_base = 1
    base = base[:max_base]
    return f"{prefix}{base}{suffix}"


def _is_username_creation_error(err: ForgejoError) -> bool:
    if err.status_code != 422:
        return False
    msg = err.body.lower()
    return ("reserved" in msg or "invalid" in msg) and ("name" in msg or "username" in msg)


def _is_transient_target_not_found(err: ForgejoError) -> bool:
    if err.status_code != 404:
        return False
    msg = err.body.lower().replace(" ", "")
    return (
        ("targetcouldn'tbefound" in msg or "targetcouldn\\u0027tbefound" in msg)
        and '"errors":[]' in msg
    )


def _is_no_changes_between_head_and_base(err: ForgejoError) -> bool:
    if err.status_code != 422:
        return False
    msg = " ".join(err.body.lower().split())
    return "no changes between the head and the base" in msg


def apply_plan(plan: Plan, client: _ForgejoOps, *, user_password: str) -> dict[str, str]:
    forgejo_username_by_gitlab_username: dict[str, str] = {}

    for user in plan.users:
        try:
            client.ensure_user(
                username=user.username,
                email=user.email,
                full_name=user.full_name,
                password=user_password,
            )
            forgejo_username_by_gitlab_username[user.username] = user.username
        except ForgejoError as err:
            if not _is_username_creation_error(err):
                raise
            fallback = _fallback_username(user.username, user.gitlab_user_id)
            client.ensure_user(
                username=fallback,
                email=user.email,
                full_name=user.full_name,
                password=user_password,
            )
            forgejo_username_by_gitlab_username[user.username] = fallback

    forgejo_user_by_gitlab_user_id: dict[int, str] = {}
    for user in plan.users:
        forgejo_username = forgejo_username_by_gitlab_username.get(user.username)
        if forgejo_username:
            forgejo_user_by_gitlab_user_id[user.gitlab_user_id] = forgejo_username

    org_by_project_id = {r.gitlab_project_id: r.owner for r in plan.repos}
    extra_members_by_org: dict[str, set[str]] = {o.name: set() for o in plan.orgs}

    def add_interactor(project_id: int, user_id: int) -> None:
        org = org_by_project_id.get(project_id)
        if not org:
            return
        username = forgejo_user_by_gitlab_user_id.get(user_id)
        if not username:
            return
        extra_members_by_org.setdefault(org, set()).add(username)

    for issue in plan.issues:
        add_interactor(issue.gitlab_project_id, issue.author_id)
    for mr in plan.merge_requests:
        add_interactor(mr.gitlab_target_project_id, mr.author_id)
    for note in plan.notes:
        add_interactor(note.gitlab_project_id, note.author_id)

    for org in plan.orgs:
        client.ensure_org(org=org.name, full_name=org.full_path, description=org.description)

    for org in plan.orgs:
        members = plan.org_members.get(org.name, {})

        mapped_members: dict[str, int] = {}
        for gitlab_username, lvl in members.items():
            forgejo_username = forgejo_username_by_gitlab_username.get(gitlab_username)
            if not forgejo_username:
                continue
            mapped_members[forgejo_username] = lvl

        owners, maintainers, developers, reporters = _iter_members_by_level(mapped_members)
        extra_reporters = extra_members_by_org.get(org.name, set()) - set(mapped_members)
        if extra_reporters:
            reporters = sorted(set(reporters) | extra_reporters)

        if owners:
            owner_team_id = client.get_owner_team_id(org.name)
            for username in owners:
                client.add_team_member(team_id=owner_team_id, username=username)

        if maintainers:
            team_id = client.ensure_team(
                org=org.name,
                name="Maintainers",
                permission="admin",
                includes_all_repositories=True,
            )
            for username in maintainers:
                client.add_team_member(team_id=team_id, username=username)

        if developers:
            team_id = client.ensure_team(
                org=org.name,
                name="Developers",
                permission="write",
                includes_all_repositories=True,
            )
            for username in developers:
                client.add_team_member(team_id=team_id, username=username)

        if reporters:
            team_id = client.ensure_team(
                org=org.name,
                name="Reporters",
                permission="read",
                includes_all_repositories=True,
            )
            for username in reporters:
                client.add_team_member(team_id=team_id, username=username)

    return forgejo_username_by_gitlab_username


def apply_repos(plan: Plan, client: _ForgejoRepoOps, *, private: bool) -> None:
    for repo in plan.repos:
        try:
            default_branch = guess_default_branch(repo.refs_path)
        except (FileNotFoundError, ValueError):
            default_branch = None
        client.ensure_org_repo(
            org=repo.owner,
            name=repo.name,
            private=private,
            default_branch=default_branch,
        )


def push_repos(plan: Plan, *, forgejo_url: str, git_username: str, git_token: str) -> None:
    base = forgejo_url.rstrip("/")
    for repo in plan.repos:
        push_bundle_http(
            bundle_path=repo.bundle_path,
            refs_path=repo.refs_path,
            remote_url=f"{base}/{repo.owner}/{repo.name}.git",
            username=git_username,
            token=git_token,
        )


def push_wikis(plan: Plan, *, forgejo_url: str, git_username: str, git_token: str) -> None:
    base = forgejo_url.rstrip("/")
    for repo in plan.repos:
        refspecs = list_wiki_push_refspecs(repo.wiki_refs_path)
        if not refspecs or not repo.wiki_bundle_path.exists():
            continue
        ensure_wiki_repo_exists(owner=repo.owner, repo=repo.name)
        push_bundle_http(
            bundle_path=repo.wiki_bundle_path,
            refs_path=repo.wiki_refs_path,
            remote_url=f"{base}/{repo.owner}/{repo.name}.wiki.git",
            username=git_username,
            token=git_token,
            refspecs=refspecs,
        )


def push_merge_request_heads(
    plan: Plan, *, forgejo_url: str, git_username: str, git_token: str
) -> None:
    """Create synthetic branches in Forgejo for merged GitLab MRs missing their source branches.

    Forgejo's PR API requires a head branch name; it does not accept raw commit SHAs as `head`.
    For merged MRs where the original source branch no longer exists, create a deterministic
    branch `gitlab-mr-iid-<iid>` pointing at the MR head commit SHA.
    """
    base = forgejo_url.rstrip("/")
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    refs_by_project_id: dict[int, dict[str, str]] = {}
    refspecs_by_project_id: dict[int, list[str]] = {}

    for mr in plan.merge_requests:
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            continue

        refs = refs_by_project_id.get(repo.gitlab_project_id)
        if refs is None:
            try:
                refs = read_ref_shas(repo.refs_path)
            except FileNotFoundError:
                refs = {}
            refs_by_project_id[repo.gitlab_project_id] = refs

        if f"refs/heads/{mr.source_branch}" in refs:
            continue
        if mr.state_id != 3:
            continue

        sha = mr.head_commit_sha
        if not sha:
            mr_ref = f"refs/merge-requests/{mr.gitlab_mr_iid}/head"
            sha = refs.get(mr_ref, "")
        if not sha:
            continue

        branch_name = f"gitlab-mr-iid-{mr.gitlab_mr_iid}"
        refspec = f"{sha}:refs/heads/{branch_name}"
        refspecs_by_project_id.setdefault(repo.gitlab_project_id, []).append(refspec)

    for project_id, refspecs in refspecs_by_project_id.items():
        repo = repo_by_project_id[project_id]
        push_bundle_http(
            bundle_path=repo.bundle_path,
            refs_path=repo.refs_path,
            remote_url=f"{base}/{repo.owner}/{repo.name}.git",
            username=git_username,
            token=git_token,
            refspecs=sorted(set(refspecs)),
        )


def apply_issues(
    plan: Plan, client: _ForgejoRepoOps, *, user_by_id: Mapping[int, str]
) -> dict[int, int]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    issue_number_by_gitlab_issue_id: dict[int, int] = {}

    for issue in plan.issues:
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            raise ValueError(f"no repo found for issue project_id={issue.gitlab_project_id}")

        resp = client.create_issue(
            owner=repo.owner,
            repo=repo.name,
            title=issue.title,
            body=issue.description,
            sudo=user_by_id.get(issue.author_id),
        )
        number = int(resp["number"])
        issue_number_by_gitlab_issue_id[issue.gitlab_issue_id] = number

    return issue_number_by_gitlab_issue_id


def apply_merge_requests(
    plan: Plan, client: _ForgejoRepoOps, *, user_by_id: Mapping[int, str]
) -> dict[int, int]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    pr_number_by_gitlab_mr_id: dict[int, int] = {}
    refs_by_project_id: dict[int, dict[str, str]] = {}

    for mr in plan.merge_requests:
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            raise ValueError(
                f"no repo found for mr target_project_id={mr.gitlab_target_project_id}"
            )

        refs = refs_by_project_id.get(repo.gitlab_project_id)
        if refs is None:
            try:
                refs = read_ref_shas(repo.refs_path)
            except FileNotFoundError:
                refs = {}
            refs_by_project_id[repo.gitlab_project_id] = refs

        source_branch_ref = f"refs/heads/{mr.source_branch}"
        if source_branch_ref in refs:
            head = mr.source_branch
        elif mr.state_id == 3 and (
            mr.head_commit_sha or refs.get(f"refs/merge-requests/{mr.gitlab_mr_iid}/head")
        ):
            head = f"gitlab-mr-iid-{mr.gitlab_mr_iid}"
        else:
            body = "\n".join(
                [
                    mr.description,
                    "",
                    (
                        f"_Imported from GitLab MR !{mr.gitlab_mr_iid} "
                        f"({mr.source_branch} → {mr.target_branch})_"
                    ),
                ]
            ).strip()
            resp = client.create_issue(
                owner=repo.owner,
                repo=repo.name,
                title=f"MR: {mr.title}",
                body=body,
                sudo=user_by_id.get(mr.author_id),
            )
            number = int(resp["number"])
            pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = number
            continue

        delays = (0.2, 0.5, 1.0)
        resp: Mapping[str, object] | None = None
        created_issue = False
        for attempt in range(len(delays) + 1):
            try:
                resp = client.create_pull_request(
                    owner=repo.owner,
                    repo=repo.name,
                    title=mr.title,
                    body=mr.description,
                    head=head,
                    base=mr.target_branch,
                    sudo=user_by_id.get(mr.author_id),
                )
                break
            except ForgejoError as err:
                if _is_no_changes_between_head_and_base(err):
                    issue_body = "\n".join(
                        [
                            mr.description,
                            "",
                            (
                                f"_Imported from GitLab MR !{mr.gitlab_mr_iid} "
                                f"({mr.source_branch} → {mr.target_branch})_"
                            ),
                            "",
                            (
                                "_Forgejo pull request not created because there are no changes "
                                "between the head and base._"
                            ),
                        ]
                    ).strip()
                    issue_resp = client.create_issue(
                        owner=repo.owner,
                        repo=repo.name,
                        title=f"MR: {mr.title}",
                        body=issue_body,
                        sudo=user_by_id.get(mr.author_id),
                    )
                    pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = int(issue_resp["number"])
                    created_issue = True
                    break
                if not _is_transient_target_not_found(err) or attempt >= len(delays):
                    raise
                time.sleep(delays[attempt])

        if created_issue:
            continue

        assert resp is not None
        number = int(resp["number"])
        pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = number

    return pr_number_by_gitlab_mr_id


def apply_notes(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_by_id: Mapping[int, str],
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
) -> dict[int, int]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    comment_id_by_gitlab_note_id: dict[int, int] = {}

    for note in plan.notes:
        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None:
            raise ValueError(f"no repo found for note project_id={note.gitlab_project_id}")

        issue_number: int | None
        if note.noteable_type == "Issue":
            issue_number = issue_number_by_gitlab_issue_id.get(note.noteable_id)
        elif note.noteable_type == "MergeRequest":
            issue_number = pr_number_by_gitlab_mr_id.get(note.noteable_id)
        else:
            continue

        if issue_number is None:
            continue

        resp = client.create_issue_comment(
            owner=repo.owner,
            repo=repo.name,
            issue_number=issue_number,
            body=note.body,
            sudo=user_by_id.get(note.author_id),
        )
        comment_id_raw = resp.get("id")
        if comment_id_raw is not None:
            comment_id_by_gitlab_note_id[note.gitlab_note_id] = int(comment_id_raw)

    return comment_id_by_gitlab_note_id


def apply_issue_and_pr_uploads(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_by_id: Mapping[int, str],
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
    upload_bytes_by_upload: Mapping[GitLabProjectUpload, bytes],
) -> None:
    if not upload_bytes_by_upload:
        return

    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    for issue in plan.issues:
        issue_number = issue_number_by_gitlab_issue_id.get(issue.gitlab_issue_id)
        if issue_number is None:
            continue
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            raise ValueError(
                f"no repo found for issue uploads project_id={issue.gitlab_project_id}"
            )

        sudo = user_by_id.get(issue.author_id)
        mapping: dict[str, str] = {}
        seen_urls: set[str] = set()
        for url, upload_hash, filename in iter_gitlab_upload_urls(issue.description):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            upload = GitLabProjectUpload(
                disk_path=repo.gitlab_disk_path,
                upload_hash=upload_hash,
                filename=filename,
            )
            content = upload_bytes_by_upload.get(upload)
            if content is None:
                continue
            resp = client.create_issue_attachment(
                owner=repo.owner,
                repo=repo.name,
                issue_number=int(issue_number),
                filename=filename,
                content=content,
                sudo=sudo,
            )
            new_url = resp.get("browser_download_url")
            if new_url:
                mapping[url] = str(new_url)

        if not mapping:
            continue
        new_body = replace_gitlab_upload_urls(issue.description, mapping=mapping)
        if new_body == issue.description:
            continue
        client.edit_issue_body(
            owner=repo.owner,
            repo=repo.name,
            issue_number=int(issue_number),
            body=new_body,
            sudo=sudo,
        )

    for mr in plan.merge_requests:
        pr_number = pr_number_by_gitlab_mr_id.get(mr.gitlab_mr_id)
        if pr_number is None:
            continue
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            raise ValueError(
                f"no repo found for merge request uploads project_id={mr.gitlab_target_project_id}"
            )

        sudo = user_by_id.get(mr.author_id)
        mapping: dict[str, str] = {}
        seen_urls: set[str] = set()
        for url, upload_hash, filename in iter_gitlab_upload_urls(mr.description):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            upload = GitLabProjectUpload(
                disk_path=repo.gitlab_disk_path,
                upload_hash=upload_hash,
                filename=filename,
            )
            content = upload_bytes_by_upload.get(upload)
            if content is None:
                continue
            resp = client.create_issue_attachment(
                owner=repo.owner,
                repo=repo.name,
                issue_number=int(pr_number),
                filename=filename,
                content=content,
                sudo=sudo,
            )
            new_url = resp.get("browser_download_url")
            if new_url:
                mapping[url] = str(new_url)

        if not mapping:
            continue
        new_body = replace_gitlab_upload_urls(mr.description, mapping=mapping)
        if new_body == mr.description:
            continue
        try:
            client.edit_pull_request_body(
                owner=repo.owner,
                repo=repo.name,
                pr_number=int(pr_number),
                body=new_body,
                sudo=sudo,
            )
        except ForgejoError as err:
            # MRs imported as issues do not have a pull request to edit.
            if err.status_code == 404:
                continue
            raise


def apply_note_uploads(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_by_id: Mapping[int, str],
    comment_id_by_gitlab_note_id: Mapping[int, int],
    upload_bytes_by_upload: Mapping[GitLabProjectUpload, bytes],
) -> None:
    if not upload_bytes_by_upload:
        return

    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    for note in plan.notes:
        comment_id = comment_id_by_gitlab_note_id.get(note.gitlab_note_id)
        if comment_id is None:
            continue

        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None:
            raise ValueError(f"no repo found for note uploads project_id={note.gitlab_project_id}")

        sudo = user_by_id.get(note.author_id)
        attachment_sudo = sudo
        mapping: dict[str, str] = {}
        seen_urls: set[str] = set()
        for url, upload_hash, filename in iter_gitlab_upload_urls(note.body):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            upload = GitLabProjectUpload(
                disk_path=repo.gitlab_disk_path,
                upload_hash=upload_hash,
                filename=filename,
            )
            content = upload_bytes_by_upload.get(upload)
            if content is None:
                continue
            try:
                resp = client.create_issue_comment_attachment(
                    owner=repo.owner,
                    repo=repo.name,
                    comment_id=int(comment_id),
                    filename=filename,
                    content=content,
                    sudo=attachment_sudo,
                )
            except ForgejoError as err:
                if err.status_code != 403 or attachment_sudo is None:
                    raise
                attachment_sudo = None
                resp = client.create_issue_comment_attachment(
                    owner=repo.owner,
                    repo=repo.name,
                    comment_id=int(comment_id),
                    filename=filename,
                    content=content,
                    sudo=attachment_sudo,
                )
            new_url = resp.get("browser_download_url")
            if new_url:
                mapping[url] = str(new_url)

        if not mapping:
            continue
        new_body = replace_gitlab_upload_urls(note.body, mapping=mapping)
        if new_body == note.body:
            continue
        client.edit_issue_comment(
            owner=repo.owner,
            repo=repo.name,
            comment_id=int(comment_id),
            body=new_body,
            sudo=sudo,
        )


def collect_project_uploads(plan: Plan) -> set[GitLabProjectUpload]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    uploads: set[GitLabProjectUpload] = set()

    for issue in plan.issues:
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None or not repo.gitlab_disk_path:
            continue
        for _, upload_hash, filename in iter_gitlab_upload_urls(issue.description):
            uploads.add(
                GitLabProjectUpload(
                    disk_path=repo.gitlab_disk_path,
                    upload_hash=upload_hash,
                    filename=filename,
                )
            )

    for mr in plan.merge_requests:
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None or not repo.gitlab_disk_path:
            continue
        for _, upload_hash, filename in iter_gitlab_upload_urls(mr.description):
            uploads.add(
                GitLabProjectUpload(
                    disk_path=repo.gitlab_disk_path,
                    upload_hash=upload_hash,
                    filename=filename,
                )
            )

    for note in plan.notes:
        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None or not repo.gitlab_disk_path:
            continue
        for _, upload_hash, filename in iter_gitlab_upload_urls(note.body):
            uploads.add(
                GitLabProjectUpload(
                    disk_path=repo.gitlab_disk_path,
                    upload_hash=upload_hash,
                    filename=filename,
                )
            )

    return uploads


def apply_user_avatars(plan: Plan, client: _ForgejoOps, *, user_by_id: Mapping[int, str]) -> None:
    uploads = plan.uploads_tar_path
    if uploads is None:
        return

    desired = {u.gitlab_user_id: u.avatar for u in plan.users if u.avatar}
    if not desired:
        return

    avatar_bytes = read_user_avatars_from_uploads(uploads, desired=desired)
    for user_id, raw in sorted(avatar_bytes.items()):
        sudo = user_by_id.get(user_id)
        if not sudo:
            continue
        image_b64 = base64.b64encode(raw).decode("ascii")
        client.update_user_avatar(image_b64=image_b64, sudo=sudo)


def ensure_repo_labels(plan: Plan, client: _ForgejoRepoOps) -> None:
    label_by_id = {label.gitlab_label_id: label for label in plan.labels}
    if not label_by_id:
        return

    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    label_ids_by_project: dict[int, set[int]] = {}

    for issue in plan.issues:
        label_ids = plan.issue_label_ids_by_gitlab_issue_id.get(issue.gitlab_issue_id, ())
        if label_ids:
            label_ids_by_project.setdefault(issue.gitlab_project_id, set()).update(label_ids)

    for mr in plan.merge_requests:
        label_ids = plan.mr_label_ids_by_gitlab_mr_id.get(mr.gitlab_mr_id, ())
        if label_ids:
            label_ids_by_project.setdefault(mr.gitlab_target_project_id, set()).update(label_ids)

    for project_id, label_ids in sorted(label_ids_by_project.items()):
        repo = repo_by_project_id.get(project_id)
        if repo is None:
            raise ValueError(f"no repo found for labels project_id={project_id}")

        existing_by_name = {
            str(label_obj.get("name") or ""): label_obj
            for label_obj in client.list_repo_labels(owner=repo.owner, repo=repo.name)
        }

        def sort_key(label_id: int) -> tuple[str, int]:
            label = label_by_id.get(label_id)
            return ((label.title.lower() if label else ""), label_id)

        for label_id in sorted(label_ids, key=sort_key):
            label = label_by_id.get(label_id)
            if label is None or not label.title:
                continue
            if label.title in existing_by_name:
                continue
            client.create_repo_label(
                owner=repo.owner,
                repo=repo.name,
                name=label.title,
                color=label.color,
                description=label.description,
            )


def apply_issue_and_mr_labels(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
) -> None:
    label_by_id = {label.gitlab_label_id: label for label in plan.labels}
    if not label_by_id:
        return

    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    def label_names(label_ids: tuple[int, ...]) -> list[str]:
        names: list[str] = []
        for label_id in label_ids:
            label = label_by_id.get(label_id)
            if label and label.title:
                names.append(label.title)
        # Deterministic order + de-dupe.
        return sorted(set(names), key=str.lower)

    for issue in plan.issues:
        issue_number = issue_number_by_gitlab_issue_id.get(issue.gitlab_issue_id)
        if issue_number is None:
            continue
        label_ids = plan.issue_label_ids_by_gitlab_issue_id.get(issue.gitlab_issue_id)
        if not label_ids:
            continue
        names = label_names(label_ids)
        if not names:
            continue
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            raise ValueError(f"no repo found for issue labels project_id={issue.gitlab_project_id}")
        client.replace_issue_labels(
            owner=repo.owner,
            repo=repo.name,
            issue_number=issue_number,
            labels=names,
            sudo=None,
        )

    for mr in plan.merge_requests:
        pr_number = pr_number_by_gitlab_mr_id.get(mr.gitlab_mr_id)
        if pr_number is None:
            continue
        label_ids = plan.mr_label_ids_by_gitlab_mr_id.get(mr.gitlab_mr_id)
        if not label_ids:
            continue
        names = label_names(label_ids)
        if not names:
            continue
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            raise ValueError(
                f"no repo found for merge request labels project_id={mr.gitlab_target_project_id}"
            )
        client.replace_issue_labels(
            owner=repo.owner,
            repo=repo.name,
            issue_number=pr_number,
            labels=names,
            sudo=None,
        )


def migrate_plan(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_password: str,
    private_repos: bool,
    forgejo_url: str,
    git_username: str,
    git_token: str,
) -> None:
    forgejo_username_by_gitlab_username = apply_plan(plan, client, user_password=user_password)
    forgejo_user_by_gitlab_user_id: dict[int, str] = {}
    for u in plan.users:
        forgejo_username = forgejo_username_by_gitlab_username.get(u.username)
        if forgejo_username:
            forgejo_user_by_gitlab_user_id[u.gitlab_user_id] = forgejo_username

    upload_bytes_by_upload: dict[GitLabProjectUpload, bytes] = {}
    if plan.uploads_tar_path is not None:
        desired_uploads = collect_project_uploads(plan)
        if desired_uploads:
            upload_bytes_by_upload = read_project_uploads_from_uploads(
                plan.uploads_tar_path, desired=desired_uploads
            )

    apply_user_avatars(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)

    apply_repos(plan, client, private=private_repos)
    ensure_repo_labels(plan, client)
    push_repos(plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token)
    push_wikis(plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token)
    push_merge_request_heads(
        plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token
    )

    issue_numbers = apply_issues(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    pr_numbers = apply_merge_requests(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    comment_ids = apply_notes(
        plan,
        client,
        user_by_id=forgejo_user_by_gitlab_user_id,
        issue_number_by_gitlab_issue_id=issue_numbers,
        pr_number_by_gitlab_mr_id=pr_numbers,
    )
    apply_issue_and_pr_uploads(
        plan,
        client,
        user_by_id=forgejo_user_by_gitlab_user_id,
        issue_number_by_gitlab_issue_id=issue_numbers,
        pr_number_by_gitlab_mr_id=pr_numbers,
        upload_bytes_by_upload=upload_bytes_by_upload,
    )
    apply_note_uploads(
        plan,
        client,
        user_by_id=forgejo_user_by_gitlab_user_id,
        comment_id_by_gitlab_note_id=comment_ids,
        upload_bytes_by_upload=upload_bytes_by_upload,
    )
    apply_issue_and_mr_labels(
        plan,
        client,
        issue_number_by_gitlab_issue_id=issue_numbers,
        pr_number_by_gitlab_mr_id=pr_numbers,
    )
    sql = build_metadata_fix_sql(
        plan,
        issue_number_by_gitlab_issue_id=issue_numbers,
        pr_number_by_gitlab_mr_id=pr_numbers,
        comment_id_by_gitlab_note_id=comment_ids,
    )
    apply_metadata_fix_sql(sql)
