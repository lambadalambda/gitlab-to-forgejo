from __future__ import annotations

import base64
import logging
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Protocol

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.forgejo_db import (
    apply_metadata_fix_sql,
    build_fast_issue_import_sql,
    build_fast_note_import_sql,
    build_metadata_fix_sql,
    build_password_hash_fix_sql,
)
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

logger = logging.getLogger(__name__)


@contextmanager
def _phase(name: str) -> Iterator[None]:
    start = time.monotonic()
    logger.info("==> %s", name)
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        logger.info("<== %s (%.1fs)", name, elapsed)


def _progress_step(total: int, *, target_messages: int = 50, min_step: int = 25) -> int:
    if total <= 0:
        return 1
    step = max(1, total // target_messages)
    return max(min_step, step)


def _format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    total = int(round(seconds))
    parts: list[str] = []
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return "".join(parts)


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
        "targetcouldn'tbefound" in msg or "targetcouldn\\u0027tbefound" in msg
    ) and '"errors":[]' in msg


def _is_missing_pull_request_base(err: ForgejoError) -> bool:
    if err.status_code != 404:
        return False
    msg = err.body.lower()
    return "could not find" in msg and "base repository" in msg


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
                logger.error(
                    "Create user failed for gitlab user %s (id=%s) status=%s body=%r",
                    user.username,
                    user.gitlab_user_id,
                    err.status_code,
                    err.body,
                )
                continue
            fallback = _fallback_username(user.username, user.gitlab_user_id)
            try:
                client.ensure_user(
                    username=fallback,
                    email=user.email,
                    full_name=user.full_name,
                    password=user_password,
                )
                forgejo_username_by_gitlab_username[user.username] = fallback
            except ForgejoError as err2:
                logger.error(
                    "Create user failed for gitlab user %s (id=%s) fallback=%s status=%s body=%r",
                    user.username,
                    user.gitlab_user_id,
                    fallback,
                    err2.status_code,
                    err2.body,
                )
                continue
            except Exception:
                logger.exception(
                    "Create user failed for gitlab user %s (id=%s) fallback=%s",
                    user.username,
                    user.gitlab_user_id,
                    fallback,
                )
                continue
        except Exception:
            logger.exception(
                "Create user failed for gitlab user %s (id=%s)",
                user.username,
                user.gitlab_user_id,
            )
            continue

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
        try:
            client.ensure_org(org=org.name, full_name=org.full_path, description=org.description)
        except ForgejoError as err:
            logger.error(
                "Create org failed for %s status=%s body=%r",
                org.name,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception("Create org failed for %s", org.name)
            continue

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
            try:
                owner_team_id = client.get_owner_team_id(org.name)
            except ForgejoError as err:
                logger.error(
                    "Get owner team failed for org=%s status=%s body=%r",
                    org.name,
                    err.status_code,
                    err.body,
                )
                owner_team_id = None
            except Exception:
                logger.exception("Get owner team failed for org=%s", org.name)
                owner_team_id = None
            if owner_team_id is not None:
                for username in owners:
                    try:
                        client.add_team_member(team_id=owner_team_id, username=username)
                    except ForgejoError as err:
                        logger.error(
                            "Add owner team member failed org=%s user=%s status=%s body=%r",
                            org.name,
                            username,
                            err.status_code,
                            err.body,
                        )
                    except Exception:
                        logger.exception(
                            "Add owner team member failed org=%s user=%s",
                            org.name,
                            username,
                        )

        if maintainers:
            try:
                team_id = client.ensure_team(
                    org=org.name,
                    name="Maintainers",
                    permission="admin",
                    includes_all_repositories=True,
                )
            except ForgejoError as err:
                logger.error(
                    "Ensure team failed org=%s team=Maintainers status=%s body=%r",
                    org.name,
                    err.status_code,
                    err.body,
                )
                team_id = None
            except Exception:
                logger.exception("Ensure team failed org=%s team=Maintainers", org.name)
                team_id = None
            if team_id is not None:
                for username in maintainers:
                    try:
                        client.add_team_member(team_id=team_id, username=username)
                    except ForgejoError as err:
                        logger.error(
                            "Add team member failed org=%s team=Maintainers "
                            "user=%s status=%s body=%r",
                            org.name,
                            username,
                            err.status_code,
                            err.body,
                        )
                    except Exception:
                        logger.exception(
                            "Add team member failed org=%s team=Maintainers user=%s",
                            org.name,
                            username,
                        )

        if developers:
            try:
                team_id = client.ensure_team(
                    org=org.name,
                    name="Developers",
                    permission="write",
                    includes_all_repositories=True,
                )
            except ForgejoError as err:
                logger.error(
                    "Ensure team failed org=%s team=Developers status=%s body=%r",
                    org.name,
                    err.status_code,
                    err.body,
                )
                team_id = None
            except Exception:
                logger.exception("Ensure team failed org=%s team=Developers", org.name)
                team_id = None
            if team_id is not None:
                for username in developers:
                    try:
                        client.add_team_member(team_id=team_id, username=username)
                    except ForgejoError as err:
                        logger.error(
                            "Add team member failed org=%s team=Developers "
                            "user=%s status=%s body=%r",
                            org.name,
                            username,
                            err.status_code,
                            err.body,
                        )
                    except Exception:
                        logger.exception(
                            "Add team member failed org=%s team=Developers user=%s",
                            org.name,
                            username,
                        )

        if reporters:
            try:
                team_id = client.ensure_team(
                    org=org.name,
                    name="Reporters",
                    permission="read",
                    includes_all_repositories=True,
                )
            except ForgejoError as err:
                logger.error(
                    "Ensure team failed org=%s team=Reporters status=%s body=%r",
                    org.name,
                    err.status_code,
                    err.body,
                )
                team_id = None
            except Exception:
                logger.exception("Ensure team failed org=%s team=Reporters", org.name)
                team_id = None
            if team_id is not None:
                for username in reporters:
                    try:
                        client.add_team_member(team_id=team_id, username=username)
                    except ForgejoError as err:
                        logger.error(
                            "Add team member failed org=%s team=Reporters "
                            "user=%s status=%s body=%r",
                            org.name,
                            username,
                            err.status_code,
                            err.body,
                        )
                    except Exception:
                        logger.exception(
                            "Add team member failed org=%s team=Reporters user=%s",
                            org.name,
                            username,
                        )

    return forgejo_username_by_gitlab_username


def apply_repos(plan: Plan, client: _ForgejoRepoOps, *, private: bool) -> None:
    if plan.repos:
        logger.info("Ensuring repositories (%d)", len(plan.repos))
    for repo in plan.repos:
        try:
            default_branch = guess_default_branch(repo.refs_path)
        except (FileNotFoundError, ValueError):
            default_branch = None
        try:
            client.ensure_org_repo(
                org=repo.owner,
                name=repo.name,
                private=private,
                default_branch=default_branch,
            )
        except ForgejoError as err:
            logger.error(
                "Create repo failed for %s/%s status=%s body=%r",
                repo.owner,
                repo.name,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception("Create repo failed for %s/%s", repo.owner, repo.name)
            continue


def push_repos(plan: Plan, *, forgejo_url: str, git_username: str, git_token: str) -> None:
    base = forgejo_url.rstrip("/")
    total = len(plan.repos)
    if total:
        logger.info("Pushing git repositories (%d)", total)
    for idx, repo in enumerate(plan.repos, start=1):
        logger.info("Git push repo %d/%d %s/%s", idx, total, repo.owner, repo.name)
        try:
            push_bundle_http(
                bundle_path=repo.bundle_path,
                refs_path=repo.refs_path,
                remote_url=f"{base}/{repo.owner}/{repo.name}.git",
                username=git_username,
                token=git_token,
            )
        except Exception:
            logger.exception("Push repo failed for %s/%s", repo.owner, repo.name)
            continue


def push_wikis(plan: Plan, *, forgejo_url: str, git_username: str, git_token: str) -> None:
    base = forgejo_url.rstrip("/")
    if plan.repos:
        logger.info("Pushing git wikis (best-effort)")
    total = len(plan.repos)
    for idx, repo in enumerate(plan.repos, start=1):
        refspecs = list_wiki_push_refspecs(repo.wiki_refs_path)
        if not refspecs or not repo.wiki_bundle_path.exists():
            continue
        logger.info("Git push wiki %d/%d %s/%s", idx, total, repo.owner, repo.name)
        try:
            ensure_wiki_repo_exists(owner=repo.owner, repo=repo.name)
        except Exception:
            logger.exception("Ensure wiki repo failed for %s/%s", repo.owner, repo.name)
            continue
        try:
            push_bundle_http(
                bundle_path=repo.wiki_bundle_path,
                refs_path=repo.wiki_refs_path,
                remote_url=f"{base}/{repo.owner}/{repo.name}.wiki.git",
                username=git_username,
                token=git_token,
                refspecs=refspecs,
            )
        except Exception:
            logger.exception("Push wiki failed for %s/%s", repo.owner, repo.name)
            continue


def push_merge_request_heads(
    plan: Plan, *, forgejo_url: str, git_username: str, git_token: str
) -> None:
    """Create synthetic branches in Forgejo for GitLab MRs missing source/target branches.

    Forgejo's PR API requires a head branch name; it does not accept raw commit SHAs as `head`.
    For merged MRs where the original source branch no longer exists, create a deterministic
    branch `gitlab-mr-iid-<iid>` pointing at the MR head commit SHA.

    Forgejo also assumes the PR base is a branch name during merge-base calculation. When a
    GitLab MR targets a branch that is missing from the GitLab backup, create a deterministic
    base branch `gitlab-mr-base-iid-<iid>` pointing at `merge_request_diffs.base_commit_sha`.
    """
    base = forgejo_url.rstrip("/")
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    refs_by_project_id: dict[int, dict[str, str]] = {}
    refspecs_by_project_id: dict[int, list[str]] = {}

    if plan.merge_requests:
        logger.info("Pushing merge request helper branches (%d)", len(plan.merge_requests))
    for mr in plan.merge_requests:
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            continue

        refs = refs_by_project_id.get(repo.gitlab_project_id)
        if refs is None:
            try:
                refs = read_ref_shas(repo.refs_path)
            except (FileNotFoundError, ValueError):
                refs = {}
            refs_by_project_id[repo.gitlab_project_id] = refs

        sha = mr.head_commit_sha
        if not sha:
            mr_ref = f"refs/merge-requests/{mr.gitlab_mr_iid}/head"
            sha = refs.get(mr_ref, "")
        if sha:
            branch_name = f"gitlab-mr-iid-{mr.gitlab_mr_iid}"
            refspec = f"{sha}:refs/heads/{branch_name}"
            refspecs_by_project_id.setdefault(repo.gitlab_project_id, []).append(refspec)

        target_branch_ref = f"refs/heads/{mr.target_branch}"
        if target_branch_ref not in refs and mr.base_commit_sha:
            branch_name = f"gitlab-mr-base-iid-{mr.gitlab_mr_iid}"
            refspec = f"{mr.base_commit_sha}:refs/heads/{branch_name}"
            refspecs_by_project_id.setdefault(repo.gitlab_project_id, []).append(refspec)

    for project_id, refspecs in refspecs_by_project_id.items():
        repo = repo_by_project_id[project_id]
        try:
            push_bundle_http(
                bundle_path=repo.bundle_path,
                refs_path=repo.refs_path,
                remote_url=f"{base}/{repo.owner}/{repo.name}.git",
                username=git_username,
                token=git_token,
                refspecs=sorted(set(refspecs)),
            )
        except Exception:
            logger.exception("Push merge request branches failed for %s/%s", repo.owner, repo.name)
            continue


def apply_issues(
    plan: Plan, client: _ForgejoRepoOps, *, user_by_id: Mapping[int, str]
) -> dict[int, int]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    issue_number_by_gitlab_issue_id: dict[int, int] = {}

    total = len(plan.issues)
    if total:
        logger.info("Importing issues (%d)", total)
    step = _progress_step(total)
    started = time.monotonic()

    for idx, issue in enumerate(plan.issues, start=1):
        if total and (idx == 1 or idx % step == 0 or idx == total):
            elapsed = time.monotonic() - started
            avg = elapsed / idx if idx else 0.0
            eta = avg * (total - idx)
            logger.info(
                "Issues progress: %d/%d (avg %.2fs, eta %s)",
                idx,
                total,
                avg,
                _format_duration(eta),
            )
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            logger.error("No repo found for issue project_id=%s", issue.gitlab_project_id)
            continue

        sudo = user_by_id.get(issue.author_id)
        try:
            resp = client.create_issue(
                owner=repo.owner,
                repo=repo.name,
                title=issue.title,
                body=issue.description,
                sudo=sudo,
            )
        except ForgejoError as err:
            logger.error(
                "Create issue failed for %s/%s GitLab issue #%s (id=%s) sudo=%s status=%s body=%r",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Create issue failed for %s/%s GitLab issue #%s (id=%s) sudo=%s",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
                sudo,
            )
            continue
        number = int(resp["number"])
        issue_number_by_gitlab_issue_id[issue.gitlab_issue_id] = number

    return issue_number_by_gitlab_issue_id


def apply_issues_db_fast(
    plan: Plan, client: _ForgejoRepoOps, *, user_by_id: Mapping[int, str]
) -> dict[int, int]:
    issue_number_by_gitlab_issue_id = {
        issue.gitlab_issue_id: issue.gitlab_issue_iid for issue in plan.issues
    }
    sql = build_fast_issue_import_sql(
        plan,
        issue_number_by_gitlab_issue_id=issue_number_by_gitlab_issue_id,
        forgejo_username_by_gitlab_user_id=user_by_id,
    )
    if not sql:
        return {}

    logger.info("Importing issues via DB fast-path (%d)", len(issue_number_by_gitlab_issue_id))
    try:
        apply_metadata_fix_sql(sql)
    except Exception:
        logger.exception("Fast DB issue import failed; falling back to API issue import")
        return apply_issues(plan, client, user_by_id=user_by_id)
    return issue_number_by_gitlab_issue_id


def apply_merge_requests(
    plan: Plan, client: _ForgejoRepoOps, *, user_by_id: Mapping[int, str]
) -> dict[int, int]:
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}
    pr_number_by_gitlab_mr_id: dict[int, int] = {}
    refs_by_project_id: dict[int, dict[str, str]] = {}

    total = len(plan.merge_requests)
    if total:
        logger.info("Importing merge requests (%d)", total)
    step = _progress_step(total)
    started = time.monotonic()

    for idx, mr in enumerate(plan.merge_requests, start=1):
        if total and (idx == 1 or idx % step == 0 or idx == total):
            elapsed = time.monotonic() - started
            avg = elapsed / idx if idx else 0.0
            eta = avg * (total - idx)
            logger.info(
                "Merge requests progress: %d/%d (avg %.2fs, eta %s)",
                idx,
                total,
                avg,
                _format_duration(eta),
            )
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            logger.error("No repo found for mr target_project_id=%s", mr.gitlab_target_project_id)
            continue

        refs = refs_by_project_id.get(repo.gitlab_project_id)
        if refs is None:
            try:
                refs = read_ref_shas(repo.refs_path)
            except (FileNotFoundError, ValueError):
                refs = {}
            refs_by_project_id[repo.gitlab_project_id] = refs

        source_branch_ref = f"refs/heads/{mr.source_branch}"
        head_sha = mr.head_commit_sha or refs.get(f"refs/merge-requests/{mr.gitlab_mr_iid}/head")
        if head_sha:
            head = f"gitlab-mr-iid-{mr.gitlab_mr_iid}"
        elif source_branch_ref in refs:
            head = mr.source_branch
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
            sudo = user_by_id.get(mr.author_id)
            try:
                resp = client.create_issue(
                    owner=repo.owner,
                    repo=repo.name,
                    title=f"MR: {mr.title}",
                    body=body,
                    sudo=sudo,
                )
                number = int(resp["number"])
                pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = number
            except ForgejoError as err:
                logger.error(
                    "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                    "sudo=%s status=%s body=%r",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    sudo,
                    err.status_code,
                    err.body,
                )
            except Exception:
                logger.exception(
                    "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) sudo=%s",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    sudo,
                )
            continue

        synthetic_base_branch = f"gitlab-mr-base-iid-{mr.gitlab_mr_iid}"
        target_branch_ref = f"refs/heads/{mr.target_branch}"
        if target_branch_ref in refs:
            base = mr.target_branch
        elif mr.base_commit_sha:
            base = synthetic_base_branch
        else:
            body = "\n".join(
                [
                    mr.description,
                    "",
                    (
                        f"_Imported from GitLab MR !{mr.gitlab_mr_iid} "
                        f"({mr.source_branch} → {mr.target_branch})_"
                    ),
                    "",
                    (
                        "_Forgejo pull request not created because the target branch "
                        "does not exist in the GitLab backup._"
                    ),
                ]
            ).strip()
            sudo = user_by_id.get(mr.author_id)
            try:
                resp = client.create_issue(
                    owner=repo.owner,
                    repo=repo.name,
                    title=f"MR: {mr.title}",
                    body=body,
                    sudo=sudo,
                )
                number = int(resp["number"])
                pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = number
            except ForgejoError as err:
                logger.error(
                    "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                    "sudo=%s status=%s body=%r",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    sudo,
                    err.status_code,
                    err.body,
                )
            except Exception:
                logger.exception(
                    "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) sudo=%s",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    sudo,
                )
            continue

        delays = (0.2, 0.5, 1.0)
        resp: Mapping[str, object] | None = None
        created_issue = False
        sudo = user_by_id.get(mr.author_id)
        for attempt in range(len(delays) + 1):
            try:
                resp = client.create_pull_request(
                    owner=repo.owner,
                    repo=repo.name,
                    title=mr.title,
                    body=mr.description,
                    head=head,
                    base=base,
                    sudo=sudo,
                )
                break
            except ForgejoError as err:
                if _is_missing_pull_request_base(err):
                    if mr.base_commit_sha and base != synthetic_base_branch:
                        base = synthetic_base_branch
                        continue
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
                                "_Forgejo pull request not created because the target branch "
                                "could not be resolved in the base repository._"
                            ),
                        ]
                    ).strip()
                    try:
                        issue_resp = client.create_issue(
                            owner=repo.owner,
                            repo=repo.name,
                            title=f"MR: {mr.title}",
                            body=issue_body,
                            sudo=sudo,
                        )
                        pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = int(issue_resp["number"])
                        created_issue = True
                    except ForgejoError as err2:
                        logger.error(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                            "sudo=%s status=%s body=%r",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                            err2.status_code,
                            err2.body,
                        )
                    except Exception:
                        logger.exception(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s "
                            "(id=%s) sudo=%s",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                        )
                    break
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
                    try:
                        issue_resp = client.create_issue(
                            owner=repo.owner,
                            repo=repo.name,
                            title=f"MR: {mr.title}",
                            body=issue_body,
                            sudo=sudo,
                        )
                        pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = int(issue_resp["number"])
                        created_issue = True
                    except ForgejoError as err2:
                        logger.error(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                            "sudo=%s status=%s body=%r",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                            err2.status_code,
                            err2.body,
                        )
                    except Exception:
                        logger.exception(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s "
                            "(id=%s) sudo=%s",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                        )
                    break
                if not _is_transient_target_not_found(err) or attempt >= len(delays):
                    logger.error(
                        "Create PR failed for %s/%s GitLab MR !%s (id=%s) "
                        "head=%s base=%s sudo=%s status=%s body=%r",
                        repo.owner,
                        repo.name,
                        mr.gitlab_mr_iid,
                        mr.gitlab_mr_id,
                        head,
                        base,
                        sudo,
                        err.status_code,
                        err.body,
                    )
                    issue_body = "\n".join(
                        [
                            mr.description,
                            "",
                            (
                                f"_Imported from GitLab MR !{mr.gitlab_mr_iid} "
                                f"({mr.source_branch} → {mr.target_branch})_"
                            ),
                            "",
                            ("_Forgejo pull request not created because PR creation failed._"),
                            "",
                            f"- head: `{head}`",
                            f"- base: `{base}`",
                            f"- error: {err.status_code} {err.body}",
                        ]
                    ).strip()
                    try:
                        issue_resp = client.create_issue(
                            owner=repo.owner,
                            repo=repo.name,
                            title=f"MR: {mr.title}",
                            body=issue_body,
                            sudo=sudo,
                        )
                        pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = int(issue_resp["number"])
                    except ForgejoError as err2:
                        logger.error(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                            "sudo=%s status=%s body=%r",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                            err2.status_code,
                            err2.body,
                        )
                    except Exception:
                        logger.exception(
                            "Create MR issue fallback failed for %s/%s GitLab MR !%s "
                            "(id=%s) sudo=%s",
                            repo.owner,
                            repo.name,
                            mr.gitlab_mr_iid,
                            mr.gitlab_mr_id,
                            sudo,
                        )
                    created_issue = True
                    break
                time.sleep(delays[attempt])
            except Exception as exc:
                logger.exception(
                    "Create PR failed for %s/%s GitLab MR !%s (id=%s) head=%s base=%s sudo=%s",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    head,
                    base,
                    sudo,
                )
                issue_body = "\n".join(
                    [
                        mr.description,
                        "",
                        (
                            f"_Imported from GitLab MR !{mr.gitlab_mr_iid} "
                            f"({mr.source_branch} → {mr.target_branch})_"
                        ),
                        "",
                        "_Forgejo pull request not created because PR creation raised an error._",
                        "",
                        f"- head: `{head}`",
                        f"- base: `{base}`",
                        f"- error: `{exc!r}`",
                    ]
                ).strip()
                try:
                    issue_resp = client.create_issue(
                        owner=repo.owner,
                        repo=repo.name,
                        title=f"MR: {mr.title}",
                        body=issue_body,
                        sudo=sudo,
                    )
                    pr_number_by_gitlab_mr_id[mr.gitlab_mr_id] = int(issue_resp["number"])
                except ForgejoError as err2:
                    logger.error(
                        "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) "
                        "sudo=%s status=%s body=%r",
                        repo.owner,
                        repo.name,
                        mr.gitlab_mr_iid,
                        mr.gitlab_mr_id,
                        sudo,
                        err2.status_code,
                        err2.body,
                    )
                except Exception:
                    logger.exception(
                        "Create MR issue fallback failed for %s/%s GitLab MR !%s (id=%s) sudo=%s",
                        repo.owner,
                        repo.name,
                        mr.gitlab_mr_iid,
                        mr.gitlab_mr_id,
                        sudo,
                    )
                created_issue = True
                break

        if created_issue:
            continue

        if resp is None:
            continue
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

    total = len(plan.notes)
    if total:
        logger.info("Importing notes/comments (%d)", total)
    step = _progress_step(total, min_step=100)
    started = time.monotonic()

    for idx, note in enumerate(plan.notes, start=1):
        if total and (idx == 1 or idx % step == 0 or idx == total):
            elapsed = time.monotonic() - started
            avg = elapsed / idx if idx else 0.0
            eta = avg * (total - idx)
            logger.info(
                "Notes progress: %d/%d (avg %.2fs, eta %s)",
                idx,
                total,
                avg,
                _format_duration(eta),
            )
        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None:
            logger.error("No repo found for note project_id=%s", note.gitlab_project_id)
            continue

        issue_number: int | None
        if note.noteable_type == "Issue":
            issue_number = issue_number_by_gitlab_issue_id.get(note.noteable_id)
        elif note.noteable_type == "MergeRequest":
            issue_number = pr_number_by_gitlab_mr_id.get(note.noteable_id)
        else:
            continue

        if issue_number is None:
            continue

        sudo = user_by_id.get(note.author_id)
        try:
            resp = client.create_issue_comment(
                owner=repo.owner,
                repo=repo.name,
                issue_number=issue_number,
                body=note.body,
                sudo=sudo,
            )
        except ForgejoError as err:
            logger.error(
                "Create comment failed for %s/%s GitLab note %s on %s %s "
                "(forgejo issue/pr #%s) sudo=%s status=%s body=%r",
                repo.owner,
                repo.name,
                note.gitlab_note_id,
                note.noteable_type,
                note.noteable_id,
                issue_number,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Create comment failed for %s/%s GitLab note %s on %s %s "
                "(forgejo issue/pr #%s) sudo=%s",
                repo.owner,
                repo.name,
                note.gitlab_note_id,
                note.noteable_type,
                note.noteable_id,
                issue_number,
                sudo,
            )
            continue
        comment_id_raw = resp.get("id")
        if comment_id_raw is not None:
            comment_id_by_gitlab_note_id[note.gitlab_note_id] = int(comment_id_raw)

    return comment_id_by_gitlab_note_id


def apply_notes_db_fast(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_by_id: Mapping[int, str],
    issue_number_by_gitlab_issue_id: Mapping[int, int],
    pr_number_by_gitlab_mr_id: Mapping[int, int],
) -> dict[int, int]:
    sql, comment_id_by_gitlab_note_id = build_fast_note_import_sql(
        plan,
        issue_number_by_gitlab_issue_id=issue_number_by_gitlab_issue_id,
        pr_number_by_gitlab_mr_id=pr_number_by_gitlab_mr_id,
        forgejo_username_by_gitlab_user_id=user_by_id,
    )
    if not sql:
        return {}

    logger.info("Importing notes/comments via DB fast-path (%d)", len(comment_id_by_gitlab_note_id))
    try:
        apply_metadata_fix_sql(sql)
    except Exception:
        logger.exception("Fast DB note import failed; falling back to API note import")
        return apply_notes(
            plan,
            client,
            user_by_id=user_by_id,
            issue_number_by_gitlab_issue_id=issue_number_by_gitlab_issue_id,
            pr_number_by_gitlab_mr_id=pr_number_by_gitlab_mr_id,
        )
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

    logger.info(
        "Migrating uploads referenced in issue/PR bodies (%d files)", len(upload_bytes_by_upload)
    )
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    for issue in plan.issues:
        issue_number = issue_number_by_gitlab_issue_id.get(issue.gitlab_issue_id)
        if issue_number is None:
            continue
        repo = repo_by_project_id.get(issue.gitlab_project_id)
        if repo is None:
            logger.error("No repo found for issue uploads project_id=%s", issue.gitlab_project_id)
            continue

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
            try:
                resp = client.create_issue_attachment(
                    owner=repo.owner,
                    repo=repo.name,
                    issue_number=int(issue_number),
                    filename=filename,
                    content=content,
                    sudo=sudo,
                )
            except ForgejoError as err:
                logger.error(
                    "Create issue attachment failed for %s/%s GitLab issue #%s (id=%s) "
                    "filename=%s sudo=%s status=%s body=%r",
                    repo.owner,
                    repo.name,
                    issue.gitlab_issue_iid,
                    issue.gitlab_issue_id,
                    filename,
                    sudo,
                    err.status_code,
                    err.body,
                )
                continue
            except Exception:
                logger.exception(
                    "Create issue attachment failed for %s/%s GitLab issue #%s (id=%s) "
                    "filename=%s sudo=%s",
                    repo.owner,
                    repo.name,
                    issue.gitlab_issue_iid,
                    issue.gitlab_issue_id,
                    filename,
                    sudo,
                )
                continue
            new_url = resp.get("browser_download_url")
            if new_url:
                mapping[url] = str(new_url)

        if not mapping:
            continue
        new_body = replace_gitlab_upload_urls(issue.description, mapping=mapping)
        if new_body == issue.description:
            continue
        try:
            client.edit_issue_body(
                owner=repo.owner,
                repo=repo.name,
                issue_number=int(issue_number),
                body=new_body,
                sudo=sudo,
            )
        except ForgejoError as err:
            logger.error(
                "Edit issue body failed for %s/%s GitLab issue #%s (id=%s) "
                "forgejo issue #%s sudo=%s status=%s body=%r",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
                issue_number,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Edit issue body failed for %s/%s GitLab issue #%s (id=%s) "
                "forgejo issue #%s sudo=%s",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
                issue_number,
                sudo,
            )
            continue

    for mr in plan.merge_requests:
        pr_number = pr_number_by_gitlab_mr_id.get(mr.gitlab_mr_id)
        if pr_number is None:
            continue
        repo = repo_by_project_id.get(mr.gitlab_target_project_id)
        if repo is None:
            logger.error(
                "No repo found for merge request uploads project_id=%s",
                mr.gitlab_target_project_id,
            )
            continue

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
            try:
                resp = client.create_issue_attachment(
                    owner=repo.owner,
                    repo=repo.name,
                    issue_number=int(pr_number),
                    filename=filename,
                    content=content,
                    sudo=sudo,
                )
            except ForgejoError as err:
                logger.error(
                    "Create PR attachment failed for %s/%s GitLab MR !%s (id=%s) "
                    "filename=%s sudo=%s status=%s body=%r",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    filename,
                    sudo,
                    err.status_code,
                    err.body,
                )
                continue
            except Exception:
                logger.exception(
                    "Create PR attachment failed for %s/%s GitLab MR !%s (id=%s) "
                    "filename=%s sudo=%s",
                    repo.owner,
                    repo.name,
                    mr.gitlab_mr_iid,
                    mr.gitlab_mr_id,
                    filename,
                    sudo,
                )
                continue
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
            logger.error(
                "Edit PR body failed for %s/%s GitLab MR !%s (id=%s) forgejo pr #%s "
                "sudo=%s status=%s body=%r",
                repo.owner,
                repo.name,
                mr.gitlab_mr_iid,
                mr.gitlab_mr_id,
                pr_number,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Edit PR body failed for %s/%s GitLab MR !%s (id=%s) forgejo pr #%s sudo=%s",
                repo.owner,
                repo.name,
                mr.gitlab_mr_iid,
                mr.gitlab_mr_id,
                pr_number,
                sudo,
            )
            continue


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

    logger.info(
        "Migrating uploads referenced in note/comment bodies (%d files)",
        len(upload_bytes_by_upload),
    )
    repo_by_project_id = {r.gitlab_project_id: r for r in plan.repos}

    for note in plan.notes:
        comment_id = comment_id_by_gitlab_note_id.get(note.gitlab_note_id)
        if comment_id is None:
            continue

        repo = repo_by_project_id.get(note.gitlab_project_id)
        if repo is None:
            logger.error("No repo found for note uploads project_id=%s", note.gitlab_project_id)
            continue

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
                    logger.error(
                        "Create comment attachment failed for %s/%s GitLab note %s "
                        "filename=%s sudo=%s status=%s body=%r",
                        repo.owner,
                        repo.name,
                        note.gitlab_note_id,
                        filename,
                        attachment_sudo,
                        err.status_code,
                        err.body,
                    )
                    continue
                attachment_sudo = None
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
                    logger.error(
                        "Create comment attachment failed for %s/%s GitLab note %s "
                        "filename=%s sudo=%s status=%s body=%r",
                        repo.owner,
                        repo.name,
                        note.gitlab_note_id,
                        filename,
                        attachment_sudo,
                        err.status_code,
                        err.body,
                    )
                    continue
                except Exception:
                    logger.exception(
                        "Create comment attachment failed for %s/%s GitLab note %s "
                        "filename=%s sudo=%s",
                        repo.owner,
                        repo.name,
                        note.gitlab_note_id,
                        filename,
                        attachment_sudo,
                    )
                    continue
            except Exception:
                logger.exception(
                    "Create comment attachment failed for %s/%s GitLab note %s filename=%s sudo=%s",
                    repo.owner,
                    repo.name,
                    note.gitlab_note_id,
                    filename,
                    attachment_sudo,
                )
                continue
            new_url = resp.get("browser_download_url")
            if new_url:
                mapping[url] = str(new_url)

        if not mapping:
            continue
        new_body = replace_gitlab_upload_urls(note.body, mapping=mapping)
        if new_body == note.body:
            continue
        try:
            client.edit_issue_comment(
                owner=repo.owner,
                repo=repo.name,
                comment_id=int(comment_id),
                body=new_body,
                sudo=sudo,
            )
        except ForgejoError as err:
            logger.error(
                "Edit comment body failed for %s/%s GitLab note %s forgejo comment %s "
                "sudo=%s status=%s body=%r",
                repo.owner,
                repo.name,
                note.gitlab_note_id,
                comment_id,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Edit comment body failed for %s/%s GitLab note %s forgejo comment %s sudo=%s",
                repo.owner,
                repo.name,
                note.gitlab_note_id,
                comment_id,
                sudo,
            )
            continue


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

    try:
        avatar_bytes = read_user_avatars_from_uploads(uploads, desired=desired)
    except Exception:
        logger.exception("Read user avatars from uploads.tar.gz failed")
        return
    for user_id, raw in sorted(avatar_bytes.items()):
        sudo = user_by_id.get(user_id)
        if not sudo:
            continue
        image_b64 = base64.b64encode(raw).decode("ascii")
        try:
            client.update_user_avatar(image_b64=image_b64, sudo=sudo)
        except ForgejoError as err:
            logger.error(
                "Update user avatar failed for gitlab user id=%s sudo=%s status=%s body=%r",
                user_id,
                sudo,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Update user avatar failed for gitlab user id=%s sudo=%s", user_id, sudo
            )
            continue


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
            logger.error("No repo found for labels project_id=%s", project_id)
            continue

        try:
            existing_by_name = {
                str(label_obj.get("name") or ""): label_obj
                for label_obj in client.list_repo_labels(owner=repo.owner, repo=repo.name)
            }
        except ForgejoError as err:
            logger.error(
                "List repo labels failed for %s/%s status=%s body=%r",
                repo.owner,
                repo.name,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception("List repo labels failed for %s/%s", repo.owner, repo.name)
            continue

        def sort_key(label_id: int) -> tuple[str, int]:
            label = label_by_id.get(label_id)
            return ((label.title.lower() if label else ""), label_id)

        for label_id in sorted(label_ids, key=sort_key):
            label = label_by_id.get(label_id)
            if label is None or not label.title:
                continue
            if label.title in existing_by_name:
                continue
            try:
                client.create_repo_label(
                    owner=repo.owner,
                    repo=repo.name,
                    name=label.title,
                    color=label.color,
                    description=label.description,
                )
            except ForgejoError as err:
                logger.error(
                    "Create repo label failed for %s/%s label=%s status=%s body=%r",
                    repo.owner,
                    repo.name,
                    label.title,
                    err.status_code,
                    err.body,
                )
                continue
            except Exception:
                logger.exception(
                    "Create repo label failed for %s/%s label=%s",
                    repo.owner,
                    repo.name,
                    label.title,
                )
                continue


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
            logger.error("No repo found for issue labels project_id=%s", issue.gitlab_project_id)
            continue
        try:
            client.replace_issue_labels(
                owner=repo.owner,
                repo=repo.name,
                issue_number=issue_number,
                labels=names,
                sudo=None,
            )
        except ForgejoError as err:
            logger.error(
                "Apply issue labels failed for %s/%s GitLab issue #%s (id=%s) status=%s body=%r",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Apply issue labels failed for %s/%s GitLab issue #%s (id=%s)",
                repo.owner,
                repo.name,
                issue.gitlab_issue_iid,
                issue.gitlab_issue_id,
            )
            continue

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
            logger.error(
                "No repo found for merge request labels project_id=%s", mr.gitlab_target_project_id
            )
            continue
        try:
            client.replace_issue_labels(
                owner=repo.owner,
                repo=repo.name,
                issue_number=pr_number,
                labels=names,
                sudo=None,
            )
        except ForgejoError as err:
            logger.error(
                "Apply MR labels failed for %s/%s GitLab MR !%s (id=%s) status=%s body=%r",
                repo.owner,
                repo.name,
                mr.gitlab_mr_iid,
                mr.gitlab_mr_id,
                err.status_code,
                err.body,
            )
            continue
        except Exception:
            logger.exception(
                "Apply MR labels failed for %s/%s GitLab MR !%s (id=%s)",
                repo.owner,
                repo.name,
                mr.gitlab_mr_iid,
                mr.gitlab_mr_id,
            )
            continue


def migrate_plan(
    plan: Plan,
    client: _ForgejoRepoOps,
    *,
    user_password: str,
    private_repos: bool,
    forgejo_url: str,
    git_username: str,
    git_token: str,
    migrate_password_hashes: bool = False,
    fast_db_issues: bool = False,
) -> None:
    logger.info(
        "Starting migration (backup_id=%s): orgs=%d repos=%d users=%d "
        "issues=%d mrs=%d notes=%d labels=%d",
        plan.backup_id,
        len(plan.orgs),
        len(plan.repos),
        len(plan.users),
        len(plan.issues),
        len(plan.merge_requests),
        len(plan.notes),
        len(plan.labels),
    )
    logger.info("Forgejo: %s", forgejo_url.rstrip("/"))

    with _phase("Users/orgs/teams"):
        forgejo_username_by_gitlab_username = apply_plan(plan, client, user_password=user_password)
    forgejo_user_by_gitlab_user_id: dict[int, str] = {}
    for u in plan.users:
        forgejo_username = forgejo_username_by_gitlab_username.get(u.username)
        if forgejo_username:
            forgejo_user_by_gitlab_user_id[u.gitlab_user_id] = forgejo_username

    if migrate_password_hashes:
        sql = build_password_hash_fix_sql(
            plan,
            forgejo_username_by_gitlab_username=forgejo_username_by_gitlab_username,
            skip_forgejo_usernames={"root", git_username},
        )
        with _phase("Password hashes (DB)"):
            try:
                apply_metadata_fix_sql(sql)
            except Exception:
                logger.exception("Apply password hash migration SQL failed")

    upload_bytes_by_upload: dict[GitLabProjectUpload, bytes] = {}
    if plan.uploads_tar_path is not None:
        desired_uploads = collect_project_uploads(plan)
        if desired_uploads:
            logger.info("Uploads: scanning %d referenced /uploads files", len(desired_uploads))
        if desired_uploads:
            try:
                with _phase("Read uploads.tar.gz"):
                    upload_bytes_by_upload = read_project_uploads_from_uploads(
                        plan.uploads_tar_path, desired=desired_uploads
                    )
            except Exception:
                logger.exception("Read project uploads from uploads.tar.gz failed")
                upload_bytes_by_upload = {}

    with _phase("User avatars"):
        apply_user_avatars(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)

    with _phase("Repositories"):
        apply_repos(plan, client, private=private_repos)
    with _phase("Repo labels"):
        ensure_repo_labels(plan, client)
    with _phase("Git push repos"):
        push_repos(plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token)
    with _phase("Git push wikis"):
        push_wikis(plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token)
    with _phase("Git push MR helper branches"):
        push_merge_request_heads(
            plan, forgejo_url=forgejo_url, git_username=git_username, git_token=git_token
        )

    with _phase("Issues"):
        if fast_db_issues:
            issue_numbers = apply_issues_db_fast(
                plan, client, user_by_id=forgejo_user_by_gitlab_user_id
            )
        else:
            issue_numbers = apply_issues(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    with _phase("Merge requests"):
        pr_numbers = apply_merge_requests(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    with _phase("Notes/comments"):
        if fast_db_issues:
            comment_ids = apply_notes_db_fast(
                plan,
                client,
                user_by_id=forgejo_user_by_gitlab_user_id,
                issue_number_by_gitlab_issue_id=issue_numbers,
                pr_number_by_gitlab_mr_id=pr_numbers,
            )
        else:
            comment_ids = apply_notes(
                plan,
                client,
                user_by_id=forgejo_user_by_gitlab_user_id,
                issue_number_by_gitlab_issue_id=issue_numbers,
                pr_number_by_gitlab_mr_id=pr_numbers,
            )
    with _phase("Issue/PR uploads"):
        apply_issue_and_pr_uploads(
            plan,
            client,
            user_by_id=forgejo_user_by_gitlab_user_id,
            issue_number_by_gitlab_issue_id=issue_numbers,
            pr_number_by_gitlab_mr_id=pr_numbers,
            upload_bytes_by_upload=upload_bytes_by_upload,
        )
    with _phase("Note uploads"):
        apply_note_uploads(
            plan,
            client,
            user_by_id=forgejo_user_by_gitlab_user_id,
            comment_id_by_gitlab_note_id=comment_ids,
            upload_bytes_by_upload=upload_bytes_by_upload,
        )
    with _phase("Apply labels"):
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
    logger.info(
        "Metadata backfill: issues=%d prs=%d comments=%d sql_bytes=%d",
        len(issue_numbers),
        len(pr_numbers),
        len(comment_ids),
        len(sql.encode("utf-8")),
    )
    with _phase("Backfill metadata (DB)"):
        try:
            apply_metadata_fix_sql(sql)
        except Exception:
            logger.exception("Apply metadata fix SQL failed")
