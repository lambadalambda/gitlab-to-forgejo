from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo.forgejo_client import ForgejoError, ForgejoNotFound
from gitlab_to_forgejo.migrator import apply_issues, apply_merge_requests, apply_notes
from gitlab_to_forgejo.plan_builder import MergeRequestPlan, Plan, build_plan


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._next_number = 1
        self._next_comment_id = 1000

    def create_issue(
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        sudo: str | None,
    ) -> dict[str, object]:
        self.calls.append(("create_issue", owner, repo, title, body, sudo))
        number = self._next_number
        self._next_number += 1
        return {"number": number}

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
    ) -> dict[str, object]:
        self.calls.append(("create_pull_request", owner, repo, title, body, head, base, sudo))
        number = self._next_number
        self._next_number += 1
        return {"number": number}

    def create_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None,
    ) -> dict[str, object]:
        self.calls.append(("create_issue_comment", owner, repo, issue_number, body, sudo))
        comment_id = self._next_comment_id
        self._next_comment_id += 1
        return {"id": comment_id}


def test_apply_issues_merge_requests_and_notes_from_fixture() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()

    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    issue_numbers = apply_issues(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    pr_numbers = apply_merge_requests(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)
    apply_notes(
        plan,
        client,
        user_by_id=forgejo_user_by_gitlab_user_id,
        issue_number_by_gitlab_issue_id=issue_numbers,
        pr_number_by_gitlab_mr_id=pr_numbers,
    )

    assert issue_numbers == {2978: 1}
    assert pr_numbers == {3973: 2}

    assert client.calls[0][0] == "create_issue"
    assert client.calls[0][1:4] == ("pleroma", "docs", "Provide a link to the main site")
    assert "nav bar" in str(client.calls[0][4])
    assert client.calls[0][5] == "lanodan"

    assert client.calls[1][0] == "create_pull_request"
    assert client.calls[1][1:4] == ("pleroma", "docs", "Add dropdown menu")
    assert client.calls[1][5:7] == ("features/menu", "master")
    assert client.calls[1][7] == "lanodan"

    comment_calls = [c for c in client.calls if c[0] == "create_issue_comment"]
    assert len(comment_calls) == 3
    assert comment_calls[0][1:4] == ("pleroma", "docs", 1)
    assert comment_calls[0][5] == "rinpatch"
    assert comment_calls[1][1:4] == ("pleroma", "docs", 2)
    assert comment_calls[1][5] == "lanodan"
    assert comment_calls[2][1:4] == ("pleroma", "docs", 2)
    assert comment_calls[2][5] == "lanodan"


def test_apply_merge_requests_falls_back_to_merge_request_head_sha_when_branch_missing() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()
    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    original = plan.merge_requests[0]
    mr = MergeRequestPlan(
        gitlab_mr_id=original.gitlab_mr_id,
        gitlab_mr_iid=original.gitlab_mr_iid,
        gitlab_target_project_id=original.gitlab_target_project_id,
        source_branch="deleted-branch",
        target_branch=original.target_branch,
        title=original.title,
        description=original.description,
        author_id=original.author_id,
        state_id=3,
        head_commit_sha="8d363825a9a6a94a4db1bc8da1be5b3afd2441fb",
    )
    plan_missing_branch = Plan(
        backup_id=plan.backup_id,
        orgs=plan.orgs,
        repos=plan.repos,
        users=plan.users,
        org_members=plan.org_members,
        issues=[],
        merge_requests=[mr],
        notes=[],
    )

    apply_merge_requests(plan_missing_branch, client, user_by_id=forgejo_user_by_gitlab_user_id)

    assert client.calls[0][0] == "create_pull_request"
    assert client.calls[0][5] == f"gitlab-mr-iid-{original.gitlab_mr_iid}"


def test_apply_merge_requests_uses_base_commit_sha_when_target_branch_missing() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()
    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    original = plan.merge_requests[0]
    mr = MergeRequestPlan(
        gitlab_mr_id=original.gitlab_mr_id,
        gitlab_mr_iid=original.gitlab_mr_iid,
        gitlab_target_project_id=original.gitlab_target_project_id,
        source_branch=original.source_branch,
        target_branch="deleted-target-branch",
        title=original.title,
        description=original.description,
        author_id=original.author_id,
        state_id=original.state_id,
        base_commit_sha="8d363825a9a6a94a4db1bc8da1be5b3afd2441fb",
    )
    plan_missing_target = Plan(
        backup_id=plan.backup_id,
        orgs=plan.orgs,
        repos=plan.repos,
        users=plan.users,
        org_members=plan.org_members,
        issues=[],
        merge_requests=[mr],
        notes=[],
    )

    apply_merge_requests(plan_missing_target, client, user_by_id=forgejo_user_by_gitlab_user_id)

    assert client.calls[0][0] == "create_pull_request"
    assert client.calls[0][5:7] == (
        original.source_branch,
        "8d363825a9a6a94a4db1bc8da1be5b3afd2441fb",
    )


class _FlakyPullRequestForgejo(_FakeForgejo):
    def __init__(self) -> None:
        super().__init__()
        self._failures_remaining = 1

    def create_pull_request(  # type: ignore[override]
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str,
        sudo: str | None,
    ) -> dict[str, object]:
        self.calls.append(("create_pull_request", owner, repo, title, body, head, base, sudo))
        if self._failures_remaining:
            self._failures_remaining -= 1
            raise ForgejoNotFound(
                method="POST",
                url="http://example.test/api/v1/repos/pleroma/docs/pulls",
                status_code=404,
                body='{"message":"The target couldn\\u0027t be found.","errors":[]}',
            )
        number = self._next_number
        self._next_number += 1
        return {"number": number}


def test_apply_merge_requests_retries_on_transient_404() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FlakyPullRequestForgejo()
    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    with patch("gitlab_to_forgejo.migrator.time.sleep"):
        pr_numbers = apply_merge_requests(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)

    assert pr_numbers == {3973: 1}
    assert [c[0] for c in client.calls].count("create_pull_request") == 2


def test_apply_merge_requests_falls_back_to_issue_when_branch_missing_and_not_merged() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()
    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    original = plan.merge_requests[0]
    mr = MergeRequestPlan(
        gitlab_mr_id=original.gitlab_mr_id,
        gitlab_mr_iid=original.gitlab_mr_iid,
        gitlab_target_project_id=original.gitlab_target_project_id,
        source_branch="deleted-branch",
        target_branch=original.target_branch,
        title=original.title,
        description=original.description,
        author_id=original.author_id,
        state_id=1,
    )
    plan_missing_branch = Plan(
        backup_id=plan.backup_id,
        orgs=plan.orgs,
        repos=plan.repos,
        users=plan.users,
        org_members=plan.org_members,
        issues=[],
        merge_requests=[mr],
        notes=[],
    )

    numbers = apply_merge_requests(
        plan_missing_branch, client, user_by_id=forgejo_user_by_gitlab_user_id
    )

    assert numbers == {original.gitlab_mr_id: 1}
    assert client.calls[0][0] == "create_issue"


class _NoChangesPullRequestForgejo(_FakeForgejo):
    def create_pull_request(  # type: ignore[override]
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str,
        sudo: str | None,
    ) -> dict[str, object]:
        self.calls.append(("create_pull_request", owner, repo, title, body, head, base, sudo))
        raise ForgejoError(
            method="POST",
            url="http://example.test/api/v1/repos/pleroma/docs/pulls",
            status_code=422,
            body='{"message":"Invalid PullRequest: There are no changes between the head and the base","url":"http://example.test/api/swagger"}',
        )


def test_apply_merge_requests_falls_back_to_issue_when_no_changes_between_head_and_base() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _NoChangesPullRequestForgejo()
    forgejo_user_by_gitlab_user_id = {u.gitlab_user_id: u.username for u in plan.users}

    numbers = apply_merge_requests(plan, client, user_by_id=forgejo_user_by_gitlab_user_id)

    assert numbers == {plan.merge_requests[0].gitlab_mr_id: 1}
    assert [c[0] for c in client.calls] == ["create_pull_request", "create_issue"]
