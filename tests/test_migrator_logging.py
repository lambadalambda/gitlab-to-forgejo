from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.migrator import apply_merge_requests, migrate_plan
from gitlab_to_forgejo.plan_builder import MergeRequestPlan, OrgPlan, Plan, RepoPlan


class _BoomForgejo:
    def __init__(self) -> None:
        self.issue_numbers: list[int] = []

    def create_issue(  # type: ignore[override]
        self,
        *,
        owner: str,
        repo: str,
        title: str,
        body: str,
        sudo: str | None,
    ) -> dict[str, object]:
        number = 1234
        self.issue_numbers.append(number)
        return {"number": number}

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
        raise ForgejoError(
            method="POST",
            url=f"http://example.test/api/v1/repos/{owner}/{repo}/pulls",
            status_code=500,
            body='{"message":"","url":"http://example.test/api/swagger"}',
        )


def test_apply_merge_requests_logs_context_on_unhandled_forgejo_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    refs_path = tmp_path / "repo.refs"
    refs_path.write_text(
        "\n".join(
            [
                "1111111111111111111111111111111111111111 refs/heads/feature",
                "2222222222222222222222222222222222222222 refs/heads/master",
                "",
            ]
        ),
        encoding="utf-8",
    )

    plan = Plan(
        backup_id="x",
        orgs=[
            OrgPlan(name="pleroma", full_path="pleroma", gitlab_namespace_id=1, description=None)
        ],
        repos=[
            RepoPlan(
                owner="pleroma",
                name="pleroma-fe",
                gitlab_project_id=1,
                gitlab_disk_path="@hashed/aa/bb/pleroma-fe",
                bundle_path=tmp_path / "repo.bundle",
                refs_path=refs_path,
                wiki_bundle_path=tmp_path / "wiki.bundle",
                wiki_refs_path=tmp_path / "wiki.refs",
            )
        ],
        users=[],
        org_members={},
        issues=[],
        merge_requests=[
            MergeRequestPlan(
                gitlab_mr_id=123,
                gitlab_mr_iid=7,
                gitlab_target_project_id=1,
                source_branch="feature",
                target_branch="master",
                title="MR",
                description="D",
                author_id=1,
            )
        ],
        notes=[],
    )

    caplog.set_level(logging.ERROR, logger="gitlab_to_forgejo.migrator")

    client = _BoomForgejo()
    pr_numbers = apply_merge_requests(plan, client, user_by_id={1: "alice"})

    assert pr_numbers == {123: 1234}

    assert "pleroma/pleroma-fe" in caplog.text
    assert "GitLab MR !7" in caplog.text
    assert "head=feature" in caplog.text
    assert "base=master" in caplog.text
    assert "status=500" in caplog.text


def test_migrate_plan_logs_phase_progress(caplog: pytest.LogCaptureFixture) -> None:
    plan = Plan(
        backup_id="x",
        orgs=[],
        repos=[],
        users=[],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    caplog.set_level(logging.INFO, logger="gitlab_to_forgejo.migrator")

    with patch("gitlab_to_forgejo.migrator.apply_metadata_fix_sql"):
        migrate_plan(
            plan,
            client=object(),  # type: ignore[arg-type]
            user_password="pw",
            private_repos=True,
            forgejo_url="http://example.test",
            git_username="root",
            git_token="t0",
        )

    assert "Starting migration" in caplog.text
    assert "Users/orgs/teams" in caplog.text
    assert "Backfill metadata" in caplog.text
