from __future__ import annotations

import datetime as dt
from pathlib import Path

from gitlab_to_forgejo.forgejo_db import build_metadata_fix_sql
from gitlab_to_forgejo.plan_builder import (
    IssuePlan,
    NotePlan,
    OrgPlan,
    Plan,
    RepoPlan,
    UserPlan,
    build_plan,
)


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


def _unix(ts: str) -> int:
    return int(dt.datetime.fromisoformat(ts).replace(tzinfo=dt.UTC).timestamp())


def test_build_metadata_fix_sql_updates_issue_and_comment_timestamps() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    sql = build_metadata_fix_sql(
        plan,
        issue_number_by_gitlab_issue_id={2978: 12},
        pr_number_by_gitlab_mr_id={3973: 34},
        comment_id_by_gitlab_note_id={53164: 101, 53191: 102, 53193: 103},
    )

    assert "BEGIN;" in sql
    assert "COMMIT;" in sql

    assert "UPDATE issue i" in sql
    assert "u.lower_name = lower('pleroma')" in sql
    assert "r.lower_name = lower('docs')" in sql
    assert 'i."index" = 12' in sql
    assert f"created_unix = {_unix('2020-02-23 18:11:52.11909')}" in sql
    assert f"updated_unix = {_unix('2020-03-08 14:04:32.974976')}" in sql

    # MR/PR opening post timestamp.
    assert "-- gitlab mr !3 → pleroma/docs #34" in sql
    assert f"created_unix = {_unix('2020-03-08 15:10:43.272445')}" in sql
    assert f"updated_unix = {_unix('2020-03-08 16:02:46.115598')}" in sql

    # Issue note comment timestamp.
    assert "UPDATE comment" in sql
    assert f"created_unix = {_unix('2020-03-08 14:04:32.951042')}" in sql
    assert "WHERE id = 101" in sql


def test_build_metadata_fix_sql_closes_closed_issues_and_sets_closed_time() -> None:
    plan = Plan(
        backup_id="x",
        orgs=[
            OrgPlan(
                name="pleroma",
                full_path="pleroma",
                gitlab_namespace_id=3,
                description=None,
            )
        ],
        repos=[
            RepoPlan(
                owner="pleroma",
                name="docs",
                gitlab_project_id=673,
                bundle_path=Path("/tmp/repo.bundle"),
                refs_path=Path("/tmp/repo.refs"),
                wiki_bundle_path=Path("/tmp/wiki.bundle"),
                wiki_refs_path=Path("/tmp/wiki.refs"),
            )
        ],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@b",
                full_name="A",
                state="active",
            )
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=1,
                gitlab_issue_iid=7,
                gitlab_project_id=673,
                title="T",
                description="D",
                author_id=1,
                state_id=2,
                created_unix=100,
                updated_unix=200,
                closed_unix=0,
            )
        ],
        merge_requests=[],
        notes=[
            NotePlan(
                gitlab_note_id=1,
                gitlab_project_id=673,
                noteable_type="Issue",
                noteable_id=1,
                author_id=1,
                body="hi",
                created_unix=150,
                updated_unix=150,
            )
        ],
    )

    sql = build_metadata_fix_sql(
        plan,
        issue_number_by_gitlab_issue_id={1: 7},
        pr_number_by_gitlab_mr_id={},
        comment_id_by_gitlab_note_id={1: 999},
    )

    assert "is_closed = TRUE" in sql
    assert "closed_unix = 200" in sql
    assert "WHERE id = 999" in sql
