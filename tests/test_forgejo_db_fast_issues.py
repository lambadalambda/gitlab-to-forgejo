from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.forgejo_db import (
    build_fast_issue_import_sql,
    build_fast_note_import_sql,
)
from gitlab_to_forgejo.plan_builder import (
    IssuePlan,
    NotePlan,
    OrgPlan,
    Plan,
    RepoPlan,
    UserPlan,
)


def _mk_plan() -> Plan:
    return Plan(
        backup_id="x",
        orgs=[
            OrgPlan(
                name="pleroma",
                full_path="pleroma",
                gitlab_namespace_id=1,
                description=None,
            )
        ],
        repos=[
            RepoPlan(
                owner="pleroma",
                name="pleroma-fe",
                gitlab_project_id=10,
                gitlab_disk_path="@hashed/aa/bb/cc",
                bundle_path=Path("/tmp/repo.bundle"),
                refs_path=Path("/tmp/repo.refs"),
                wiki_bundle_path=Path("/tmp/wiki.bundle"),
                wiki_refs_path=Path("/tmp/wiki.refs"),
            )
        ],
        users=[
            UserPlan(
                gitlab_user_id=20,
                username="alice",
                email="alice@example.test",
                full_name="Alice",
                state="active",
            ),
            UserPlan(
                gitlab_user_id=21,
                username="bob",
                email="bob@example.test",
                full_name="Bob",
                state="active",
            ),
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=1001,
                gitlab_issue_iid=79,
                gitlab_project_id=10,
                title="Issue title",
                description="Issue body",
                author_id=20,
                state_id=1,
                created_unix=100,
                updated_unix=150,
                closed_unix=0,
            )
        ],
        merge_requests=[],
        notes=[
            NotePlan(
                gitlab_note_id=5001,
                gitlab_project_id=10,
                noteable_type="Issue",
                noteable_id=1001,
                author_id=21,
                body="comment body",
                created_unix=120,
                updated_unix=121,
            )
        ],
    )


def test_build_fast_issue_import_sql_generates_insert_and_issue_index_sync() -> None:
    plan = _mk_plan()
    sql = build_fast_issue_import_sql(
        plan,
        issue_number_by_gitlab_issue_id={1001: 79},
        forgejo_username_by_gitlab_user_id={20: "alice"},
    )

    assert "BEGIN;" in sql
    assert "INSERT INTO issue (" in sql
    assert 'ON CONFLICT (repo_id, "index") DO NOTHING;' in sql
    assert "issue_index" in sql
    assert "max_index" in sql
    assert "COMMIT;" in sql
    assert "79" in sql
    assert "'Issue title'" in sql


def test_build_fast_note_import_sql_uses_note_id_for_comment_id_and_returns_mapping() -> None:
    plan = _mk_plan()
    sql, comment_ids = build_fast_note_import_sql(
        plan,
        issue_number_by_gitlab_issue_id={1001: 79},
        pr_number_by_gitlab_mr_id={},
        forgejo_username_by_gitlab_user_id={21: "bob"},
    )

    assert comment_ids == {5001: 5001}
    assert "BEGIN;" in sql
    assert "INSERT INTO comment (" in sql
    assert "ON CONFLICT (id) DO NOTHING;" in sql
    assert 'i."index" = 79' in sql
    assert "num_comments" in sql
    assert "COMMIT;" in sql
