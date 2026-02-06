from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo.migrator import migrate_plan
from gitlab_to_forgejo.plan_builder import IssuePlan, NotePlan, OrgPlan, Plan, RepoPlan, UserPlan


def _plan() -> Plan:
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
            )
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
            )
        ],
        merge_requests=[],
        notes=[
            NotePlan(
                gitlab_note_id=5001,
                gitlab_project_id=10,
                noteable_type="Issue",
                noteable_id=1001,
                author_id=20,
                body="comment body",
            )
        ],
    )


def test_migrate_plan_uses_fast_db_issue_and_note_paths_when_enabled() -> None:
    plan = _plan()

    with (
        patch("gitlab_to_forgejo.migrator.apply_plan", return_value={"alice": "alice"}),
        patch("gitlab_to_forgejo.migrator.apply_user_avatars"),
        patch("gitlab_to_forgejo.migrator.apply_repos"),
        patch("gitlab_to_forgejo.migrator.ensure_repo_labels"),
        patch("gitlab_to_forgejo.migrator.push_repos"),
        patch("gitlab_to_forgejo.migrator.push_wikis"),
        patch("gitlab_to_forgejo.migrator.push_merge_request_heads"),
        patch("gitlab_to_forgejo.migrator.apply_issues") as apply_issues,
        patch(
            "gitlab_to_forgejo.migrator.apply_issues_db_fast",
            return_value={1001: 79},
        ) as apply_issues_db_fast,
        patch("gitlab_to_forgejo.migrator.apply_merge_requests", return_value={}),
        patch("gitlab_to_forgejo.migrator.apply_notes") as apply_notes,
        patch(
            "gitlab_to_forgejo.migrator.apply_notes_db_fast",
            return_value={5001: 5001},
        ) as apply_notes_db_fast,
        patch("gitlab_to_forgejo.migrator.apply_issue_and_pr_uploads"),
        patch("gitlab_to_forgejo.migrator.apply_note_uploads"),
        patch("gitlab_to_forgejo.migrator.apply_issue_and_mr_labels"),
        patch("gitlab_to_forgejo.migrator.build_metadata_fix_sql", return_value=""),
        patch("gitlab_to_forgejo.migrator.apply_metadata_fix_sql"),
    ):
        migrate_plan(
            plan,
            client=object(),  # type: ignore[arg-type]
            user_password="pw",
            private_repos=True,
            forgejo_url="http://example.test",
            git_username="root",
            git_token="t0",
            fast_db_issues=True,
        )

    apply_issues_db_fast.assert_called_once()
    apply_notes_db_fast.assert_called_once()
    apply_issues.assert_not_called()
    apply_notes.assert_not_called()
