from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo import cli
from gitlab_to_forgejo.plan_builder import (
    IssuePlan,
    LabelPlan,
    MergeRequestPlan,
    OrgPlan,
    Plan,
    RepoPlan,
    UserPlan,
)


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


def test_cli_migrate_builds_plan_and_applies(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("t0\n", encoding="utf-8")

    fake_client = object()

    with (
        patch("gitlab_to_forgejo.cli.migrate_plan") as migrate_plan,
        patch("gitlab_to_forgejo.cli.ForgejoClient", return_value=fake_client) as forgejo_client,
    ):
        rc = cli.main(
            [
                "migrate",
                "--backup",
                str(_fixture_backup_root()),
                "--root-group",
                "pleroma",
                "--forgejo-url",
                "http://example.test",
                "--token-file",
                str(token_file),
                "--user-password",
                "pw",
            ]
        )

    assert rc == 0
    forgejo_client.assert_called_once_with(base_url="http://example.test", token="t0")
    migrate_plan.assert_called_once()

    plan = migrate_plan.call_args.args[0]
    assert plan.backup_id == "1770183352_2026_02_04_18.4.6"

    assert migrate_plan.call_args.args[1] is fake_client
    assert migrate_plan.call_args.kwargs["user_password"] == "pw"
    assert migrate_plan.call_args.kwargs["private_repos"] is True
    assert migrate_plan.call_args.kwargs["forgejo_url"] == "http://example.test"
    assert migrate_plan.call_args.kwargs["git_username"] == "root"
    assert migrate_plan.call_args.kwargs["git_token"] == "t0"
    assert migrate_plan.call_args.kwargs["migrate_password_hashes"] is False


def test_cli_migrate_can_enable_password_hash_migration(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("t0\n", encoding="utf-8")

    with (
        patch("gitlab_to_forgejo.cli.migrate_plan") as migrate_plan,
        patch("gitlab_to_forgejo.cli.ForgejoClient"),
    ):
        rc = cli.main(
            [
                "migrate",
                "--backup",
                str(_fixture_backup_root()),
                "--root-group",
                "pleroma",
                "--forgejo-url",
                "http://example.test",
                "--token-file",
                str(token_file),
                "--migrate-password-hashes",
            ]
        )

    assert rc == 0
    assert migrate_plan.call_args.kwargs["migrate_password_hashes"] is True


def test_cli_migrate_supports_only_repo_filter(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("t0\n", encoding="utf-8")

    with (
        patch("gitlab_to_forgejo.cli.migrate_plan") as migrate_plan,
        patch("gitlab_to_forgejo.cli.ForgejoClient"),
    ):
        rc = cli.main(
            [
                "migrate",
                "--backup",
                str(_fixture_backup_root()),
                "--root-group",
                "pleroma",
                "--forgejo-url",
                "http://example.test",
                "--token-file",
                str(token_file),
                "--only-repo",
                "pleroma/docs",
            ]
        )

    assert rc == 0

    plan = migrate_plan.call_args.args[0]
    assert [(r.owner, r.name) for r in plan.repos] == [("pleroma", "docs")]
    assert [o.name for o in plan.orgs] == ["pleroma"]
    assert {i.gitlab_project_id for i in plan.issues} == {673}
    assert {mr.gitlab_target_project_id for mr in plan.merge_requests} == {673}
    assert {n.gitlab_project_id for n in plan.notes} == {673}


def test_filter_plan_to_single_repo_filters_labels_and_keeps_uploads_path(tmp_path: Path) -> None:
    plan = Plan(
        backup_id="x",
        orgs=[
            OrgPlan(name="pleroma", full_path="pleroma", gitlab_namespace_id=3, description=None)
        ],
        repos=[
            RepoPlan(
                owner="pleroma",
                name="docs",
                gitlab_project_id=1,
                gitlab_disk_path="@hashed/aa/bb/docs",
                bundle_path=tmp_path / "docs.bundle",
                refs_path=tmp_path / "docs.refs",
                wiki_bundle_path=tmp_path / "docs.wiki.bundle",
                wiki_refs_path=tmp_path / "docs.wiki.refs",
            ),
            RepoPlan(
                owner="pleroma",
                name="meta",
                gitlab_project_id=2,
                gitlab_disk_path="@hashed/aa/bb/meta",
                bundle_path=tmp_path / "meta.bundle",
                refs_path=tmp_path / "meta.refs",
                wiki_bundle_path=tmp_path / "meta.wiki.bundle",
                wiki_refs_path=tmp_path / "meta.wiki.refs",
            ),
        ],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
            ),
            UserPlan(
                gitlab_user_id=2,
                username="bob",
                email="b@e",
                full_name="B",
                state="active",
            ),
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=100,
                gitlab_issue_iid=1,
                gitlab_project_id=1,
                title="Docs issue",
                description="",
                author_id=1,
            ),
            IssuePlan(
                gitlab_issue_id=101,
                gitlab_issue_iid=1,
                gitlab_project_id=2,
                title="Meta issue",
                description="",
                author_id=2,
            ),
        ],
        merge_requests=[
            MergeRequestPlan(
                gitlab_mr_id=200,
                gitlab_mr_iid=1,
                gitlab_target_project_id=2,
                source_branch="feature",
                target_branch="master",
                title="MR",
                description="",
                author_id=2,
            )
        ],
        notes=[],
        uploads_tar_path=tmp_path / "uploads.tar.gz",
        labels=[
            LabelPlan(gitlab_label_id=10, title="bug", color="#ff0000", description=""),
            LabelPlan(gitlab_label_id=11, title="discussion", color="#00ff00", description=""),
        ],
        issue_label_ids_by_gitlab_issue_id={100: (10,), 101: (11,)},
        mr_label_ids_by_gitlab_mr_id={200: (11,)},
    )

    filtered = cli._filter_plan_to_single_repo(plan, only_repo="pleroma/docs")

    assert [(r.owner, r.name) for r in filtered.repos] == [("pleroma", "docs")]
    assert filtered.uploads_tar_path == plan.uploads_tar_path
    assert filtered.issue_label_ids_by_gitlab_issue_id == {100: (10,)}
    assert filtered.mr_label_ids_by_gitlab_mr_id == {}
    assert [label.gitlab_label_id for label in filtered.labels] == [10]
