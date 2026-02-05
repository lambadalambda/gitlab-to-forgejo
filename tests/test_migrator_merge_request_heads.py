from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo.migrator import push_merge_request_heads
from gitlab_to_forgejo.plan_builder import MergeRequestPlan, Plan, RepoPlan


def test_push_merge_request_heads_pushes_synthetic_branches_for_missing_source_branch(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "001.bundle"
    bundle.write_bytes(b"not a real bundle")

    refs = tmp_path / "001.refs"
    refs.write_text(
        "\n".join(
            [
                "deadbeef HEAD",
                "aaaaaaaa refs/heads/existing",
                "",
            ]
        ),
        encoding="utf-8",
    )

    repo = RepoPlan(
        owner="pleroma",
        name="docs",
        gitlab_project_id=1,
        bundle_path=bundle,
        refs_path=refs,
        wiki_bundle_path=tmp_path / "wiki.bundle",
        wiki_refs_path=tmp_path / "wiki.refs",
    )

    plan = Plan(
        backup_id="b",
        orgs=[],
        repos=[repo],
        users=[],
        org_members={},
        issues=[],
        merge_requests=[
            MergeRequestPlan(
                gitlab_mr_id=101,
                gitlab_mr_iid=3,
                gitlab_target_project_id=1,
                source_branch="missing",
                target_branch="master",
                title="t",
                description="d",
                author_id=1,
                state_id=3,
                head_commit_sha="b" * 40,
            ),
            MergeRequestPlan(
                gitlab_mr_id=102,
                gitlab_mr_iid=4,
                gitlab_target_project_id=1,
                source_branch="existing",
                target_branch="master",
                title="t2",
                description="d2",
                author_id=1,
                state_id=3,
                head_commit_sha="c" * 40,
            ),
        ],
        notes=[],
    )

    with patch("gitlab_to_forgejo.migrator.push_bundle_http") as push:
        push_merge_request_heads(
            plan,
            forgejo_url="http://example.test",
            git_username="root",
            git_token="t0",
        )

    assert push.call_count == 1
    first = push.call_args_list[0].kwargs
    assert first["remote_url"] == "http://example.test/pleroma/docs.git"
    assert first["refspecs"] == ["b" * 40 + ":refs/heads/gitlab-mr-iid-3"]
    assert first["username"] == "root"
    assert first["token"] == "t0"

