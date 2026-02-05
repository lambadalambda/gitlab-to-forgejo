from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo.migrator import apply_repos, push_repos, push_wikis
from gitlab_to_forgejo.plan_builder import Plan, RepoPlan, build_plan


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def ensure_org_repo(
        self, *, org: str, name: str, private: bool, default_branch: str | None
    ) -> None:
        self.calls.append(("ensure_org_repo", org, name, private, default_branch))


def test_apply_repos_creates_org_repos_with_default_branch() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()

    apply_repos(plan, client, private=True)

    assert client.calls == [
        ("ensure_org_repo", "pleroma", "docs", True, "master"),
        ("ensure_org_repo", "pleroma-elixir-libraries", "pool-benchmark", True, "master"),
    ]


def test_apply_repos_handles_missing_default_branch() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()

    with patch("gitlab_to_forgejo.migrator.guess_default_branch", side_effect=ValueError):
        apply_repos(plan, client, private=True)

    assert client.calls == [
        ("ensure_org_repo", "pleroma", "docs", True, None),
        ("ensure_org_repo", "pleroma-elixir-libraries", "pool-benchmark", True, None),
    ]


def test_push_repos_pushes_bundles_to_remote_urls() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")

    with patch("gitlab_to_forgejo.migrator.push_bundle_http") as push:
        push_repos(
            plan,
            forgejo_url="http://example.test",
            git_username="root",
            git_token="t0",
        )

    assert push.call_count == 2
    first = push.call_args_list[0].kwargs
    assert first["remote_url"] == "http://example.test/pleroma/docs.git"
    assert first["username"] == "root"
    assert first["token"] == "t0"


def test_push_wikis_initializes_wiki_repo_and_pushes_when_bundle_exists(tmp_path: Path) -> None:
    wiki_bundle = tmp_path / "001.bundle"
    wiki_bundle.write_bytes(b"not a real bundle")

    wiki_refs = tmp_path / "001.refs"
    wiki_refs.write_text("aaaaaaaa refs/heads/master\n", encoding="utf-8")

    plan = Plan(
        backup_id="b",
        orgs=[],
        repos=[
            RepoPlan(
                owner="pleroma",
                name="docs",
                gitlab_project_id=1,
                gitlab_disk_path="@hashed/aa/bb/docs",
                bundle_path=tmp_path / "repo.bundle",
                refs_path=tmp_path / "repo.refs",
                wiki_bundle_path=wiki_bundle,
                wiki_refs_path=wiki_refs,
            ),
            RepoPlan(
                owner="pleroma",
                name="no-wiki",
                gitlab_project_id=2,
                gitlab_disk_path="@hashed/aa/bb/no-wiki",
                bundle_path=tmp_path / "repo2.bundle",
                refs_path=tmp_path / "repo2.refs",
                wiki_bundle_path=tmp_path / "missing.bundle",
                wiki_refs_path=tmp_path / "missing.refs",
            ),
        ],
        users=[],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    with (
        patch("gitlab_to_forgejo.migrator.ensure_wiki_repo_exists") as ensure,
        patch("gitlab_to_forgejo.migrator.push_bundle_http") as push,
    ):
        push_wikis(
            plan,
            forgejo_url="http://example.test",
            git_username="root",
            git_token="t0",
        )

    assert ensure.call_count == 1
    assert ensure.call_args.kwargs == {"owner": "pleroma", "repo": "docs"}

    assert push.call_count == 1
    first = push.call_args_list[0].kwargs
    assert first["remote_url"] == "http://example.test/pleroma/docs.wiki.git"
    assert first["username"] == "root"
    assert first["token"] == "t0"
    assert first["refspecs"] == ["refs/heads/master:refs/heads/main"]
