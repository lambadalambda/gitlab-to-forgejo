from __future__ import annotations

import datetime as dt
from pathlib import Path

from gitlab_to_forgejo.plan_builder import build_plan


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


def _unix(ts: str) -> int:
    return int(dt.datetime.fromisoformat(ts).replace(tzinfo=dt.UTC).timestamp())


def test_build_plan_from_fixture() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")

    assert plan.backup_id == "1770183352_2026_02_04_18.4.6"

    org_names = sorted(o.name for o in plan.orgs)
    assert org_names == ["pleroma", "pleroma-elixir-libraries"]

    repos = {(r.owner, r.name) for r in plan.repos}
    assert repos == {
        ("pleroma", "docs"),
        ("pleroma-elixir-libraries", "pool-benchmark"),
    }
    disk_path_by_repo = {(r.owner, r.name): r.gitlab_disk_path for r in plan.repos}
    assert disk_path_by_repo[("pleroma", "docs")].startswith("@hashed/f4/46/")
    assert disk_path_by_repo[("pleroma-elixir-libraries", "pool-benchmark")].startswith(
        "@hashed/82/9f/"
    )
    for repo in plan.repos:
        assert repo.bundle_path.is_file()
        assert repo.refs_path.is_file()

    usernames = {u.username for u in plan.users}
    assert usernames == {"lambadalambda", "lanodan", "rinpatch"}
    assert {plan_user.avatar for plan_user in plan.users} == {"avatar.png"}

    # Membership inheritance: subgroup should include parent group members.
    subgroup_members = set(plan.org_members["pleroma-elixir-libraries"])
    assert {"lambadalambda", "lanodan"}.issubset(subgroup_members)

    assert len(plan.issues) == 1
    issue = plan.issues[0]
    assert issue.title == "Provide a link to the main site"
    assert issue.state_id == 1
    assert issue.created_unix == _unix("2020-02-23 18:11:52.11909")
    assert issue.updated_unix == _unix("2020-03-08 14:04:32.974976")
    assert issue.closed_unix == 0

    assert len(plan.merge_requests) == 1
    mr = plan.merge_requests[0]
    assert mr.title == "Add dropdown menu"
    assert mr.state_id == 1
    assert mr.created_unix == _unix("2020-03-08 15:10:43.272445")
    assert mr.updated_unix == _unix("2020-03-08 16:02:46.115598")
    assert mr.closed_unix == 0

    issue_notes = [n for n in plan.notes if n.noteable_type == "Issue"]
    assert len(issue_notes) == 1
    assert issue_notes[0].created_unix == _unix("2020-03-08 14:04:32.951042")
