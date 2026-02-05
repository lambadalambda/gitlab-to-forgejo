from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.migrator import apply_plan
from gitlab_to_forgejo.plan_builder import IssuePlan, OrgPlan, Plan, RepoPlan, UserPlan, build_plan


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._next_team_id = 100
        self._team_ids: dict[tuple[str, str], int] = {}
        self._owner_team_ids: dict[str, int] = {}

    def ensure_user(self, *, username: str, email: str, full_name: str, password: str) -> None:
        self.calls.append(("ensure_user", username, email, full_name, password))

    def ensure_org(self, *, org: str, full_name: str, description: str | None) -> None:
        self.calls.append(("ensure_org", org, full_name, description))

    def get_owner_team_id(self, org: str) -> int:
        self.calls.append(("get_owner_team_id", org))
        if org not in self._owner_team_ids:
            self._owner_team_ids[org] = self._next_team_id
            self._next_team_id += 1
        return self._owner_team_ids[org]

    def ensure_team(
        self,
        *,
        org: str,
        name: str,
        permission: str,
        includes_all_repositories: bool,
    ) -> int:
        self.calls.append(("ensure_team", org, name, permission, includes_all_repositories))
        key = (org, name)
        if key not in self._team_ids:
            self._team_ids[key] = self._next_team_id
            self._next_team_id += 1
        return self._team_ids[key]

    def add_team_member(self, *, team_id: int, username: str) -> None:
        self.calls.append(("add_team_member", team_id, username))


def test_apply_plan_creates_users_orgs_and_memberships() -> None:
    plan = build_plan(_fixture_backup_root(), root_group_path="pleroma")
    client = _FakeForgejo()

    apply_plan(plan, client, user_password="temp1234")

    assert client.calls[:3] == [
        ("ensure_user", "lambadalambda", "pleromagit@rogerbraun.net", "lain", "temp1234"),
        (
            "ensure_user",
            "lanodan",
            "contact+git.pleroma.social@hacktivis.me",
            "Haelwenn",
            "temp1234",
        ),
        ("ensure_user", "rinpatch", "rin+pleroma@patch.cx", "rinpatch", "temp1234"),
    ]

    assert client.calls[3:5] == [
        ("ensure_org", "pleroma", "pleroma", "All Pleroma development"),
        (
            "ensure_org",
            "pleroma-elixir-libraries",
            "pleroma/elixir-libraries",
            "Elixir libraries written or forked by Pleroma Project",
        ),
    ]

    # Memberships: lambadalambda is an owner (50), lanodan is a maintainer (40). Inheritance means
    # both orgs should receive those members. Interacting non-members should at least be reporters
    # so issue/MR authorship/comments can be attributed via Forgejo "sudo".
    assert client.calls[5:] == [
        ("get_owner_team_id", "pleroma"),
        ("add_team_member", 100, "lambadalambda"),
        ("ensure_team", "pleroma", "Maintainers", "admin", True),
        ("add_team_member", 101, "lanodan"),
        ("ensure_team", "pleroma", "Reporters", "read", True),
        ("add_team_member", 102, "rinpatch"),
        ("get_owner_team_id", "pleroma-elixir-libraries"),
        ("add_team_member", 103, "lambadalambda"),
        ("ensure_team", "pleroma-elixir-libraries", "Maintainers", "admin", True),
        ("add_team_member", 104, "lanodan"),
    ]


def test_apply_plan_adds_reporters_for_interactors_without_group_members(tmp_path: Path) -> None:
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
                gitlab_disk_path="@hashed/aa/bb/docs",
                bundle_path=tmp_path / "repo.bundle",
                refs_path=tmp_path / "repo.refs",
                wiki_bundle_path=tmp_path / "wiki.bundle",
                wiki_refs_path=tmp_path / "wiki.refs",
            )
        ],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="alice@example.com",
                full_name="Alice",
                state="active",
            )
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=1,
                gitlab_issue_iid=1,
                gitlab_project_id=673,
                title="Hello",
                description="World",
                author_id=1,
            )
        ],
        merge_requests=[],
        notes=[],
    )

    client = _FakeForgejo()
    apply_plan(plan, client, user_password="pw")

    assert client.calls == [
        ("ensure_user", "alice", "alice@example.com", "Alice", "pw"),
        ("ensure_org", "pleroma", "pleroma", None),
        ("ensure_team", "pleroma", "Reporters", "read", True),
        ("add_team_member", 100, "alice"),
    ]
