from __future__ import annotations

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.migrator import apply_plan
from gitlab_to_forgejo.plan_builder import Plan, UserPlan


class _FakeForgejo:
    def __init__(self) -> None:
        self.usernames: list[str] = []

    def ensure_user(self, *, username: str, email: str, full_name: str, password: str) -> None:
        self.usernames.append(username)
        if username == "ghost":
            raise ForgejoError(
                method="POST",
                url="http://example.test/api/v1/admin/users",
                status_code=422,
                body='{"message":"name is reserved [name: ghost]"}',
            )

    def ensure_org(
        self, *, org: str, full_name: str, description: str | None
    ) -> None:  # pragma: no cover
        raise AssertionError("not used")

    def get_owner_team_id(self, org: str) -> int:  # pragma: no cover
        raise AssertionError("not used")

    def ensure_team(  # pragma: no cover
        self,
        *,
        org: str,
        name: str,
        permission: str,
        includes_all_repositories: bool,
    ) -> int:
        raise AssertionError("not used")

    def add_team_member(self, *, team_id: int, username: str) -> None:  # pragma: no cover
        raise AssertionError("not used")


def test_apply_plan_falls_back_for_reserved_username() -> None:
    plan = Plan(
        backup_id="x",
        orgs=[],
        repos=[],
        users=[
            UserPlan(
                gitlab_user_id=3733,
                username="ghost",
                email="ghost@example.com",
                full_name="Ghost User",
                state="active",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    client = _FakeForgejo()
    apply_plan(plan, client, user_password="pw")

    assert client.usernames == ["ghost", "gitlab-ghost-3733"]


class _FakeForgejoInvalidUsername(_FakeForgejo):
    def ensure_user(self, *, username: str, email: str, full_name: str, password: str) -> None:
        self.usernames.append(username)
        if username == "namachan10777_":
            raise ForgejoError(
                method="POST",
                url="http://example.test/api/v1/admin/users",
                status_code=422,
                body='{"message":"[Username]: invalid username"}',
            )


def test_apply_plan_falls_back_for_invalid_username() -> None:
    plan = Plan(
        backup_id="x",
        orgs=[],
        repos=[],
        users=[
            UserPlan(
                gitlab_user_id=10777,
                username="namachan10777_",
                email="namachan10777@example.com",
                full_name="Namachan",
                state="active",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    client = _FakeForgejoInvalidUsername()
    apply_plan(plan, client, user_password="pw")

    assert client.usernames == ["namachan10777_", "gitlab-namachan10777-10777"]
