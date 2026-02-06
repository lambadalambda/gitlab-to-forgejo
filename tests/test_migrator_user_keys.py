from __future__ import annotations

import logging
from pathlib import Path

import pytest

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.migrator import apply_user_ssh_keys
from gitlab_to_forgejo.plan_builder import OrgPlan, Plan, RepoPlan, UserPlan, UserSSHKeyPlan


class _FakeForgejoUserKeys:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self._fail_first_with_duplicate = False

    def create_user_ssh_key(self, *, title: str, key: str, sudo: str | None) -> None:
        self.calls.append((title, key, sudo))
        if self._fail_first_with_duplicate:
            self._fail_first_with_duplicate = False
            raise ForgejoError(
                method="POST",
                url="http://example.test/api/v1/user/keys",
                status_code=422,
                body='{"message":"Key content has been used as non-deploy key"}',
            )


def _plan_with_keys(tmp_path: Path) -> Plan:
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
                bundle_path=tmp_path / "repo.bundle",
                refs_path=tmp_path / "repo.refs",
                wiki_bundle_path=tmp_path / "wiki.bundle",
                wiki_refs_path=tmp_path / "wiki.refs",
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
        issues=[],
        merge_requests=[],
        notes=[],
        user_ssh_keys=[
            UserSSHKeyPlan(
                gitlab_key_id=1001,
                gitlab_user_id=20,
                title="alice-laptop",
                key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeOne== alice@example.test",
            ),
            UserSSHKeyPlan(
                gitlab_key_id=1002,
                gitlab_user_id=21,
                title="bob-laptop",
                key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeTwo== bob@example.test",
            ),
        ],
    )


def test_apply_user_ssh_keys_creates_keys_for_mapped_users(tmp_path: Path) -> None:
    plan = _plan_with_keys(tmp_path)
    client = _FakeForgejoUserKeys()

    apply_user_ssh_keys(plan, client, user_by_id={20: "alice"})

    assert client.calls == [
        (
            "alice-laptop",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeOne== alice@example.test",
            "alice",
        )
    ]


def test_apply_user_ssh_keys_ignores_duplicate_conflicts(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    plan = _plan_with_keys(tmp_path)
    client = _FakeForgejoUserKeys()
    client._fail_first_with_duplicate = True

    caplog.set_level(logging.INFO, logger="gitlab_to_forgejo.migrator")
    apply_user_ssh_keys(plan, client, user_by_id={20: "alice", 21: "bob"})

    assert len(client.calls) == 2
    assert "already exists/skipping" in caplog.text
