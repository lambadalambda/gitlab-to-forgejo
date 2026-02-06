from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import gitlab_to_forgejo.plan_builder as plan_builder


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


def _pass2_tables() -> set[str]:
    return {"members", "issues", "merge_requests", "notes", "users", "labels", "keys"}


def test_build_plan_reads_user_ssh_keys_and_otp_flag() -> None:
    original = plan_builder.iter_copy_rows

    def injected_iter_copy_rows(path: Path, *, tables: set[str]):
        yield from original(path, tables=tables)
        if tables == _pass2_tables():
            yield "users", {
                "id": "43",
                "username": "lanodan",
                "email": "lanodan@example.com",
                "name": "Lanodan",
                "state": "active",
                "avatar": None,
                "encrypted_password": None,
                "otp_required_for_login": "t",
            }
            yield "keys", {
                "id": "100001",
                "user_id": "43",
                "key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeKeyValue== lanodan@example.com",
                "title": "lanodan-laptop",
                "type": None,
            }
            yield "keys", {
                "id": "100002",
                "user_id": "43",
                "key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDeployKey== deploy@example.com",
                "title": "deploy-key",
                "type": "DeployKey",
            }
            yield "keys", {
                "id": "100003",
                "user_id": "999999",
                "key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINotInScope== noscope@example.com",
                "title": "not-in-scope",
                "type": None,
            }

    with patch.object(plan_builder, "iter_copy_rows", side_effect=injected_iter_copy_rows):
        plan = plan_builder.build_plan(_fixture_backup_root(), root_group_path="pleroma")

    user = next(u for u in plan.users if u.gitlab_user_id == 43)
    assert user.gitlab_otp_required_for_login is True

    assert len(plan.user_ssh_keys) == 1
    key = plan.user_ssh_keys[0]
    assert key.gitlab_key_id == 100001
    assert key.gitlab_user_id == 43
    assert key.title == "lanodan-laptop"
    assert key.key.startswith("ssh-ed25519 ")
