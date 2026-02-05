from __future__ import annotations

from gitlab_to_forgejo.forgejo_db import build_password_hash_fix_sql
from gitlab_to_forgejo.plan_builder import Plan, UserPlan


def test_build_password_hash_fix_sql_updates_bcrypt_passwords() -> None:
    plan = Plan(
        backup_id="x",
        orgs=[],
        repos=[],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
                gitlab_encrypted_password="$2a$10$7EqJtq98hPqEX7fNZaFWoOa6F0lB1/6v7tKqB8p0fTrbqXc9F3u6y",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    sql = build_password_hash_fix_sql(
        plan, forgejo_username_by_gitlab_username={"alice": "alice"}
    )

    assert "BEGIN;" in sql
    assert "COMMIT;" in sql
    assert 'UPDATE "user" u' in sql
    assert "passwd_hash_algo = 'bcrypt'" in sql
    assert "u.lower_name = lower('alice')" in sql


def test_build_password_hash_fix_sql_skips_admin_username() -> None:
    plan = Plan(
        backup_id="x",
        orgs=[],
        repos=[],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="root",
                email="r@e",
                full_name="R",
                state="active",
                gitlab_encrypted_password="$2a$10$7EqJtq98hPqEX7fNZaFWoOa6F0lB1/6v7tKqB8p0fTrbqXc9F3u6y",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
    )

    sql = build_password_hash_fix_sql(
        plan,
        forgejo_username_by_gitlab_username={"root": "root"},
        skip_forgejo_usernames={"root"},
    )

    assert sql == ""

