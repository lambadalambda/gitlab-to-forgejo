from __future__ import annotations

import base64
import io
import tarfile
from pathlib import Path

from gitlab_to_forgejo.migrator import apply_user_avatars
from gitlab_to_forgejo.plan_builder import OrgPlan, Plan, RepoPlan, UserPlan


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def update_user_avatar(self, *, image_b64: str, sudo: str | None = None) -> None:
        self.calls.append(("update_user_avatar", image_b64, sudo))


def test_apply_user_avatars_extracts_from_uploads_tar_and_sets_avatar(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads.tar.gz"
    png_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00"
    with tarfile.open(uploads, "w:gz") as tf:
        info = tarfile.TarInfo(name="./-/system/user/avatar/43/avatar.png")
        info.size = len(png_bytes)
        tf.addfile(info, io.BytesIO(png_bytes))

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
                bundle_path=Path("repo.bundle"),
                refs_path=Path("repo.refs"),
                wiki_bundle_path=Path("wiki.bundle"),
                wiki_refs_path=Path("wiki.refs"),
            )
        ],
        users=[
            UserPlan(
                gitlab_user_id=43,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
                avatar="avatar.png",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[],
        uploads_tar_path=uploads,
    )

    client = _FakeForgejo()
    apply_user_avatars(plan, client, user_by_id={43: "alice"})

    expected = base64.b64encode(png_bytes).decode("ascii")
    assert client.calls == [("update_user_avatar", expected, "alice")]
