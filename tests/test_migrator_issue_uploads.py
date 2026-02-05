from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.forgejo_client import ForgejoError
from gitlab_to_forgejo.gitlab_uploads import GitLabProjectUpload
from gitlab_to_forgejo.migrator import apply_issue_and_pr_uploads, apply_note_uploads
from gitlab_to_forgejo.plan_builder import (
    IssuePlan,
    MergeRequestPlan,
    NotePlan,
    OrgPlan,
    Plan,
    RepoPlan,
    UserPlan,
)


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def create_issue_attachment(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        filename: str,
        content: bytes,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            ("create_issue_attachment", owner, repo, issue_number, filename, content, sudo)
        )
        return {"browser_download_url": f"http://example.test/attachments/{filename}"}

    def edit_issue_body(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(("edit_issue_body", owner, repo, issue_number, body, sudo))
        return {"number": issue_number}

    def create_issue_comment_attachment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        filename: str,
        content: bytes,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            ("create_issue_comment_attachment", owner, repo, comment_id, filename, content, sudo)
        )
        return {"browser_download_url": f"http://example.test/attachments/{comment_id}/{filename}"}

    def edit_pull_request_body(
        self,
        *,
        owner: str,
        repo: str,
        pr_number: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(("edit_pull_request_body", owner, repo, pr_number, body, sudo))
        return {"number": pr_number}

    def edit_issue_comment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        body: str,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(("edit_issue_comment", owner, repo, comment_id, body, sudo))
        return {"id": comment_id}


class _FakeForgejoCommentAttachment403OnSudo(_FakeForgejo):
    def create_issue_comment_attachment(
        self,
        *,
        owner: str,
        repo: str,
        comment_id: int,
        filename: str,
        content: bytes,
        sudo: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            ("create_issue_comment_attachment", owner, repo, comment_id, filename, content, sudo)
        )
        if sudo is not None:
            raise ForgejoError(
                method="POST",
                url="http://example.test",
                status_code=403,
                body='{"message":"user should have permission to edit comment"}',
            )
        return {"browser_download_url": f"http://example.test/attachments/{comment_id}/{filename}"}


def test_apply_issue_and_pr_uploads_uploads_and_rewrites_body(tmp_path: Path) -> None:
    repo = RepoPlan(
        owner="pleroma",
        name="meta",
        gitlab_project_id=1,
        gitlab_disk_path="@hashed/aa/bb/meta",
        bundle_path=tmp_path / "repo.bundle",
        refs_path=tmp_path / "repo.refs",
        wiki_bundle_path=tmp_path / "wiki.bundle",
        wiki_refs_path=tmp_path / "wiki.refs",
    )
    issue = IssuePlan(
        gitlab_issue_id=10,
        gitlab_issue_iid=40,
        gitlab_project_id=1,
        title="UI/UX",
        description="Screenshot: ![](/uploads/765b08065cca166722283f5cf5234971/screen.png)",
        author_id=1,
    )
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
        repos=[repo],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
            )
        ],
        org_members={},
        issues=[issue],
        merge_requests=[],
        notes=[],
    )

    upload = GitLabProjectUpload(
        disk_path=repo.gitlab_disk_path,
        upload_hash="765b08065cca166722283f5cf5234971",
        filename="screen.png",
    )
    upload_bytes = {upload: b"png-bytes"}

    client = _FakeForgejo()
    apply_issue_and_pr_uploads(
        plan,
        client,
        user_by_id={1: "alice"},
        issue_number_by_gitlab_issue_id={10: 1},
        pr_number_by_gitlab_mr_id={},
        upload_bytes_by_upload=upload_bytes,
    )

    assert client.calls == [
        (
            "create_issue_attachment",
            "pleroma",
            "meta",
            1,
            "screen.png",
            b"png-bytes",
            "alice",
        ),
        (
            "edit_issue_body",
            "pleroma",
            "meta",
            1,
            "Screenshot: ![](http://example.test/attachments/screen.png)",
            "alice",
        ),
    ]


def test_apply_issue_and_pr_uploads_rewrites_pull_request_body(tmp_path: Path) -> None:
    repo = RepoPlan(
        owner="pleroma",
        name="meta",
        gitlab_project_id=1,
        gitlab_disk_path="@hashed/aa/bb/meta",
        bundle_path=tmp_path / "repo.bundle",
        refs_path=tmp_path / "repo.refs",
        wiki_bundle_path=tmp_path / "wiki.bundle",
        wiki_refs_path=tmp_path / "wiki.refs",
    )
    mr = MergeRequestPlan(
        gitlab_mr_id=30,
        gitlab_mr_iid=1,
        gitlab_target_project_id=1,
        source_branch="feature",
        target_branch="master",
        title="MR",
        description="Screenshot: /uploads/765b08065cca166722283f5cf5234971/screen.png",
        author_id=1,
    )
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
        repos=[repo],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[mr],
        notes=[],
    )

    upload = GitLabProjectUpload(
        disk_path=repo.gitlab_disk_path,
        upload_hash="765b08065cca166722283f5cf5234971",
        filename="screen.png",
    )
    upload_bytes = {upload: b"png-bytes"}

    client = _FakeForgejo()
    apply_issue_and_pr_uploads(
        plan,
        client,
        user_by_id={1: "alice"},
        issue_number_by_gitlab_issue_id={},
        pr_number_by_gitlab_mr_id={30: 2},
        upload_bytes_by_upload=upload_bytes,
    )

    assert client.calls == [
        (
            "create_issue_attachment",
            "pleroma",
            "meta",
            2,
            "screen.png",
            b"png-bytes",
            "alice",
        ),
        (
            "edit_pull_request_body",
            "pleroma",
            "meta",
            2,
            "Screenshot: http://example.test/attachments/screen.png",
            "alice",
        ),
    ]


def test_apply_note_uploads_uploads_and_rewrites_comment_body(tmp_path: Path) -> None:
    repo = RepoPlan(
        owner="pleroma",
        name="meta",
        gitlab_project_id=1,
        gitlab_disk_path="@hashed/aa/bb/meta",
        bundle_path=tmp_path / "repo.bundle",
        refs_path=tmp_path / "repo.refs",
        wiki_bundle_path=tmp_path / "wiki.bundle",
        wiki_refs_path=tmp_path / "wiki.refs",
    )
    note = NotePlan(
        gitlab_note_id=20,
        gitlab_project_id=1,
        noteable_type="Issue",
        noteable_id=10,
        author_id=1,
        body="See: /uploads/765b08065cca166722283f5cf5234971/screen.png",
    )
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
        repos=[repo],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[note],
    )

    upload = GitLabProjectUpload(
        disk_path=repo.gitlab_disk_path,
        upload_hash="765b08065cca166722283f5cf5234971",
        filename="screen.png",
    )
    upload_bytes = {upload: b"png-bytes"}

    client = _FakeForgejo()
    apply_note_uploads(
        plan,
        client,
        user_by_id={1: "alice"},
        comment_id_by_gitlab_note_id={20: 123},
        upload_bytes_by_upload=upload_bytes,
    )

    assert client.calls == [
        (
            "create_issue_comment_attachment",
            "pleroma",
            "meta",
            123,
            "screen.png",
            b"png-bytes",
            "alice",
        ),
        (
            "edit_issue_comment",
            "pleroma",
            "meta",
            123,
            "See: http://example.test/attachments/123/screen.png",
            "alice",
        ),
    ]


def test_apply_note_uploads_falls_back_to_admin_for_comment_attachments(tmp_path: Path) -> None:
    repo = RepoPlan(
        owner="pleroma",
        name="meta",
        gitlab_project_id=1,
        gitlab_disk_path="@hashed/aa/bb/meta",
        bundle_path=tmp_path / "repo.bundle",
        refs_path=tmp_path / "repo.refs",
        wiki_bundle_path=tmp_path / "wiki.bundle",
        wiki_refs_path=tmp_path / "wiki.refs",
    )
    note = NotePlan(
        gitlab_note_id=20,
        gitlab_project_id=1,
        noteable_type="Issue",
        noteable_id=10,
        author_id=1,
        body="See: /uploads/765b08065cca166722283f5cf5234971/screen.png",
    )
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
        repos=[repo],
        users=[
            UserPlan(
                gitlab_user_id=1,
                username="alice",
                email="a@e",
                full_name="A",
                state="active",
            )
        ],
        org_members={},
        issues=[],
        merge_requests=[],
        notes=[note],
    )

    upload = GitLabProjectUpload(
        disk_path=repo.gitlab_disk_path,
        upload_hash="765b08065cca166722283f5cf5234971",
        filename="screen.png",
    )
    upload_bytes = {upload: b"png-bytes"}

    client = _FakeForgejoCommentAttachment403OnSudo()
    apply_note_uploads(
        plan,
        client,
        user_by_id={1: "alice"},
        comment_id_by_gitlab_note_id={20: 123},
        upload_bytes_by_upload=upload_bytes,
    )

    assert client.calls == [
        (
            "create_issue_comment_attachment",
            "pleroma",
            "meta",
            123,
            "screen.png",
            b"png-bytes",
            "alice",
        ),
        (
            "create_issue_comment_attachment",
            "pleroma",
            "meta",
            123,
            "screen.png",
            b"png-bytes",
            None,
        ),
        (
            "edit_issue_comment",
            "pleroma",
            "meta",
            123,
            "See: http://example.test/attachments/123/screen.png",
            "alice",
        ),
    ]
