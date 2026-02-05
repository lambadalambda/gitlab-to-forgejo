from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import gitlab_to_forgejo.plan_builder as plan_builder


def _fixture_backup_root() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini"


def test_build_plan_ignores_null_member_user_id_and_infers_note_project_id() -> None:
    expected = plan_builder.build_plan(_fixture_backup_root(), root_group_path="pleroma")

    original = plan_builder.iter_copy_rows

    def injected_iter_copy_rows(path: Path, *, tables: set[str]):
        yield from original(path, tables=tables)
        if tables == {"members", "issues", "merge_requests", "notes", "users", "labels"}:
            yield "members", {
                "source_type": "Namespace",
                "source_id": "3",
                "user_id": None,
                "access_level": "40",
            }
            yield "notes", {
                "project_id": None,
                "system": "f",
                "noteable_type": "Issue",
                "author_id": "2",
                "noteable_id": "2978",
                "id": "999999",
                "note": "synthetic note with null project_id",
            }

    with patch.object(plan_builder, "iter_copy_rows", side_effect=injected_iter_copy_rows):
        plan = plan_builder.build_plan(_fixture_backup_root(), root_group_path="pleroma")

    assert plan.org_members == expected.org_members

    extra_notes = [n for n in plan.notes if n.gitlab_note_id == 999999]
    assert len(extra_notes) == 1
    assert extra_notes[0].gitlab_project_id == 673
    assert extra_notes[0].noteable_type == "Issue"
    assert extra_notes[0].noteable_id == 2978
    assert extra_notes[0].body == "synthetic note with null project_id"


def test_build_plan_reads_merge_request_base_commit_sha_from_merge_request_diffs() -> None:
    original = plan_builder.iter_copy_rows

    diff_id = "999999"
    mr_id = "999998"
    base_sha = "0123456789abcdef0123456789abcdef01234567"
    head_sha = "89abcdef0123456789abcdef0123456789abcdef"

    def injected_iter_copy_rows(path: Path, *, tables: set[str]):
        yield from original(path, tables=tables)
        if tables == {"members", "issues", "merge_requests", "notes", "users", "labels"}:
            yield "merge_requests", {
                "id": mr_id,
                "iid": "999",
                "target_project_id": "673",
                "source_project_id": None,
                "source_branch": "features/menu",
                "target_branch": "master",
                "title": "Synthetic MR",
                "description": "Synthetic MR description",
                "author_id": "43",
                "state_id": "1",
                "latest_merge_request_diff_id": diff_id,
                "created_at": "2020-03-08 15:10:43.272445",
                "updated_at": "2020-03-08 16:02:46.115598",
                "closed_at": None,
                "merged_at": None,
            }
        elif tables == {"merge_request_diffs"}:
            yield "merge_request_diffs", {
                "id": diff_id,
                "head_commit_sha": head_sha,
                "base_commit_sha": base_sha,
            }

    with patch.object(plan_builder, "iter_copy_rows", side_effect=injected_iter_copy_rows):
        plan = plan_builder.build_plan(_fixture_backup_root(), root_group_path="pleroma")

    mr = next(m for m in plan.merge_requests if m.gitlab_mr_id == int(mr_id))
    assert mr.head_commit_sha == head_sha
    assert mr.base_commit_sha == base_sha
