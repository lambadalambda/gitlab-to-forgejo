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
