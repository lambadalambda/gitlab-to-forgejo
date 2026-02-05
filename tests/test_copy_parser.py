from __future__ import annotations

from collections import Counter
from pathlib import Path

from gitlab_to_forgejo.copy_parser import iter_copy_rows


def _fixture_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures/gitlab-mini/db/database.sql"


def test_iter_copy_rows_counts() -> None:
    counts: Counter[str] = Counter()
    for table, _row in iter_copy_rows(_fixture_db_path()):
        counts[table] += 1

    assert counts == {
        "shards": 1,
        "namespaces": 2,
        "projects": 2,
        "project_repositories": 2,
        "users": 4,
        "members": 2,
        "issues": 1,
        "merge_requests": 1,
        "notes": 3,
    }


def test_nulls_and_unescape() -> None:
    issues = [
        row
        for table, row in iter_copy_rows(_fixture_db_path(), tables={"issues"})
        if table == "issues"
    ]
    assert len(issues) == 1
    issue = issues[0]

    # null handling (\N)
    assert issue["milestone_id"] is None

    # COPY escaping: the dump contains literal \"\\n\" sequences which should decode to newlines
    assert "\n\n" in issue["description"]
    assert "\\n" not in issue["description"]


def test_quoted_column_names_are_normalized() -> None:
    notes = [
        row
        for table, row in iter_copy_rows(_fixture_db_path(), tables={"notes"})
        if table == "notes"
    ]
    assert notes, "fixture should contain notes rows"
    assert "position" in notes[0]
