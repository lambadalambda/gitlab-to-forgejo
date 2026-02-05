from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.plan_builder import build_plan


def test_build_plan_parses_labels_and_assignments(tmp_path: Path) -> None:
    backup_root = tmp_path / "backup"
    (backup_root / "db").mkdir(parents=True)
    (backup_root / "backup_information.yml").write_text(
        ":backup_id: 123\n", encoding="utf-8"
    )

    issues_header = (
        "COPY public.issues (id, iid, project_id, title, description, author_id, state_id, "
        "created_at, updated_at, closed_at) FROM stdin;"
    )
    merge_requests_header = (
        "COPY public.merge_requests (id, iid, target_project_id, source_project_id, source_branch, "
        "target_branch, title, description, author_id, state_id, latest_merge_request_diff_id, "
        "created_at, updated_at, closed_at, merged_at) FROM stdin;"
    )
    notes_header = (
        "COPY public.notes (id, note, noteable_type, noteable_id, author_id, project_id, system, "
        "created_at, updated_at) FROM stdin;"
    )
    namespaces_header = (
        "COPY public.namespaces (id, name, path, type, parent_id, traversal_ids, description) "
        "FROM stdin;"
    )
    project_repositories_header = (
        "COPY public.project_repositories (id, shard_id, disk_path, project_id, object_format) "
        "FROM stdin;"
    )

    sql_lines = [
        "COPY public.shards (id, name) FROM stdin;",
        "1\tdefault",
        "\\.",
        "",
        namespaces_header,
        "3\tPleroma\tpleroma\tGroup\t\\N\t{3}\tAll Pleroma development",
        "\\.",
        "",
        project_repositories_header,
        (
            "1\t1\t@hashed/aa/bb/"
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\t673\t0"
        ),
        "\\.",
        "",
        "COPY public.projects (id, path, namespace_id) FROM stdin;",
        "673\tdocs\t3",
        "\\.",
        "",
        "COPY public.members (source_type, source_id, user_id, access_level) FROM stdin;",
        "Namespace\t3\t43\t40",
        "\\.",
        "",
        issues_header,
        (
            "2978\t1\t673\tIssue title\tIssue body\t43\t1\t2020-01-01 00:00:00+00\t"
            "2020-01-02 00:00:00+00\t\\N"
        ),
        "\\.",
        "",
        merge_requests_header,
        (
            "3973\t1\t673\t673\tfeature\tmaster\tMR title\tMR body\t43\t1\t\\N\t"
            "2020-01-03 00:00:00+00\t2020-01-04 00:00:00+00\t\\N\t\\N"
        ),
        "\\.",
        "",
        notes_header,
        "1\tfirst!\tIssue\t2978\t43\t673\tf\t2020-01-01 00:00:01+00\t2020-01-01 00:00:01+00",
        "\\.",
        "",
        "COPY public.users (id, username, email, name, state, avatar) FROM stdin;",
        "43\talice\talice@example.test\tAlice\tactive\tavatar.png",
        "\\.",
        "",
        "COPY public.labels (id, title, color, project_id, description, group_id) FROM stdin;",
        "10\tbug\t#ff0000\t673\tBug label\t\\N",
        "11\tdiscussion\t#00ff00\t\\N\tGroup label\t3",
        "\\.",
        "",
        "COPY public.label_links (id, label_id, target_id, target_type) FROM stdin;",
        "1\t10\t2978\tIssue",
        "2\t11\t3973\tMergeRequest",
        "\\.",
        "",
    ]
    sql = "\n".join(sql_lines)
    (backup_root / "db" / "database.sql").write_text(sql, encoding="utf-8")

    plan = build_plan(backup_root, root_group_path="pleroma")

    labels_by_id = {label.gitlab_label_id: label for label in plan.labels}
    assert labels_by_id[10].title == "bug"
    assert labels_by_id[10].color == "#ff0000"
    assert labels_by_id[10].description == "Bug label"
    assert labels_by_id[11].title == "discussion"

    assert plan.issue_label_ids_by_gitlab_issue_id == {2978: (10,)}
    assert plan.mr_label_ids_by_gitlab_mr_id == {3973: (11,)}
