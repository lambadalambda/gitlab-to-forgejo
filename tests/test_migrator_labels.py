from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.migrator import apply_issue_and_mr_labels, ensure_repo_labels
from gitlab_to_forgejo.plan_builder import (
    IssuePlan,
    LabelPlan,
    MergeRequestPlan,
    OrgPlan,
    Plan,
    RepoPlan,
    UserPlan,
)


class _FakeForgejo:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self._repo_labels: dict[tuple[str, str], list[dict[str, object]]] = {}

    def list_repo_labels(self, *, owner: str, repo: str) -> list[dict[str, object]]:
        self.calls.append(("list_repo_labels", owner, repo))
        return list(self._repo_labels.get((owner, repo), []))

    def create_repo_label(
        self,
        *,
        owner: str,
        repo: str,
        name: str,
        color: str,
        description: str,
    ) -> dict[str, object]:
        self.calls.append(("create_repo_label", owner, repo, name, color, description))
        label = {"id": len(self._repo_labels.get((owner, repo), [])) + 1, "name": name}
        self._repo_labels.setdefault((owner, repo), []).append(label)
        return label

    def replace_issue_labels(
        self,
        *,
        owner: str,
        repo: str,
        issue_number: int,
        labels: list[str],
        sudo: str | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append(("replace_issue_labels", owner, repo, issue_number, tuple(labels), sudo))
        return [{"id": 1, "name": labels[0]}] if labels else []


def test_ensure_repo_labels_creates_missing_labels() -> None:
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
            )
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=1,
                gitlab_issue_iid=1,
                gitlab_project_id=673,
                title="Issue",
                description="Body",
                author_id=43,
            )
        ],
        merge_requests=[],
        notes=[],
        labels=[
            LabelPlan(gitlab_label_id=10, title="bug", color="#ff0000", description="Bug label"),
            LabelPlan(
                gitlab_label_id=11,
                title="discussion",
                color="#00ff00",
                description="Discuss label",
            ),
        ],
        issue_label_ids_by_gitlab_issue_id={1: (11, 10)},
    )

    client = _FakeForgejo()
    ensure_repo_labels(plan, client)

    assert client.calls == [
        ("list_repo_labels", "pleroma", "docs"),
        ("create_repo_label", "pleroma", "docs", "bug", "#ff0000", "Bug label"),
        ("create_repo_label", "pleroma", "docs", "discussion", "#00ff00", "Discuss label"),
    ]


def test_apply_issue_and_mr_labels_replaces_labels_by_name() -> None:
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
            )
        ],
        org_members={},
        issues=[
            IssuePlan(
                gitlab_issue_id=1,
                gitlab_issue_iid=1,
                gitlab_project_id=673,
                title="Issue",
                description="Body",
                author_id=43,
            )
        ],
        merge_requests=[
            MergeRequestPlan(
                gitlab_mr_id=2,
                gitlab_mr_iid=1,
                gitlab_target_project_id=673,
                source_branch="feature",
                target_branch="master",
                title="MR",
                description="Body",
                author_id=43,
            )
        ],
        notes=[],
        labels=[
            LabelPlan(gitlab_label_id=10, title="bug", color="#ff0000", description="Bug label"),
            LabelPlan(
                gitlab_label_id=11,
                title="discussion",
                color="#00ff00",
                description="Discuss label",
            ),
        ],
        issue_label_ids_by_gitlab_issue_id={1: (11, 10)},
        mr_label_ids_by_gitlab_mr_id={2: (11,)},
    )

    client = _FakeForgejo()
    apply_issue_and_mr_labels(
        plan,
        client,
        issue_number_by_gitlab_issue_id={1: 5},
        pr_number_by_gitlab_mr_id={2: 6},
    )

    assert client.calls == [
        ("replace_issue_labels", "pleroma", "docs", 5, ("bug", "discussion"), None),
        ("replace_issue_labels", "pleroma", "docs", 6, ("discussion",), None),
    ]
