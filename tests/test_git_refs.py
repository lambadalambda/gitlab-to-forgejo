from __future__ import annotations

from pathlib import Path

from gitlab_to_forgejo.git_refs import (
    guess_default_branch,
    list_push_refspecs,
    list_wiki_push_refspecs,
    read_ref_shas,
)


def _docs_refs() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "fixtures/gitlab-mini/repositories/default/@hashed/f4/46/"
        / "f4466a4b51d21014b34f621813a1ed75f1c750ec328d908d9edc989c64778962.git"
        / "1770183352_2026_02_04_18.4.6/001.refs"
    )


def test_guess_default_branch_prefers_master_when_present() -> None:
    assert guess_default_branch(_docs_refs()) == "master"


def test_guess_default_branch_prefers_develop_when_no_main_or_master(tmp_path: Path) -> None:
    refs = tmp_path / "001.refs"
    refs.write_text(
        "\n".join(
            [
                "deadbeef HEAD",
                "ffffffff refs/heads/aaa",
                "aaaaaaaa refs/heads/feature/x",
                "bbbbbbbb refs/heads/develop",
                "cccccccc refs/heads/zzz",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert guess_default_branch(refs) == "develop"


def test_list_push_refspecs_includes_only_heads_and_tags() -> None:
    assert list_push_refspecs(_docs_refs()) == [
        "refs/heads/features/menu:refs/heads/features/menu",
        "refs/heads/goto-libera:refs/heads/goto-libera",
        "refs/heads/master:refs/heads/master",
        "refs/heads/pin-dependencies:refs/heads/pin-dependencies",
    ]


def test_read_ref_shas_includes_merge_request_refs() -> None:
    shas = read_ref_shas(_docs_refs())
    assert shas["refs/merge-requests/3/head"] == "8d363825a9a6a94a4db1bc8da1be5b3afd2441fb"


def test_list_wiki_push_refspecs_maps_master_to_main() -> None:
    refs_path = Path(__file__).resolve().parents[1] / "fixtures/wiki-refs/master_only.refs"
    assert list_wiki_push_refspecs(refs_path) == [
        "refs/heads/master:refs/heads/main",
    ]
