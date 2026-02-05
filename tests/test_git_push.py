from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from gitlab_to_forgejo.git_push import push_bundle_http


def test_push_bundle_http_uses_askpass_env(tmp_path: Path) -> None:
    bundle = tmp_path / "001.bundle"
    bundle.write_bytes(b"not a real bundle")

    refs = tmp_path / "001.refs"
    refs.write_text(
        "\n".join(
            [
                "deadbeef HEAD",
                "aaaaaaaa refs/heads/master",
                "bbbbbbbb refs/heads/feat",
                "",
            ]
        ),
        encoding="utf-8",
    )

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def _fake_run_git(argv: list[str], *, env: dict[str, str] | None = None) -> None:
        calls.append((argv, env))

    with patch("gitlab_to_forgejo.git_push._run_git", side_effect=_fake_run_git):
        push_bundle_http(
            bundle_path=bundle,
            refs_path=refs,
            remote_url="http://example.test/org/repo.git",
            username="root",
            token="t0",
        )

    assert calls[0][0][:3] == ["git", "clone", "--mirror"]
    assert calls[1][0][-4:] == ["remote", "add", "forgejo", "http://example.test/org/repo.git"]
    assert calls[2][0][-2:] == [
        "refs/heads/feat:refs/heads/feat",
        "refs/heads/master:refs/heads/master",
    ]

    env = calls[2][1]
    assert env is not None
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["FORGEJO_GIT_USERNAME"] == "root"
    assert env["FORGEJO_GIT_PASSWORD"] == "t0"
    assert env["GIT_ASKPASS"].endswith("/askpass.sh")

    flat_cmd = " ".join(" ".join(argv) for argv, _ in calls)
    assert "t0" not in flat_cmd


def test_push_bundle_http_skips_when_no_pushable_refs(tmp_path: Path) -> None:
    bundle = tmp_path / "001.bundle"
    bundle.write_bytes(b"not a real bundle")

    refs = tmp_path / "001.refs"
    refs.write_text("deadbeef HEAD\n", encoding="utf-8")

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def _fake_run_git(argv: list[str], *, env: dict[str, str] | None = None) -> None:
        calls.append((argv, env))

    with patch("gitlab_to_forgejo.git_push._run_git", side_effect=_fake_run_git):
        push_bundle_http(
            bundle_path=bundle,
            refs_path=refs,
            remote_url="http://example.test/org/repo.git",
            username="root",
            token="t0",
        )

    assert calls == []


def test_push_bundle_http_allows_explicit_refspecs(tmp_path: Path) -> None:
    bundle = tmp_path / "001.bundle"
    bundle.write_bytes(b"not a real bundle")

    missing_refs = tmp_path / "missing.refs"

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def _fake_run_git(argv: list[str], *, env: dict[str, str] | None = None) -> None:
        calls.append((argv, env))

    with patch("gitlab_to_forgejo.git_push._run_git", side_effect=_fake_run_git):
        push_bundle_http(
            bundle_path=bundle,
            refs_path=missing_refs,
            remote_url="http://example.test/org/repo.git",
            username="root",
            token="t0",
            refspecs=["refs/heads/master:refs/heads/main"],
        )

    assert calls[2][0][-1] == "refs/heads/master:refs/heads/main"


def test_push_bundle_http_chunks_refspecs(tmp_path: Path) -> None:
    bundle = tmp_path / "001.bundle"
    bundle.write_bytes(b"not a real bundle")

    missing_refs = tmp_path / "missing.refs"

    refspecs = [f"refs/heads/b{i}:refs/heads/b{i}" for i in range(5)]

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def _fake_run_git(argv: list[str], *, env: dict[str, str] | None = None) -> None:
        calls.append((argv, env))

    with patch("gitlab_to_forgejo.git_push._run_git", side_effect=_fake_run_git):
        push_bundle_http(
            bundle_path=bundle,
            refs_path=missing_refs,
            remote_url="http://example.test/org/repo.git",
            username="root",
            token="t0",
            refspecs=refspecs,
            chunk_size=2,
        )

    push_calls = [argv for argv, _ in calls if "push" in argv]
    assert len(push_calls) == 3
    assert push_calls[0][-2:] == ["refs/heads/b0:refs/heads/b0", "refs/heads/b1:refs/heads/b1"]
    assert push_calls[1][-2:] == ["refs/heads/b2:refs/heads/b2", "refs/heads/b3:refs/heads/b3"]
    assert push_calls[2][-1] == "refs/heads/b4:refs/heads/b4"
