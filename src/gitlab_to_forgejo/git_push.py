from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path

from gitlab_to_forgejo.git_refs import list_push_refspecs


def _run_git(argv: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(argv, check=True, env=env)


def _write_askpass_script(dir_path: Path) -> Path:
    path = dir_path / "askpass.sh"
    path.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                'case "$1" in',
                '*Username*) echo "$FORGEJO_GIT_USERNAME" ;;',
                '*Password*) echo "$FORGEJO_GIT_PASSWORD" ;;',
                "*) echo ;;",
                "esac",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _git_http_auth_env(*, askpass_path: Path, username: str, token: str) -> dict[str, str]:
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_ASKPASS"] = str(askpass_path)
    env["FORGEJO_GIT_USERNAME"] = username
    env["FORGEJO_GIT_PASSWORD"] = token
    return env


def _iter_chunks(items: list[str], *, chunk_size: int) -> Iterable[list[str]]:
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def push_bundle_http(
    *,
    bundle_path: Path,
    refs_path: Path,
    remote_url: str,
    username: str,
    token: str,
    refspecs: list[str] | None = None,
    chunk_size: int = 200,
) -> None:
    """Push GitLab `*.bundle` to a Forgejo HTTP remote (branches + tags only)."""
    if refspecs is None:
        try:
            refspecs = list_push_refspecs(refs_path)
        except FileNotFoundError:
            return
    if not refspecs or not bundle_path.exists():
        return

    with tempfile.TemporaryDirectory(prefix="gitlab-to-forgejo-") as tmp:
        tmp_path = Path(tmp)
        repo_dir = tmp_path / "repo.git"
        askpass = _write_askpass_script(tmp_path)
        env = _git_http_auth_env(askpass_path=askpass, username=username, token=token)

        _run_git(["git", "clone", "--mirror", str(bundle_path), str(repo_dir)])
        _run_git(["git", "-C", str(repo_dir), "remote", "add", "forgejo", remote_url])
        for chunk in _iter_chunks(refspecs, chunk_size=chunk_size):
            _run_git(
                ["git", "-c", "credential.helper=", "-C", str(repo_dir), "push", "forgejo", *chunk],
                env=env,
            )
