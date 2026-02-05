from __future__ import annotations

import subprocess


def ensure_wiki_repo_exists(
    *,
    owner: str,
    repo: str,
    compose_service: str = "forgejo",
    repos_root: str = "/data/git/repositories",
) -> None:
    wiki_dir = f"{repos_root}/{owner}/{repo}.wiki.git"
    script = "\n".join(
        [
            "set -eu",
            'path="$1"',
            'parent="${path%/*}"',
            'mkdir -p "$parent"',
            'if [ ! -d "$path" ]; then',
            '  git init --bare "$path"',
            '  git --git-dir="$path" symbolic-ref HEAD refs/heads/main',
            "fi",
        ]
    )

    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "--user",
            "git",
            compose_service,
            "sh",
            "-c",
            script,
            "--",
            wiki_dir,
        ],
        check=True,
    )

